use std::{
    collections::HashMap,
    io::{Cursor, Read, SeekFrom, Write},
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use axum_typed_multipart::{FieldData, TryFromMultipart, TypedMultipart};
use lazy_static::lazy_static;
use revolt_config::{config, report_internal_error, FilesLimit};
use revolt_database::{iso8601_timestamp::Timestamp, Database, FileHash, Metadata, User};
use revolt_files::{
    create_thumbnail, decode_image, fetch_from_s3, image_size, is_animated, upload_to_s3,
    AUTHENTICATION_TAG_SIZE_BYTES,
};
use revolt_result::{create_error, Error, Result, ToRevoltError};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use stoat_otel::WideContextRepository;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::io::ReaderStream;
use tower_http::cors::{AllowHeaders, Any, CorsLayer};
use url_escape::encode_component;
use utoipa::ToSchema;

use crate::{
    exif::strip_metadata, metadata::generate_metadata, mime_type::determine_mime_type, AppState,
};

/// Build the API router
pub async fn router() -> Router<AppState> {
    // Plaintext media cache is intentionally
    // ephemeral and must not survive restart.
    let _ = fs::remove_dir_all(media_disk_cache_dir()).await;

    let _ = fs::create_dir_all(media_disk_cache_dir()).await;

    let config = config().await;

    let cors = CorsLayer::new()
        .allow_methods([Method::POST])
        .allow_headers(AllowHeaders::mirror_request())
        .expose_headers(vec![
            "X-RateLimit-Limit".try_into().unwrap(),
            "X-RateLimit-Bucket".try_into().unwrap(),
            "X-RateLimit-Remaining".try_into().unwrap(),
            "X-RateLimit-Reset-After".try_into().unwrap(),
            "Accept-Ranges".try_into().unwrap(),
            "Content-Range".try_into().unwrap(),
        ])
        .allow_origin(Any);

    Router::new()
        .route("/", get(root))
        .route(
            "/:tag",
            post(upload_file)
                .options(options)
                .layer(DefaultBodyLimit::max(
                    config.features.limits.global.body_limit_size,
                )),
        )
        .route("/:tag/:file_id", get(fetch_preview))
        .route(
            "/:tag/:file_id/:file_name",
            get(fetch_file).head(fetch_file_head),
        )
        .route("/mod/:tag/:file_id/:file_name", get(fetch_file_mod))
        .layer(cors)
}

const DEFAULT_MEDIA_DISK_CACHE_DIR: &str = "/tmp/stoat-autumn-media-cache";

fn media_disk_cache_dir() -> PathBuf {
    std::env::var_os("AUTUMN_MEDIA_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_MEDIA_DISK_CACHE_DIR))
}

const MEDIA_DISK_CACHE_TTL: Duration = Duration::from_secs(30 * 60);

const MEDIA_DISK_CACHE_MAX_BYTES: u64 = 20 * 1024 * 1024 * 1024;

const MEDIA_RAM_CACHE_LIMIT: usize = 64 * 1024 * 1024;

lazy_static! {
    static ref MEDIA_CACHE_LOCKS: Mutex<HashMap<String, Weak<Mutex<()>>>> =
        Mutex::new(HashMap::new());
}

lazy_static! {
    /// Short-lived file cache to allow us to populate different CDN regions without increasing bandwidth to S3 provider
    /// Uploads will also be stored here to prevent immediately queued downloads from doing the entire round-trip
    static ref S3_CACHE: moka::future::Cache<String, Result<Vec<u8>>> = moka::future::Cache::builder()
        .weigher(|_key, value: &Result<Vec<u8>>| -> u32 {
            std::mem::size_of::<Result<Vec<u8>>>() as u32 + if let Ok(vec) = value {
                vec.len().try_into().unwrap_or(u32::MAX)
            } else {
                std::mem::size_of::<Error>() as u32
            }
        })
        // TODO config
        // .max_capacity(1024 * 1024 * 1024) // Cache up to 1GiB in memory
        // .max_capacity(512 * 1024 * 1024) // Cache up to 512MiB in memory
        .max_capacity(256 * 1024 * 1024) // Keep small files in RAM only
        .time_to_live(Duration::from_secs(5 * 60)) // For up to 5 minutes
        .build();
}

/// Retrieve hash information and file data by given hash
async fn retrieve_file_by_hash(hash: &FileHash) -> Result<Vec<u8>> {
    if let Some(data) = S3_CACHE.get(&hash.id).await {
        data
    } else {
        let data = fetch_from_s3(&hash.bucket_id, &hash.path, &hash.iv).await;
        S3_CACHE.insert(hash.id.to_owned(), data.clone()).await;
        data
    }
}

fn media_cache_path(hash: &FileHash) -> PathBuf {
    media_disk_cache_dir().join(&hash.id)
}

async fn ensure_media_cache_dir() -> Result<()> {
    fs::create_dir_all(media_disk_cache_dir())
        .await
        .to_internal_error()
}

async fn touch_media_cache(path: &FsPath) {
    let path = path.to_owned();

    let _ = tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new().write(true).open(path)?;

        file.set_modified(SystemTime::now())
    })
    .await;
}

async fn media_cache_lock(hash_id: &str) -> Arc<Mutex<()>> {
    let mut locks = MEDIA_CACHE_LOCKS.lock().await;

    // Remove locks whose request group no longer exists.
    locks.retain(|_, weak_lock| weak_lock.strong_count() > 0);

    if let Some(lock) = locks.get(hash_id).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(Mutex::new(()));

    locks.insert(hash_id.to_owned(), Arc::downgrade(&lock));

    lock
}

async fn cached_media_path(hash: &FileHash) -> Result<PathBuf> {
    ensure_media_cache_dir().await?;

    let final_path = media_cache_path(hash);

    if let Ok(metadata) = fs::metadata(&final_path).await {
        if metadata.is_file() {
            touch_media_cache(&final_path).await;

            return Ok(final_path);
        }
    }

    let cache_lock = media_cache_lock(&hash.id).await;

    let _guard = cache_lock.lock().await;

    if fs::metadata(&final_path).await.is_ok() {
        touch_media_cache(&final_path).await;

        return Ok(final_path);
    }

    let tmp_path = final_path.with_extension("part");

    let _ = fs::remove_file(&tmp_path).await;

    // Legacy encrypted files still require one complete decrypt,
    // but this data is immediately written to disk and is not
    // retained inside the long-lived Moka cache.
    let data = fetch_from_s3(&hash.bucket_id, &hash.path, &hash.iv).await?;

    {
        let mut file = fs::File::create(&tmp_path).await.to_internal_error()?;

        file.write_all(&data).await.to_internal_error()?;

        file.flush().await.to_internal_error()?;
    }

    fs::rename(&tmp_path, &final_path)
        .await
        .to_internal_error()?;

    touch_media_cache(&final_path).await;

    Ok(final_path)
}

async fn read_cached_range(path: &FsPath, range: MediaRange) -> Result<Vec<u8>> {
    let length = range.end - range.start + 1;

    let mut file = fs::File::open(path).await.to_internal_error()?;

    file.seek(SeekFrom::Start(range.start as u64))
        .await
        .to_internal_error()?;

    let mut data = vec![0u8; length];

    file.read_exact(&mut data).await.to_internal_error()?;

    Ok(data)
}

async fn cleanup_media_disk_cache() {
    let Ok(mut entries) = fs::read_dir(media_disk_cache_dir()).await else {
        return;
    };

    let now = SystemTime::now();

    let mut cached_files: Vec<(PathBuf, SystemTime, u64)> = Vec::new();

    let mut total_size = 0u64;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        let Ok(metadata) = entry.metadata().await else {
            continue;
        };

        if !metadata.is_file() {
            continue;
        }

        let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);

        let age = now.duration_since(modified).unwrap_or_default();

        let is_partial = path.extension().and_then(|value| value.to_str()) == Some("part");

        if is_partial {
            if age > MEDIA_DISK_CACHE_TTL {
                let _ = fs::remove_file(&path).await;
            }

            continue;
        }

        if age > MEDIA_DISK_CACHE_TTL {
            let _ = fs::remove_file(&path).await;

            continue;
        }

        total_size = total_size.saturating_add(metadata.len());

        cached_files.push((path, modified, metadata.len()));
    }

    if total_size <= MEDIA_DISK_CACHE_MAX_BYTES {
        return;
    }

    // Oldest access first because mtime is
    // refreshed by touch_media_cache().
    cached_files.sort_by_key(|(_, modified, _)| *modified);

    for (path, _, size) in cached_files {
        if total_size <= MEDIA_DISK_CACHE_MAX_BYTES {
            break;
        }

        if fs::remove_file(&path).await.is_ok() {
            total_size = total_size.saturating_sub(size);
        }
    }
}

/// Successful root response
#[derive(Serialize, Debug, ToSchema)]
pub struct RootResponse {
    autumn: &'static str,
    version: &'static str,
}

/// Capture crate version from Cargo
static CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Root response from service
#[utoipa::path(
    get,
    path = "/",
    tag = "Files",
    responses(
        (status = 200, description = "Echo response", body = RootResponse)
    )
)]
async fn root() -> Json<RootResponse> {
    Json(RootResponse {
        autumn: "Hello, I am a file server!",
        version: CRATE_VERSION,
    })
}

/// Empty handler for OPTIONS routes
async fn options() {}

/// Available tags to upload to
#[derive(Clone, Deserialize, Debug, ToSchema, strum_macros::IntoStaticStr)]
#[allow(non_camel_case_types)]
pub enum Tag {
    attachments,
    avatars,
    backgrounds,
    icons,
    banners,
    emojis,
}

/// Request body for upload
#[derive(ToSchema, TryFromMultipart)]
pub struct UploadPayload {
    #[schema(format = Binary)]
    #[allow(dead_code)]
    #[form_data(limit = "unlimited")] // handled by axum
    file: FieldData<NamedTempFile>,
}

/// Successful upload response
#[derive(Serialize, Debug, ToSchema)]
pub struct UploadResponse {
    /// ID to attach uploaded file to object
    id: String,
}

/// Upload a file
///
/// Available tags and restrictions:
///
/// | Tag | Size | Resolution | Type |
/// | :-: | --: | :-- | :-: |
/// | attachments | 20 MB | - | Any |
/// | avatars | 4 MB | 40 MP or 10,000px | Image |
/// | backgrounds | 6 MB | 40 MP or 10,000px | Image |
/// | icons | 2.5 MB | 40 MP or 10,000px | Image |
/// | banners | 6 MB | 40 MP or 10,000px | Image |
/// | emojis | 500 KB | 40 MP or 10,000px | Image |
#[utoipa::path(
    post,
    path = "/{tag}",
    tag = "Files",
    responses(
        (status = 200, description = "Upload was successful", body = UploadResponse)
    ),
    params(
        ("tag" = Tag, Path, description = "Tag to upload to (e.g. attachments, icons, ...)")
    ),
    request_body(content_type = "multipart/form-data", content = UploadPayload),
    security(
        ("session_token" = []),
        ("bot_token" = [])
    )
)]
async fn upload_file(
    State(db): State<Database>,
    wide_context: WideContextRepository,
    user: User,
    Path(tag): Path<Tag>,
    TypedMultipart(UploadPayload { mut file }): TypedMultipart<UploadPayload>,
) -> Result<Json<UploadResponse>> {
    wide_context.add("user_id", user.id.clone());

    // Fetch configuration
    let config = config().await;

    // Keep track of processing time
    let now = Instant::now();

    // Extract the filename, or give it a generic name
    let filename = file.metadata.file_name.unwrap_or("unnamed-file".to_owned());

    // Load file to memory
    let mut buf = Vec::<u8>::new();
    report_internal_error!(file.contents.read_to_end(&mut buf))?;

    // Take note of original file size
    let original_file_size = buf.len();

    // Ensure the file is not empty
    if original_file_size < config.files.limit.min_file_size {
        return Err(create_error!(FileTooSmall));
    }

    // Get user's file upload limits
    let limits = user.limits().await;
    let size_limit = *limits
        .file_upload_size_limit
        .get(tag.clone().into())
        .expect("size limit");

    if original_file_size > size_limit {
        return Err(create_error!(FileTooLarge { max: size_limit }));
    }

    // Generate sha256 hash
    let original_hash = {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&buf);
        hasher.finalize()
    };

    // Generate an ID for this file
    let id = if matches!(tag, Tag::emojis) {
        ulid::Ulid::new().to_string()
    } else {
        nanoid::nanoid!(42)
    };

    // Determine the mime type for the file
    let mime_type = determine_mime_type(&mut file.contents, &buf, &filename);

    // Check blocklist for mime type
    if config
        .files
        .blocked_mime_types
        .iter()
        .any(|m| m == mime_type)
    {
        return Err(create_error!(FileTypeNotAllowed));
    }

    if let Some((width, height)) = image_size(&file.contents)
        && !image_within_limits(width, height, &config.files.limit)
    {
        return Err(create_error!(ImageResolutionNotAllowed));
    }

    // Determine metadata for the file
    let metadata = generate_metadata(&file.contents, mime_type);

    // Block non-images for non-attachment uploads
    if !matches!(tag, Tag::attachments) && !matches!(metadata, Metadata::Image { .. }) {
        return Err(create_error!(FileTypeNotAllowed));
    }

    // Find an existing hash and use that if possible
    let file_hash_exists = if let Ok(file_hash) = db
        .fetch_attachment_hash(&format!("{original_hash:02x}"))
        .await
    {
        if !file_hash.iv.is_empty() {
            let tag: &'static str = tag.into();
            db.insert_attachment(&file_hash.into_file(
                id.clone(),
                tag.to_owned(),
                filename,
                user.id,
            ))
            .await?;

            return Ok(Json(UploadResponse { id }));
        }

        true
    } else {
        false
    };

    // Strip metadata
    let (buf, metadata) = strip_metadata(file.contents, buf, metadata, mime_type).await?;

    // Virus scan files if ClamAV is configured
    if matches!(metadata, Metadata::File)
        && (config.files.scan_mime_types.is_empty()
            || config.files.scan_mime_types.iter().any(|v| v == mime_type))
        && crate::clamav::is_malware(&buf).await?
    {
        return Err(create_error!(InternalError));
    }

    // Print file information for debug purposes
    let new_file_size = buf.len() + AUTHENTICATION_TAG_SIZE_BYTES;
    let processed_hash = {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&buf);
        hasher.finalize()
    };
    let process_ratio = new_file_size as f32 / original_file_size as f32;
    let time_to_process = Instant::now() - now;

    tracing::info!("Received file {filename}\nOriginal hash: {original_hash:02x}\nOriginal size: {original_file_size} bytes\nMime type: {mime_type}\nMetadata: {metadata:?}\nProcessed file size: {new_file_size} bytes ({:.2}%).\nProcessed hash: {processed_hash:02x}\nProcessing took {time_to_process:?}", process_ratio * 100.0);

    // Create hash entry in database
    let file_hash = FileHash {
        id: format!("{original_hash:02x}"),
        processed_hash: format!("{processed_hash:02x}"),

        created_at: Timestamp::now_utc(),

        bucket_id: config.files.s3.default_bucket,
        path: format!("{original_hash:02x}"),
        iv: String::new(), // indicates file is not uploaded yet

        metadata,
        content_type: mime_type.to_owned(),
        size: new_file_size as isize,
    };

    // Add attachment hash if it doesn't exist
    if !file_hash_exists {
        db.insert_attachment_hash(&file_hash).await?;
    }

    // Upload the file to S3 and commit nonce to database
    let upload_start = Instant::now();
    let nonce = upload_to_s3(&file_hash.bucket_id, &file_hash.id, &buf).await?;
    db.set_attachment_hash_nonce(&file_hash.id, &nonce).await?;

    // Debug information
    let time_to_upload = Instant::now() - upload_start;
    tracing::info!("Took {time_to_upload:?} to upload {new_file_size} bytes to S3.");

    // Finally, create the file and return its ID
    let tag: &'static str = tag.into();
    db.insert_attachment(&file_hash.into_file(id.clone(), tag.to_owned(), filename, user.id))
        .await?;

    Ok(Json(UploadResponse { id }))
}

/// Check whether the given image dimensions satisfy the configured file limits.
fn image_within_limits(width: usize, height: usize, limits: &FilesLimit) -> bool {
    width <= limits.max_pixel_side
        && height <= limits.max_pixel_side
        && width.saturating_mul(height) <= limits.max_mega_pixels.saturating_mul(1_000_000)
        && width >= limits.min_resolution[0]
        && height >= limits.min_resolution[1]
}

/// Header value used for cache control
pub static CACHE_CONTROL: &str = "public, max-age=604800, must-revalidate";

/// Outcome of parsing a `Range` request header
#[derive(Debug, PartialEq, Eq)]
enum ByteRange {
    /// No usable range (absent, malformed, or multi-range): serve the whole file with 200
    Full,
    /// Inclusive byte range to serve with 206
    Partial(usize, usize),
    /// Well-formed but outside the file: respond 416
    Unsatisfiable,
}

/// Parse a single `bytes=` range per RFC 9110 §14
fn parse_byte_range(header: Option<&str>, total: usize) -> ByteRange {
    let Some(spec) = header.and_then(|h| h.trim().strip_prefix("bytes=")) else {
        return ByteRange::Full;
    };

    // Multi-range requests are optional to support; ignore them
    if spec.contains(',') {
        return ByteRange::Full;
    }

    let Some((start, end)) = spec.split_once('-') else {
        return ByteRange::Full;
    };
    let (start, end) = (start.trim(), end.trim());

    // Suffix range: last N bytes
    if start.is_empty() {
        let Ok(n) = end.parse::<usize>() else {
            return ByteRange::Full;
        };
        if n == 0 || total == 0 {
            return ByteRange::Unsatisfiable;
        }
        return ByteRange::Partial(total.saturating_sub(n), total - 1);
    }

    let Ok(start) = start.parse::<usize>() else {
        return ByteRange::Full;
    };
    let end = if end.is_empty() {
        usize::MAX
    } else {
        match end.parse::<usize>() {
            Ok(v) => v,
            Err(_) => return ByteRange::Full,
        }
    };

    if end < start {
        return ByteRange::Full; // invalid, ignore
    }
    if start >= total {
        return ByteRange::Unsatisfiable;
    }

    ByteRange::Partial(start, end.min(total - 1))
}

/// Build a file response, honouring `Range` requests
fn serve_bytes(
    data: Vec<u8>,
    method: &Method,
    request_headers: &HeaderMap,
    content_type: String,
    cache_control: &str,
) -> Response {
    let total = data.len();

    // Range is only defined for GET (RFC 9110 §14.2). Without an ETag/Last-Modified
    // we can't validate If-Range, so fall back to the full file.
    let range = if method != Method::GET || request_headers.contains_key(header::IF_RANGE) {
        ByteRange::Full
    } else {
        parse_byte_range(
            request_headers
                .get(header::RANGE)
                .and_then(|v| v.to_str().ok()),
            total,
        )
    };

    let base = [
        (header::CONTENT_TYPE, content_type),
        (header::CONTENT_DISPOSITION, "attachment".to_owned()),
        (header::CACHE_CONTROL, cache_control.to_owned()),
        (header::ACCEPT_RANGES, "bytes".to_owned()),
    ];

    match range {
        ByteRange::Full => (base, data).into_response(),
        ByteRange::Partial(start, end) => (
            StatusCode::PARTIAL_CONTENT,
            base,
            [(header::CONTENT_RANGE, format!("bytes {start}-{end}/{total}"))],
            data[start..=end].to_vec(),
        )
            .into_response(),
        ByteRange::Unsatisfiable => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [
                (header::ACCEPT_RANGES, "bytes".to_owned()),
                (header::CONTENT_RANGE, format!("bytes */{total}")),
            ],
        )
            .into_response(),
    }
}

/// Fetch preview of file
///
/// This route will only return image content. <br>
/// For all other file types, please use the fetch route (you will receive a redirect if you try to use this route anyways!).
///
/// Depending on the given tag, the file will be re-processed to fit the criteria:
///
/// | Tag | Image Resolution <sup>†</sup> | Animations stripped by preview <sup>‡</sup> |
/// | :-: | --- | :-: |
/// | attachments | Up to 1280px on any axis | ❌ |
/// | avatars | Up to 128px on any axis | ✅ |
/// | backgrounds | Up to 1280x720px | ❌ |
/// | icons | Up to 128px on any axis | ✅ |
/// | banners | Up to 480px on any axis | ❌ |
/// | emojis | Up to 128px on any axis | ❌ |
///
/// <sup>†</sup> aspect ratio will always be preserved
///
/// <sup>‡</sup> to fetch animated variant, suffix `/{file_name}` or `/original` to the path
#[utoipa::path(
    get,
    path = "/{tag}/{file_id}",
    tag = "Files",
    responses(
        (status = 200, description = "Generated preview", body = Vec<u8>)
    ),
    params(
        ("tag" = Tag, Path, description = "Tag to fetch from (e.g. attachments, icons, ...)"),
        ("file_id" = String, Path, description = "File identifier")
    ),
)]
async fn fetch_preview(
    State(db): State<Database>,
    Path((tag, file_id)): Path<(Tag, String)>,
) -> Result<Response> {
    let tag_str: &'static str = tag.clone().into();
    let file = db.fetch_attachment(tag_str, &file_id).await?;

    // Ignore deleted files
    if file.deleted.is_some_and(|v| v) {
        return Err(create_error!(NotFound));
    }

    // Ignore files that haven't been attached
    if file.used_for.is_none() {
        return Err(create_error!(NotFound));
    }

    let hash = file.as_hash(&db).await?;

    let mut data = None;

    // If animated is unset, check the file contents to see if it is animated and update the filehash
    let is_animated = match &hash.metadata {
        Metadata::Image {
            animated: Some(value),
            ..
        } => *value,
        Metadata::Image { animated: None, .. } => {
            let file_data = retrieve_file_by_hash(&hash).await?;

            let mut named_file = NamedTempFile::new().to_internal_error()?;
            named_file.write(&file_data).to_internal_error()?;

            data = Some(file_data);

            // If it fails for some reason, set it to not be animated
            let animated = is_animated(&named_file, &hash.content_type).unwrap_or(false);
            db.set_attachment_hash_animated(&hash.id, animated).await?;

            animated
        }
        _ => false,
    };

    // Only process image files and don't process GIFs if not avatar or icon
    if !matches!(hash.metadata, Metadata::Image { .. })
        || (is_animated && !matches!(tag, Tag::avatars | Tag::icons))
    {
        return Ok(
            Redirect::permanent(&format!("/{tag_str}/{file_id}/{}", file.filename)).into_response(),
        );
    }

    // Original image data
    let data = if let Some(data) = data {
        data
    } else {
        retrieve_file_by_hash(&hash).await?
    };

    // Read image and create thumbnail
    let data = create_thumbnail(
        decode_image(&mut Cursor::new(data), &file.content_type)?,
        tag_str,
    )
    .await;

    Ok((
        [
            (header::CONTENT_TYPE, "image/webp"),
            (header::CONTENT_DISPOSITION, "inline"),
            (header::CACHE_CONTROL, CACHE_CONTROL),
        ],
        data,
    )
        .into_response())
}

#[derive(Debug, Clone, Copy)]
struct MediaRange {
    start: usize,
    end: usize,
}

fn parse_range_header(
    headers: &HeaderMap,
    total_size: usize,
) -> std::result::Result<Option<MediaRange>, ()> {
    let Some(value) = headers.get(header::RANGE) else {
        return Ok(None);
    };

    let value = value.to_str().map_err(|_| ())?;

    let spec = value.strip_prefix("bytes=").ok_or(())?;

    // Multipart ranges are intentionally not supported.
    if spec.contains(',') {
        return Err(());
    }

    let (start, end) = spec.split_once('-').ok_or(())?;

    if total_size == 0 {
        return Err(());
    }

    if start.is_empty() {
        let suffix = end.parse::<usize>().map_err(|_| ())?;

        if suffix == 0 {
            return Err(());
        }

        let length = suffix.min(total_size);

        return Ok(Some(MediaRange {
            start: total_size - length,
            end: total_size - 1,
        }));
    }

    let start = start.parse::<usize>().map_err(|_| ())?;

    if start >= total_size {
        return Err(());
    }

    let end = if end.is_empty() {
        total_size - 1
    } else {
        end.parse::<usize>().map_err(|_| ())?.min(total_size - 1)
    };

    if end < start {
        return Err(());
    }

    Ok(Some(MediaRange { start, end }))
}

fn range_not_satisfiable(total_size: usize) -> Response {
    let mut response = StatusCode::RANGE_NOT_SATISFIABLE.into_response();

    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    if let Ok(value) = HeaderValue::from_str(&format!("bytes */{total_size}")) {
        response.headers_mut().insert(header::CONTENT_RANGE, value);
    }

    response
}

async fn build_streaming_file_response(
    path: &FsPath,
    content_type: &str,
    total_size: usize,
) -> Result<Response> {
    let file = fs::File::open(path).await.to_internal_error()?;

    let stream = ReaderStream::with_capacity(file, 256 * 1024);

    let mut response = Response::new(Body::from_stream(stream));

    *response.status_mut() = StatusCode::OK;

    let response_headers = response.headers_mut();

    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );

    response_headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("inline"),
    );

    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_CONTROL),
    );

    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&total_size.to_string()).unwrap(),
    );

    Ok(response)
}

fn build_metadata_response(
    headers: &HeaderMap,
    content_type: &str,
    total_size: usize,
    head_only: bool,
    body: Option<Vec<u8>>,
) -> Response {
    let range = match parse_range_header(headers, total_size) {
        Ok(range) => range,
        Err(()) => {
            return range_not_satisfiable(total_size);
        }
    };

    let (status, content_length, content_range) = match range {
        Some(MediaRange { start, end }) => (
            StatusCode::PARTIAL_CONTENT,
            end - start + 1,
            Some(format!("bytes {start}-{end}/{total_size}")),
        ),

        None => (StatusCode::OK, total_size, None),
    };

    let mut response = if head_only {
        status.into_response()
    } else if let Some(body) = body {
        (status, body).into_response()
    } else {
        status.into_response()
    };

    let response_headers = response.headers_mut();

    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );

    response_headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("inline"),
    );

    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_CONTROL),
    );

    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string()).unwrap(),
    );

    if let Some(content_range) = content_range {
        response_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&content_range).unwrap(),
        );
    }

    response
}

/// Fetch original file
///
/// Content disposition header will be set to 'attachment' to prevent browser from rendering anything.
///
/// Using `original` as the file name parameter will redirect you to the original file.
#[utoipa::path(
    get,
    path = "/{tag}/{file_id}/{file_name}",
    tag = "Files",
    responses(
        (status = 200, description = "Original file", body = Vec<u8>),
        (status = 206, description = "Partial content (Range request)", body = Vec<u8>),
        (status = 416, description = "Range not satisfiable")
    ),
    params(
        ("tag" = Tag, Path, description = "Tag to fetch from (e.g. attachments, icons, ...)"),
        ("file_id" = String, Path, description = "File identifier"),
        ("file_name" = String, Path, description = "File name")
    ),
)]
async fn fetch_file(
    State(db): State<Database>,
    headers: HeaderMap,
    Path((tag, file_id, file_name)): Path<(Tag, String, String)>,
) -> Result<Response> {
    let tag: &'static str = tag.clone().into();
    let file = db.fetch_attachment(tag, &file_id).await?;

    // Ignore deleted files
    if file.deleted.is_some_and(|v| v) {
        return Err(create_error!(NotFound));
    }

    // Ignore files that haven't been attached
    if file.used_for.is_none() {
        return Err(create_error!(NotFound));
    }

    // Ensure filename is correct
    if file_name != file.filename {
        if file_name == "original" {
            let safe_filename = encode_component(&file.filename);

            return Ok(
                Redirect::permanent(&format!("/{tag}/{file_id}/{}", safe_filename)).into_response(),
            );
        }

        return Err(create_error!(NotFound));
    }

    let hash = file.as_hash(&db).await?;

    let total_size = hash.size.max(0) as usize
        - if hash.iv.is_empty() {
            0
        } else {
            AUTHENTICATION_TAG_SIZE_BYTES
        };

    let requested_range = match parse_range_header(&headers, total_size) {
        Ok(range) => range,
        Err(()) => {
            return Ok(range_not_satisfiable(total_size));
        }
    };

    if total_size <= MEDIA_RAM_CACHE_LIMIT {
        let data = retrieve_file_by_hash(&hash).await?;

        let body = match requested_range {
            Some(MediaRange { start, end }) => data[start..=end].to_vec(),

            None => data,
        };

        return Ok(build_metadata_response(
            &headers,
            &hash.content_type,
            total_size,
            false,
            Some(body),
        ));
    }

    let cache_path = cached_media_path(&hash).await?;

    if let Some(range) = requested_range {
        let body = read_cached_range(&cache_path, range).await?;

        tokio::spawn(cleanup_media_disk_cache());

        return Ok(build_metadata_response(
            &headers,
            &hash.content_type,
            total_size,
            false,
            Some(body),
        ));
    }

    tokio::spawn(cleanup_media_disk_cache());

    build_streaming_file_response(&cache_path, &hash.content_type, total_size).await
}

async fn fetch_file_head(
    State(db): State<Database>,
    headers: HeaderMap,
    Path((tag, file_id, file_name)): Path<(Tag, String, String)>,
) -> Result<Response> {
    let tag: &'static str = tag.clone().into();
    let file = db.fetch_attachment(tag, &file_id).await?;

    if file.deleted.is_some_and(|v| v) {
        return Err(create_error!(NotFound));
    }

    if file.used_for.is_none() {
        return Err(create_error!(NotFound));
    }

    if file_name != file.filename {
        if file_name == "original" {
            let safe_filename = encode_component(&file.filename);

            return Ok(
                Redirect::permanent(&format!("/{tag}/{file_id}/{safe_filename}")).into_response(),
            );
        }

        return Err(create_error!(NotFound));
    }

    let hash = file.as_hash(&db).await?;

    let total_size = hash.size.max(0) as usize
        - if hash.iv.is_empty() {
            0
        } else {
            AUTHENTICATION_TAG_SIZE_BYTES
        };

    Ok(build_metadata_response(
        &headers,
        &hash.content_type,
        total_size,
        true,
        None,
    ))
}

/// Fetch original file (Moderation)
///
/// This is intentionally left out of the OpenApi docs.
/// It uses the config key api.security.admin_keys to determine access. This is intended for server to server communication.
async fn fetch_file_mod(
    State(db): State<Database>,
    method: Method,
    headers: HeaderMap,
    Path((tag, file_id, file_name)): Path<(Tag, String, String)>,
) -> Result<Response> {
    let config = revolt_config::config().await;

    let token = headers
        .get("X-Admin-Token")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| create_error!(NotAuthenticated))?;

    if !config
        .api
        .security
        .admin_keys
        .iter()
        .any(|tok| tok == token)
    {
        return Err(create_error!(NotAuthenticated));
    }

    let tag: &'static str = tag.clone().into();
    let file = db.fetch_attachment(tag, &file_id).await?;

    // Ignore files that haven't been attached
    if file.used_for.is_none() {
        return Err(create_error!(NotFound));
    }

    // Ensure filename is correct
    if file_name != file.filename {
        if file_name == "original" {
            let safe_filename = encode_component(&file.filename);

            return Ok(
                Redirect::permanent(&format!("/{tag}/{file_id}/{}", safe_filename)).into_response(),
            );
        }

        return Err(create_error!(NotFound));
    }

    let hash = file.as_hash(&db).await?;
    let data = retrieve_file_by_hash(&hash).await?;

    Ok(serve_bytes(
        data,
        &method,
        &headers,
        hash.content_type,
        "private, max-age=300, must-revalidate",
    ))
}

#[cfg(test)]
mod media_streaming_tests {
    use super::*;

    fn range_headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn parses_bounded_range() {
        let range = parse_range_header(&range_headers("bytes=100-199"), 1_000)
            .unwrap()
            .unwrap();
        assert_eq!(range.start, 100);
        assert_eq!(range.end, 199);
    }

    #[test]
    fn parses_open_ended_range() {
        let range = parse_range_header(&range_headers("bytes=900-"), 1_000)
            .unwrap()
            .unwrap();
        assert_eq!(range.start, 900);
        assert_eq!(range.end, 999);
    }

    #[test]
    fn parses_suffix_range() {
        let range = parse_range_header(&range_headers("bytes=-128"), 1_000)
            .unwrap()
            .unwrap();
        assert_eq!(range.start, 872);
        assert_eq!(range.end, 999);
    }

    #[test]
    fn clamps_range_end_to_file_size() {
        let range = parse_range_header(&range_headers("bytes=900-5000"), 1_000)
            .unwrap()
            .unwrap();
        assert_eq!(range.start, 900);
        assert_eq!(range.end, 999);
    }

    #[test]
    fn rejects_invalid_and_multi_ranges() {
        assert!(parse_range_header(&range_headers("bytes=1000-1001"), 1_000).is_err());
        assert!(parse_range_header(&range_headers("bytes=200-100"), 1_000).is_err());
        assert!(parse_range_header(&range_headers("bytes=0-1,4-5"), 1_000).is_err());
        assert!(parse_range_header(&range_headers("items=0-1"), 1_000).is_err());
    }

    #[test]
    fn partial_response_has_http_range_contract() {
        let response = build_metadata_response(
            &range_headers("bytes=1024-2047"),
            "video/webm",
            10_000,
            false,
            Some(vec![0; 1024]),
        );

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(
            response.headers()[header::CONTENT_RANGE],
            "bytes 1024-2047/10000"
        );
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "1024");
        assert_eq!(response.headers()[header::CONTENT_TYPE], "video/webm");
    }

    #[test]
    fn invalid_range_returns_416_contract() {
        let response = build_metadata_response(
            &range_headers("bytes=20000-21000"),
            "video/webm",
            10_000,
            false,
            None,
        );

        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes */10000");
    }

    #[test]
    fn head_response_needs_only_metadata() {
        let response = build_metadata_response(
            &HeaderMap::new(),
            "video/mp4",
            8_000_000_000usize,
            true,
            None,
        );

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "8000000000");
        assert_eq!(response.headers()[header::CONTENT_TYPE], "video/mp4");
    }

    #[tokio::test]
    async fn disk_range_reader_reads_only_requested_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("media.bin");
        let payload: Vec<u8> = (0..=255).cycle().take(32 * 1024).collect();
        std::fs::write(&path, &payload).unwrap();

        let range = MediaRange {
            start: 4_321,
            end: 8_765,
        };
        let data = read_cached_range(&path, range).await.unwrap();

        assert_eq!(data.len(), range.end - range.start + 1);
        assert_eq!(data, payload[range.start..=range.end]);
    }

    #[tokio::test]
    async fn full_disk_response_is_streaming_and_has_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("video.webm");
        let payload = vec![0x5a; 2 * 1024 * 1024];
        std::fs::write(&path, &payload).unwrap();

        let response = build_streaming_file_response(&path, "video/webm", payload.len())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            payload.len().to_string()
        );
        assert_eq!(response.headers()[header::CONTENT_TYPE], "video/webm");
    }
}

mod tests {
    use super::{image_within_limits, parse_byte_range, ByteRange, serve_bytes, CACHE_CONTROL};
    use axum::http::{header, HeaderMap, Method, StatusCode};
    use revolt_config::FilesLimit;

    fn limits() -> FilesLimit {
        FilesLimit {
            min_file_size: 1,
            min_resolution: [1, 1],
            max_mega_pixels: 40,
            max_pixel_side: 10_000,
        }
    }

    #[test]
    fn accepts_images_within_limits() {
        let limits = limits();
        assert!(image_within_limits(1920, 1080, &limits));
        assert!(image_within_limits(10_000, 1, &limits));
        assert!(image_within_limits(6324, 6324, &limits));
    }

    #[test]
    fn rejects_images_exceeding_pixel_side() {
        assert!(!image_within_limits(10_001, 100, &limits()));
        assert!(!image_within_limits(100, 10_001, &limits()));
    }

    #[test]
    fn rejects_images_exceeding_mega_pixels() {
        // 40 MP is the limit, anything above must be rejected even if each side
        // is within max_pixel_side.
        assert!(!image_within_limits(6325, 6325, &limits()));
        assert!(!image_within_limits(10_000, 10_000, &limits()));
    }

    #[test]
    fn rejects_images_below_min_resolution() {
        assert!(!image_within_limits(0, 100, &limits()));
        assert!(!image_within_limits(100, 0, &limits()));
    }

    #[test]
    fn range_basic() {
        assert_eq!(parse_byte_range(Some("bytes=0-1"), 100), ByteRange::Partial(0, 1));
        assert_eq!(parse_byte_range(Some("bytes=0-"), 100), ByteRange::Partial(0, 99));
        assert_eq!(parse_byte_range(Some("bytes=50-999"), 100), ByteRange::Partial(50, 99));
    }

    #[test]
    fn range_suffix() {
        assert_eq!(parse_byte_range(Some("bytes=-10"), 100), ByteRange::Partial(90, 99));
        assert_eq!(parse_byte_range(Some("bytes=-500"), 100), ByteRange::Partial(0, 99));
        assert_eq!(parse_byte_range(Some("bytes=-0"), 100), ByteRange::Unsatisfiable);
    }

    #[test]
    fn range_unsatisfiable() {
        assert_eq!(parse_byte_range(Some("bytes=100-"), 100), ByteRange::Unsatisfiable);
        assert_eq!(parse_byte_range(Some("bytes=0-1"), 0), ByteRange::Unsatisfiable);
    }

    #[test]
    fn range_ignored() {
        assert_eq!(parse_byte_range(None, 100), ByteRange::Full);
        assert_eq!(parse_byte_range(Some("bytes=5-2"), 100), ByteRange::Full);
        assert_eq!(parse_byte_range(Some("bytes=0-1,4-5"), 100), ByteRange::Full);
        assert_eq!(parse_byte_range(Some("items=0-1"), 100), ByteRange::Full);
        assert_eq!(parse_byte_range(Some("bytes=a-b"), 100), ByteRange::Full);
    }

    #[test]
    fn head_ignores_range() {
        let mut h = HeaderMap::new();
        h.insert(header::RANGE, "bytes=0-1".parse().unwrap());

        let res = serve_bytes(vec![0u8; 100], &Method::HEAD, &h, "video/mp4".into(), CACHE_CONTROL);
        assert_eq!(res.status(), StatusCode::OK);

        let res = serve_bytes(vec![0u8; 100], &Method::GET, &h, "video/mp4".into(), CACHE_CONTROL);
        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    }
}
