//! Owned data types exchanged between apps and the coordinator.

use serde_json::Value;
use vibecast_messages::{IdleReason, MediaImage, MediaMetadata, PlayerState, StreamType};

/// Supported DRM key systems.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrmSystem {
    /// Google Widevine (`com.widevine.alpha`).
    Widevine,
    /// Microsoft PlayReady (`com.microsoft.playready`).
    PlayReady,
    /// W3C ClearKey (`org.w3.clearkey`).
    ClearKey,
    /// Apple FairPlay Streaming (`com.apple.fps`).
    FairPlay,
}

/// DRM configuration for a protected stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrmInfo {
    /// Key system.
    pub system: DrmSystem,
    /// License acquisition URL.
    pub license_url: String,
    /// Extra headers required for license acquisition.
    pub headers: std::collections::HashMap<String, String>,
}

impl DrmInfo {
    /// Build DRM info with no extra headers.
    #[must_use]
    pub fn new(system: DrmSystem, license_url: impl Into<String>) -> Self {
        Self {
            system,
            license_url: license_url.into(),
            headers: std::collections::HashMap::new(),
        }
    }
}

/// Where a stream's manifest/media bytes come from.
///
/// Most apps point at a [`Url`](StreamSource::Url) the proxy fetches (and, for
/// DASH/HLS, normalizes). Apps that construct a manifest themselves (e.g.
/// YouTube synthesizing a DASH MPD from adaptive formats) return an
/// [`InlineManifest`](StreamSource::InlineManifest) body, which the bridge
/// serves verbatim from the session manifest route — so any player (browser
/// Shaka, Kodi InputStream Adaptive, …) consumes it as an ordinary proxied
/// manifest URL. Segment/media URLs inside an inline manifest must be absolute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamSource {
    /// Fetch (and normalize, for DASH/HLS) the manifest/media from this URL.
    Url(String),
    /// Progressive clear media eligible for a bounded, current-title RAM cache.
    CachedUrl { url: String, cache: MediaCacheHint },
    /// Serve this app-generated manifest body directly. The `content_type`
    /// on the owning [`PlaybackStream`] selects the manifest kind.
    InlineManifest(String),
}

impl StreamSource {
    /// The fetch URL if this is a [`Url`](StreamSource::Url) source.
    #[must_use]
    pub fn as_url(&self) -> Option<&str> {
        match self {
            StreamSource::Url(url) | StreamSource::CachedUrl { url, .. } => Some(url),
            StreamSource::InlineManifest(_) => None,
        }
    }
}

/// Shared cache identity/status and optional source refresh contract; no media bytes.
/// Only a fully downloaded entry is safe to replay after its source URL expires.
#[derive(Debug, Clone, Default)]
pub struct MediaCacheHint(std::sync::Arc<MediaCacheState>);

#[derive(Debug, Default)]
struct MediaCacheState {
    status: std::sync::atomic::AtomicU8,
    recovery_attempts: std::sync::atomic::AtomicU8,
    refresh: std::sync::Mutex<Option<std::sync::Arc<dyn MediaSourceRefresh>>>,
}

/// Refresh an expired progressive URL without replacing the playing object.
/// Implementations must only return the same immutable byte representation;
/// changing formats would corrupt readers already consuming the cached bytes.
#[async_trait::async_trait]
pub trait MediaSourceRefresh: Send + Sync + std::fmt::Debug {
    async fn refresh(&self) -> Option<String>;
}

impl PartialEq for MediaCacheHint {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for MediaCacheHint {}

impl MediaCacheHint {
    pub fn is_complete(&self) -> bool {
        self.0.status.load(std::sync::atomic::Ordering::Acquire) == 1
    }
    pub fn is_failed(&self) -> bool {
        self.0.status.load(std::sync::atomic::Ordering::Acquire) == 2
    }
    /// Receiver-side completion notification.
    pub fn mark_complete(&self) {
        self.0.status.store(1, std::sync::atomic::Ordering::Release);
    }
    /// Receiver-side failure/cancellation notification.
    pub fn invalidate(&self) {
        self.0.status.store(2, std::sync::atomic::Ordering::Release);
    }

    pub fn set_refresh(&self, refresh: std::sync::Arc<dyn MediaSourceRefresh>) {
        *self.0.refresh.lock().unwrap() = Some(refresh);
    }

    pub fn source_refresh(&self) -> Option<std::sync::Arc<dyn MediaSourceRefresh>> {
        self.0.refresh.lock().unwrap().clone()
    }

    pub fn recovery_attempts(&self) -> u8 {
        self.0
            .recovery_attempts
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Carry the selection's consumed budget into a replacement player load.
    pub fn set_recovery_attempts(&self, attempts: u8) {
        self.0
            .recovery_attempts
            .fetch_max(attempts.min(2), std::sync::atomic::Ordering::AcqRel);
    }

    /// Reserve one of at most two automatic recovery rounds for this selection.
    pub fn try_recovery(&self) -> bool {
        self.0
            .recovery_attempts
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |attempts| (attempts < 2).then(|| attempts + 1),
            )
            .is_ok()
    }
}

/// A single playable stream candidate with optional DRM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackStream {
    /// Where the stream's manifest/media bytes come from.
    pub source: StreamSource,
    /// MIME type.
    pub content_type: String,
    /// DRM configuration, if the stream is protected.
    pub drm: Option<DrmInfo>,
}

impl PlaybackStream {
    /// Cache only the current progressive title, never a playlist/archive.
    pub fn cached_url(url: impl Into<String>, content_type: impl Into<String>) -> Self {
        Self {
            source: StreamSource::CachedUrl {
                url: url.into(),
                cache: MediaCacheHint::default(),
            },
            content_type: content_type.into(),
            drm: None,
        }
    }
    /// A clear stream the proxy fetches from `url`.
    #[must_use]
    pub fn url(url: impl Into<String>, content_type: impl Into<String>) -> Self {
        Self {
            source: StreamSource::Url(url.into()),
            content_type: content_type.into(),
            drm: None,
        }
    }

    /// A clear stream served from an app-generated manifest `body`.
    #[must_use]
    pub fn inline_manifest(body: impl Into<String>, content_type: impl Into<String>) -> Self {
        Self {
            source: StreamSource::InlineManifest(body.into()),
            content_type: content_type.into(),
            drm: None,
        }
    }

    /// Attach DRM configuration to this stream.
    #[must_use]
    pub fn with_drm(mut self, drm: DrmInfo) -> Self {
        self.drm = Some(drm);
        self
    }
}

/// Canonical media description returned by an app's `resolve_media`.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaybackMedia {
    /// Owning session id.
    pub session_id: String,
    /// Stream candidates in preference order.
    pub streams: Vec<PlaybackStream>,
    /// On-demand vs live.
    pub stream_type: StreamType,
    /// Original content identifier from the LOAD request.
    pub content_id: Option<String>,
    /// Original typed metadata when the provider preserves sender metadata.
    /// None retains the legacy title/subtitle/images normalization.
    pub metadata: Option<MediaMetadata>,
    /// Display title.
    pub title: Option<String>,
    /// Display subtitle.
    pub subtitle: Option<String>,
    /// Poster / artwork images.
    pub images: Vec<MediaImage>,
    /// Duration in seconds, if known.
    pub duration: Option<f64>,
    /// Whether playback starts automatically.
    pub autoplay: bool,
    /// Resume position in seconds.
    pub start_time: f64,
    /// App-specific data echoed to senders (`None` = omit).
    pub custom_data: Option<Value>,
}

impl PlaybackMedia {
    /// Build media with the common defaults (autoplay, no metadata).
    #[must_use]
    pub fn new(
        session_id: impl Into<String>,
        streams: Vec<PlaybackStream>,
        stream_type: StreamType,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            streams,
            stream_type,
            content_id: None,
            metadata: None,
            title: None,
            subtitle: None,
            images: Vec::new(),
            duration: None,
            autoplay: true,
            start_time: 0.0,
            custom_data: None,
        }
    }
}

/// Canonical playback state reported to apps via `on_playback_update`.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaybackState {
    /// Playback state.
    pub player_state: PlayerState,
    /// Current position in seconds.
    pub current_time: f64,
    /// Total duration in seconds, if known.
    pub duration: Option<f64>,
    /// Reason for entering IDLE, if applicable.
    pub idle_reason: Option<IdleReason>,
    /// Automatic source recovery rounds already consumed for this load.
    /// Apps carry this count into decoder-level retries instead of granting
    /// another budget after the progressive cache has exhausted its attempts.
    pub media_recovery_attempts: u8,
}

/// User control forwarded to an app from Cast or a physical output device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputControl {
    Play,
    Next,
    Previous,
}

/// Credentials supplied with a `LAUNCH` request.
#[derive(Debug, Clone, Default)]
pub struct LaunchCredentials {
    /// Opaque credentials blob.
    pub credentials: Option<String>,
    /// Credentials type discriminator.
    pub credentials_type: Option<String>,
}
