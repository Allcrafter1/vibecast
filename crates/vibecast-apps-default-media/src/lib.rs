//! Single-item HTTP(S) Default Media Receiver application.

#![forbid(unsafe_code)]

use std::sync::Arc;

use async_trait::async_trait;
use url::Url;
use vibecast_sdk::{
    normalize_stream_type, AppContext, AppManifest, AppProvider, AppSession, LaunchCredentials,
    LaunchError, LoadRequest, MediaResolveError, PlaybackMedia, PlaybackStream, StreamSource,
    StreamType,
};

/// Direct-media application, without service-specific extraction or DRM.
#[derive(Debug, Default)]
pub struct DefaultMedia;

#[async_trait]
impl AppProvider for DefaultMedia {
    fn manifest(&self) -> AppManifest {
        AppManifest::without_settings("default_media", &["CC1AD845"], "Default Media Receiver")
    }

    async fn launch(
        &self,
        _ctx: &AppContext,
        _credentials: LaunchCredentials,
    ) -> Result<Arc<dyn AppSession>, LaunchError> {
        Ok(Arc::new(DefaultMediaSession))
    }
}

struct DefaultMediaSession;

#[async_trait]
impl AppSession for DefaultMediaSession {
    async fn resolve_media(
        &self,
        ctx: &AppContext,
        request: &LoadRequest,
    ) -> Result<PlaybackMedia, MediaResolveError> {
        resolve(&ctx.session_id, request)
    }
}

fn resolve(session_id: &str, request: &LoadRequest) -> Result<PlaybackMedia, MediaResolveError> {
    let media = &request.media;
    let source = media.content_url.as_deref().unwrap_or(&media.content_id);
    let url =
        Url::parse(source).map_err(|_| MediaResolveError::invalid_request("INVALID_MEDIA_URL"))?;
    // Keep LAN URLs: this is how local media and TTS reach a Cast receiver.
    // Do not allow arbitrary decoder protocols, local paths or URL credentials.
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || source.chars().any(char::is_control)
    {
        return Err(MediaResolveError::invalid_request("INVALID_MEDIA_URL"));
    }
    if !request.current_time.is_finite()
        || request.current_time < 0.0
        || media
            .duration
            .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err(MediaResolveError::invalid_request("INVALID_MEDIA_TIME"));
    }
    if media.content_type.trim().is_empty() || media.content_type.chars().any(char::is_control) {
        return Err(MediaResolveError::invalid_request("INVALID_CONTENT_TYPE"));
    }
    let stream_type = if media.is_live_media == Some(true) {
        StreamType::Live
    } else {
        normalize_stream_type(media.stream_type)
    };
    if stream_type == StreamType::Live && request.current_time > 0.0 {
        return Err(MediaResolveError::invalid_request("LIVE_SEEK_UNSUPPORTED"));
    }
    let metadata = media.metadata.as_ref();
    Ok(PlaybackMedia {
        session_id: session_id.to_owned(),
        streams: vec![PlaybackStream {
            // Preserve original escaping and signatures rather than reserializing Url.
            source: StreamSource::Url(source.to_owned()),
            content_type: media.content_type.clone(),
            drm: None,
        }],
        stream_type,
        content_id: Some(media.content_id.clone()),
        metadata: media.metadata.clone(),
        title: metadata.and_then(|value| value.title.clone()),
        subtitle: metadata.and_then(|value| value.subtitle.clone()),
        images: metadata
            .map(|value| value.images.clone())
            .unwrap_or_default(),
        duration: media.duration,
        autoplay: request.autoplay,
        start_time: request.current_time,
        custom_data: media.custom_data.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request() -> LoadRequest {
        serde_json::from_value(json!({
            "requestId": 1,
            "media": {
                "contentId": "http://192.168.1.2/audio.mp3",
                "contentType": "audio/mpeg",
                "duration": 42.0,
                "metadata": {"title": "Track", "subtitle": "Artist",
                    "images": [{"url": "https://example.test/cover.jpg"}]}
            },
            "autoplay": false,
            "currentTime": 12.5
        }))
        .unwrap()
    }

    #[test]
    fn manifest_advertises_only_default_id() {
        let manifest = DefaultMedia.manifest();
        assert_eq!(manifest.app_ids, &["CC1AD845"]);
        assert!(manifest.namespaces.is_empty());
    }

    #[test]
    fn preserves_direct_media_and_controls_without_network_resolution() {
        let playback = resolve("session", &request()).unwrap();
        assert_eq!(
            playback.streams[0].source.as_url(),
            Some("http://192.168.1.2/audio.mp3")
        );
        assert_eq!(playback.title.as_deref(), Some("Track"));
        assert_eq!(playback.subtitle.as_deref(), Some("Artist"));
        assert_eq!(playback.images.len(), 1);
        assert_eq!(playback.duration, Some(42.0));
        assert_eq!(playback.start_time, 12.5);
        assert!(!playback.autoplay);
        assert_eq!(playback.stream_type, StreamType::Buffered);
    }

    #[test]
    fn content_url_overrides_logical_id_without_rewriting_signature() {
        let mut load = request();
        load.media.content_id = "logical-id".into();
        let source = "https://example.test/a%2fb?token=a%2Bb&x=1";
        load.media.content_url = Some(source.into());
        let playback = resolve("session", &load).unwrap();
        assert_eq!(playback.streams[0].source.as_url(), Some(source));
        assert_eq!(playback.content_id.as_deref(), Some("logical-id"));
    }

    #[test]
    fn music_metadata_is_retained_without_inventing_missing_fields() {
        let mut load = request();
        load.media.metadata = Some(
            serde_json::from_value(json!({
                "metadataType": 3, "title": "Track", "artist": "Artist",
                "albumName": "Album", "albumArtist": "Album artist"
            }))
            .unwrap(),
        );
        let playback = resolve("session", &load).unwrap();
        assert_eq!(playback.metadata, load.media.metadata);
        load.media.metadata = None;
        assert!(resolve("session", &load).unwrap().metadata.is_none());
    }

    #[test]
    fn invalid_explicit_url_does_not_fall_back_to_content_id() {
        let mut load = request();
        load.media.content_url = Some("file:///tmp/audio".into());
        assert!(resolve("session", &load).is_err());
    }

    #[test]
    fn rejects_non_web_sources_and_credentials() {
        for source in [
            "file:///tmp/audio",
            "pipe:0",
            "concat:a|b",
            "/tmp/audio",
            "https://user:password@example.test/a",
            "https://example.test/a\n",
        ] {
            let mut load = request();
            load.media.content_id = source.into();
            assert!(
                resolve("session", &load).is_err(),
                "accepted invalid source"
            );
        }
    }

    #[test]
    fn rejects_invalid_timing_and_content_type() {
        for value in [-1.0, f64::NAN, f64::INFINITY] {
            let mut load = request();
            load.current_time = value;
            assert!(resolve("session", &load).is_err());
            load.current_time = 0.0;
            load.media.duration = Some(value);
            assert!(resolve("session", &load).is_err());
        }
        for mime in ["", "audio/mpeg\r\nx: y"] {
            let mut load = request();
            load.media.content_type = mime.into();
            assert!(resolve("session", &load).is_err());
        }
    }

    #[test]
    fn live_source_preserves_unknown_duration() {
        let mut load = request();
        load.current_time = 0.0;
        load.media.duration = None;
        load.media.stream_type = StreamType::Live;
        assert_eq!(
            resolve("session", &load).unwrap().stream_type,
            StreamType::Live
        );
        load.current_time = 1.0;
        assert!(resolve("session", &load).is_err());
        load.current_time = 0.0;
        assert_eq!(resolve("session", &load).unwrap().duration, None);
        load.media.stream_type = StreamType::Buffered;
        load.media.is_live_media = Some(true);
        assert_eq!(
            resolve("session", &load).unwrap().stream_type,
            StreamType::Live
        );
    }
}
