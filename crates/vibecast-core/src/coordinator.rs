//! Per-session playback state and MEDIA_STATUS construction.
//!
//! This module owns only the serialized playback state and the status builders.
//! The surrounding IO (sending to senders, driving the player, registering
//! proxies) lives in the hub, which owns the transport registry.

use serde_json::Value;
use vibecast_messages::{
    media_command, ExtendedStatus, LoadRequest, MediaCategory, MediaInfo, MediaMetadata,
    MediaStatus, MediaStatusResponse, PlayerState, RepeatMode, StreamType, Volume,
};
use vibecast_sdk::PlaybackMedia;

const LOADING_PLAYER_STATE: &str = "LOADING";

/// Serialized playback state for one app session.
pub(crate) struct Coordinator {
    pub media_session_id: i64,
    pub player_state: PlayerState,
    pub current_time: f64,
    pub idle_reason: Option<vibecast_messages::IdleReason>,
    pub current_media: Option<MediaInfo>,
    pub playback_media: Option<PlaybackMedia>,
    pub volume: Volume,
}

impl Coordinator {
    /// Replace only matching active artwork; never change the playback clock.
    pub(crate) fn update_artwork(&mut self, source: &str, url: &str) -> bool {
        if self.player_state == PlayerState::Idle
            || source.is_empty()
            || !(url.starts_with("http://") || url.starts_with("https://"))
            || url.len() > 8192
            || url.chars().any(char::is_control)
        {
            return false;
        }
        let Some(metadata) = self
            .current_media
            .as_mut()
            .and_then(|m| m.metadata.as_mut())
        else {
            return false;
        };
        let Some(image) = metadata.images.first_mut().filter(|i| i.url == source) else {
            return false;
        };
        image.url = url.to_owned();
        image.width = None;
        image.height = None;
        // Alternate original thumbnails must not bypass the processed square.
        metadata.images.truncate(1);
        true
    }

    pub(crate) fn new(volume: Volume) -> Self {
        Self {
            media_session_id: 1,
            player_state: PlayerState::Idle,
            current_time: 0.0,
            idle_reason: None,
            current_media: None,
            playback_media: None,
            volume,
        }
    }

    /// Transition to a terminal IDLE state.
    pub(crate) fn set_idle(&mut self, idle_reason: Option<vibecast_messages::IdleReason>) {
        self.player_state = PlayerState::Idle;
        self.current_time = 0.0;
        self.idle_reason = idle_reason;
    }

    /// Clear the current media descriptors.
    pub(crate) fn clear_media(&mut self) {
        self.current_media = None;
        self.playback_media = None;
    }

    /// Build the current MEDIA_STATUS response (empty status when idle with no
    /// media and no idle reason).
    pub(crate) fn status_response(&self, request_id: i64) -> MediaStatusResponse {
        MediaStatusResponse::new(
            request_id,
            self.media_status()
                .map(|status| vec![status])
                .unwrap_or_default(),
        )
    }

    fn media_status(&self) -> Option<MediaStatus> {
        if self.current_media.is_none() && self.idle_reason.is_none() {
            return None;
        }
        let is_idle = self.player_state == PlayerState::Idle;
        let is_active = matches!(
            self.player_state,
            PlayerState::Playing | PlayerState::Paused | PlayerState::Buffering
        );
        let mut commands = if is_active {
            media_command::ACTIVE
        } else {
            media_command::IDLE
        };
        // No seekable/DVR range is modeled for live sources yet.
        if self
            .current_media
            .as_ref()
            .is_some_and(|media| media.stream_type == StreamType::Live)
        {
            commands &= !media_command::SEEK;
        }
        let playback_rate = if self.player_state == PlayerState::Playing {
            1.0
        } else {
            0.0
        };
        Some(MediaStatus {
            media_session_id: self.media_session_id,
            media: if is_idle {
                None
            } else {
                self.current_media.clone()
            },
            player_state: self.player_state,
            current_time: self.current_time,
            supported_media_commands: commands,
            volume: Some(self.volume.clone()),
            idle_reason: self.idle_reason,
            custom_data: None,
            playback_rate: Some(playback_rate),
            current_item_id: Some(1),
            repeat_mode: if is_active {
                Some(RepeatMode::RepeatOff)
            } else {
                None
            },
            extended_status: None,
        })
    }

    /// Build an IDLE + LOADING extended status during media resolution.
    pub(crate) fn loading_response(
        &self,
        request_id: i64,
        media: &MediaInfo,
    ) -> MediaStatusResponse {
        let status = MediaStatus {
            media_session_id: self.media_session_id,
            media: Some(media.clone()),
            player_state: PlayerState::Idle,
            current_time: 0.0,
            supported_media_commands: media_command::IDLE,
            volume: Some(self.volume.clone()),
            idle_reason: None,
            custom_data: None,
            playback_rate: Some(1.0),
            current_item_id: Some(1),
            repeat_mode: Some(RepeatMode::RepeatOff),
            extended_status: Some(ExtendedStatus {
                player_state: LOADING_PLAYER_STATE.to_string(),
                media: Some(media.clone()),
                media_session_id: Some(self.media_session_id),
            }),
        };
        MediaStatusResponse::new(request_id, vec![status])
    }
}

/// Build a minimal `MediaInfo` from the original LOAD request for the initial
/// LOADING broadcast (before the app resolves streams).
pub(crate) fn loading_media_info(request: &LoadRequest) -> MediaInfo {
    let content_type = if request.media.content_type.is_empty() {
        "video/*".to_string()
    } else {
        request.media.content_type.clone()
    };
    let category = category_for_content_type(&content_type);
    MediaInfo {
        content_id: request.media.content_id.clone(),
        content_type,
        stream_type: StreamType::None,
        metadata: request.media.metadata.clone(),
        duration: Some(0.0),
        custom_data: None,
        content_url: None,
        media_category: Some(category),
        start_absolute_time: None,
        is_live_media: None,
    }
}

/// Build a fully resolved `MediaInfo` from app-resolved media.
pub(crate) fn media_info(media: &PlaybackMedia) -> MediaInfo {
    let primary = media.streams.first();
    let metadata = if media.metadata.is_some() {
        media.metadata.clone()
    } else if media.title.is_some() || media.subtitle.is_some() || !media.images.is_empty() {
        Some(MediaMetadata {
            title: media.title.clone(),
            subtitle: media.subtitle.clone(),
            images: media.images.clone(),
            ..MediaMetadata::default()
        })
    } else {
        None
    };
    let content_id = media
        .content_id
        .clone()
        .or_else(|| {
            primary
                .and_then(|stream| stream.source.as_url())
                .map(str::to_string)
        })
        .unwrap_or_default();
    let is_live = media.stream_type == StreamType::Live;
    MediaInfo {
        content_id,
        content_type: primary
            .map(|stream| stream.content_type.clone())
            .unwrap_or_default(),
        stream_type: media.stream_type,
        metadata,
        duration: media.duration,
        custom_data: media.custom_data.clone().filter(is_non_empty_object),
        content_url: primary
            .and_then(|stream| stream.source.as_url())
            .map(str::to_string),
        media_category: Some(category_for_content_type(
            primary
                .map(|stream| stream.content_type.as_str())
                .unwrap_or_default(),
        )),
        start_absolute_time: None,
        is_live_media: if is_live { Some(true) } else { None },
    }
}

fn category_for_content_type(content_type: &str) -> MediaCategory {
    // Containers such as DASH/HLS do not establish an audio-only track.
    // Preserve the old fallback unless the MIME type explicitly identifies audio.
    if content_type
        .trim()
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("audio/"))
    {
        MediaCategory::Audio
    } else {
        MediaCategory::Video
    }
}

fn is_non_empty_object(value: &Value) -> bool {
    !matches!(value, Value::Object(map) if map.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artwork_replaces_only_matching_active_image_without_clock_changes() {
        let mut c = Coordinator::new(Volume::default());
        c.current_media = Some(
            serde_json::from_value(serde_json::json!({
                "contentId": "song", "contentType": "audio/mpeg", "streamType": "BUFFERED",
                "metadata": {"metadataType": 3, "title": "Track", "images": [
                    {"url": "https://example.test/a", "width": 640, "height": 360}]}
            }))
            .unwrap(),
        );
        c.player_state = PlayerState::Playing;
        c.current_time = 42.0;
        assert!(!c.update_artwork(
            "https://example.test/old",
            "http://localhost/artwork/id.jpg"
        ));
        assert!(c.update_artwork("https://example.test/a", "http://localhost/artwork/id.jpg"));
        assert_eq!(c.current_time, 42.0);
        assert_eq!(c.player_state, PlayerState::Playing);
        let m = c.current_media.as_ref().unwrap().metadata.as_ref().unwrap();
        assert_eq!(m.title.as_deref(), Some("Track"));
        assert_eq!(m.images[0].url, "http://localhost/artwork/id.jpg");
        assert_eq!(m.images[0].width, None);
        c.player_state = PlayerState::Idle;
        assert!(!c.update_artwork("http://localhost/artwork/id.jpg", "http://localhost/other"));
    }

    #[test]
    fn audio_load_reports_audio_category_before_resolution() {
        for mime in ["audio/wav", "audio/mpeg", " Audio/FLAC; codecs=flac "] {
            let request: LoadRequest = serde_json::from_value(serde_json::json!({
                "requestId": 1,
                "media": {"contentId": "https://example.test/track", "contentType": mime}
            }))
            .unwrap();
            assert_eq!(
                loading_media_info(&request).media_category,
                Some(MediaCategory::Audio)
            );
        }
        for mime in [
            "video/mp4",
            "application/dash+xml",
            "application/vnd.apple.mpegurl",
            "",
        ] {
            assert_eq!(category_for_content_type(mime), MediaCategory::Video);
        }
    }

    #[test]
    fn typed_music_metadata_reaches_status_and_player_payload() {
        let original: MediaMetadata = serde_json::from_value(serde_json::json!({
            "metadataType": 3, "title": "Track", "artist": "Artist",
            "albumName": "Album", "albumArtist": "Album artist",
            "images": [{"url": "https://example.test/cover"}]
        }))
        .unwrap();
        let mut media = PlaybackMedia::new("session", Vec::new(), StreamType::Buffered);
        media.title = Some("legacy title".into());
        media.metadata = Some(original.clone());
        assert_eq!(media_info(&media).metadata, Some(original.clone()));
        assert_eq!(crate::proxy::to_payload(&media).metadata, Some(original));
        media.metadata = None;
        let legacy = media_info(&media).metadata.unwrap();
        assert_eq!(legacy.title.as_deref(), Some("legacy title"));
        assert!(legacy.artist.is_none());
        assert_eq!(legacy.metadata_type, 0);
    }

    #[test]
    fn live_status_does_not_advertise_seek() {
        let mut coordinator = Coordinator::new(Volume::default());
        coordinator.player_state = PlayerState::Playing;
        let media: MediaInfo = serde_json::from_value(serde_json::json!({
            "contentId": "https://example.test/radio", "contentType": "audio/mpeg",
            "streamType": "LIVE"
        }))
        .unwrap();
        coordinator.current_media = Some(media);
        let status = coordinator.media_status().unwrap();
        assert_eq!(status.supported_media_commands & media_command::SEEK, 0);
        assert_ne!(
            status.supported_media_commands & media_command::STREAM_VOLUME,
            0
        );
        coordinator.current_media.as_mut().unwrap().stream_type = StreamType::Buffered;
        assert_ne!(
            coordinator.media_status().unwrap().supported_media_commands & media_command::SEEK,
            0
        );
    }
}
