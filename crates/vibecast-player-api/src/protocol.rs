//! WebSocket wire protocol between the bridge and external players.
//!
//! [`PlayerCommand`] flows bridge → player; [`PlayerReport`] flows player →
//! bridge. Both are `#[serde(tag = "type")]` enums with camelCase field names
//! on the wire.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use vibecast_messages::{IdleReason, MediaImage, MediaMetadata, PlayerState, StreamType};
use vibecast_settings::SettingValue;

/// Current external-player wire protocol. Missing registration versions are
/// treated as legacy v1; v2 adds explicit output-origin control requests.
pub const PLAYER_PROTOCOL_VERSION: u16 = 2;

/// Supported DRM key systems (EME key-system identifiers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrmSystem {
    /// Google Widevine.
    #[serde(rename = "com.widevine.alpha")]
    Widevine,
    /// Microsoft PlayReady.
    #[serde(rename = "com.microsoft.playready")]
    PlayReady,
    /// W3C ClearKey.
    #[serde(rename = "org.w3.clearkey")]
    ClearKey,
    /// Apple FairPlay Streaming.
    #[serde(rename = "com.apple.fps")]
    FairPlay,
}

/// DRM configuration attached to a stream on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrmPayload {
    /// Key system.
    pub system: DrmSystem,
    /// License acquisition URL (rewritten to the bridge proxy by the coordinator).
    pub license_url: String,
    /// Extra headers the player should attach to license requests.
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// One playable stream candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaybackStreamPayload {
    /// Manifest / media URL.
    pub url: String,
    /// MIME type.
    pub content_type: String,
    /// DRM configuration, if the stream is protected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drm: Option<DrmPayload>,
}

/// Media description carried by a `load` command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaybackMediaPayload {
    /// Original typed sender metadata, when retained by the app provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MediaMetadata>,
    /// Stream candidates in preference order.
    #[serde(default)]
    pub streams: Vec<PlaybackStreamPayload>,
    /// On-demand vs live.
    pub stream_type: StreamType,
    /// Display title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Display subtitle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    /// Poster / artwork images.
    #[serde(default)]
    pub images: Vec<MediaImage>,
    /// Duration in seconds, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
    /// Whether playback starts automatically.
    #[serde(default = "default_true")]
    pub autoplay: bool,
    /// Resume position in seconds.
    #[serde(default)]
    pub start_time: f64,
    /// App-specific opaque data.
    #[serde(default = "empty_object")]
    pub custom_data: Value,
}

impl Default for PlaybackMediaPayload {
    fn default() -> Self {
        Self {
            metadata: None,
            streams: Vec::new(),
            stream_type: StreamType::default(),
            title: None,
            subtitle: None,
            images: Vec::new(),
            duration: None,
            autoplay: true,
            start_time: 0.0,
            custom_data: empty_object(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

/// A command sent from the bridge to players.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "type",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum PlayerCommand {
    /// Load and prepare media.
    Load {
        /// Owning session id.
        session_id: String,
        /// Media to load.
        media: PlaybackMediaPayload,
    },
    /// Resume playback.
    Play {
        /// Owning session id.
        session_id: String,
    },
    /// Pause playback.
    Pause {
        /// Owning session id.
        session_id: String,
    },
    /// Seek to a position (seconds).
    Seek {
        /// Owning session id.
        session_id: String,
        /// Target position in seconds.
        position: f64,
    },
    /// Stop playback and tear down.
    Stop {
        /// Owning session id.
        session_id: String,
    },
    /// Update player volume.
    Volume {
        /// Owning session id.
        session_id: String,
        /// Level in `[0.0, 1.0]`.
        level: f64,
        /// Mute state.
        muted: bool,
    },
}

impl PlayerCommand {
    /// The session this command targets.
    #[must_use]
    pub fn session_id(&self) -> &str {
        match self {
            PlayerCommand::Load { session_id, .. }
            | PlayerCommand::Play { session_id }
            | PlayerCommand::Pause { session_id }
            | PlayerCommand::Seek { session_id, .. }
            | PlayerCommand::Stop { session_id }
            | PlayerCommand::Volume { session_id, .. } => session_id,
        }
    }
}

/// A report sent from the primary player back to the bridge.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum PlayerReport {
    /// Optional processed cover, correlated with its original image URL.
    Artwork {
        session_id: String,
        source_url: String,
        url: String,
    },
    /// A user action originating at the physical output device.
    ControlRequest {
        session_id: String,
        control: PlayerControlRequest,
    },
    /// Player state update.
    State {
        /// Owning session id.
        session_id: String,
        /// Current playback state.
        player_state: PlayerState,
        /// Current position in seconds.
        #[serde(default)]
        current_time: f64,
        /// Total duration in seconds, if known.
        #[serde(default)]
        duration: Option<f64>,
        /// Reason for entering IDLE, if applicable.
        #[serde(default)]
        idle_reason: Option<IdleReason>,
        #[serde(default)]
        volume: Option<f64>,
        #[serde(default)]
        muted: Option<bool>,
    },
    /// Player error.
    Error {
        /// Owning session id.
        session_id: String,
        /// Error code.
        code: String,
        /// Human-readable message.
        message: String,
    },
}

impl PlayerReport {
    /// The session this report concerns.
    #[must_use]
    pub fn session_id(&self) -> &str {
        match self {
            PlayerReport::State { session_id, .. }
            | PlayerReport::Error { session_id, .. }
            | PlayerReport::Artwork { session_id, .. }
            | PlayerReport::ControlRequest { session_id, .. } => session_id,
        }
    }
}

/// Explicit controls supported from an output device back into the active app.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "command", rename_all = "camelCase")]
pub enum PlayerControlRequest {
    Play,
    Pause,
    Stop,
    Seek { position: f64 },
    Volume { level: f64, muted: bool },
    Next,
    Previous,
}

/// Capability names advertised independently from a concrete control event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PlayerControlKind {
    Play,
    Pause,
    Stop,
    Seek,
    Volume,
    Next,
    Previous,
}

fn legacy_protocol_version() -> u16 {
    1
}

/// Player identity and capabilities supplied by the mandatory registration frame.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerRegistration {
    #[serde(default = "legacy_protocol_version")]
    pub protocol_version: u16,
    pub player_id: String,
    pub name: String,
    #[serde(default)]
    pub capabilities: PlayerCapabilitiesPayload,
}

/// Complete capability profile sent by a player.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PlayerCapabilitiesPayload {
    pub local_audio_cache: bool,
    pub platform: Option<String>,
    pub drm: Vec<DrmCapabilityPayload>,
    pub video_codecs: Vec<String>,
    pub audio_codecs: Vec<String>,
    pub max_resolution: Option<ResolutionPayload>,
    pub hdr_formats: Vec<String>,
    pub frame_rates: Vec<u32>,
    pub subtitle_formats: Vec<String>,
    pub hdcp_level: Option<String>,
    pub control_requests: Vec<PlayerControlKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrmCapabilityPayload {
    pub system: String,
    #[serde(default)]
    pub security_level: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct ResolutionPayload {
    pub width: u32,
    pub height: u32,
}

/// One app setting as rendered by generic player clients.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingPayload {
    pub key: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub kind: String,
    pub default: SettingValue,
    pub value: SettingValue,
    pub writable: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<SettingOptionPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingOptionPayload {
    pub value: String,
    pub label: String,
}

/// Effective settings for one app and player.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppSettingsPayload {
    pub app_key: String,
    pub display_name: String,
    pub revision: u64,
    pub settings: Vec<SettingPayload>,
}

/// Every frame accepted from a connected player.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum ClientMessage {
    #[serde(rename = "artwork")]
    Artwork {
        session_id: String,
        source_url: String,
        url: String,
    },
    #[serde(rename = "controlRequest")]
    ControlRequest {
        session_id: String,
        control: PlayerControlRequest,
    },
    #[serde(rename = "register")]
    Register { player: PlayerRegistration },
    #[serde(rename = "state")]
    State {
        session_id: String,
        player_state: PlayerState,
        #[serde(default)]
        current_time: f64,
        #[serde(default)]
        duration: Option<f64>,
        #[serde(default)]
        idle_reason: Option<IdleReason>,
        #[serde(default)]
        volume: Option<f64>,
        #[serde(default)]
        muted: Option<bool>,
    },
    #[serde(rename = "error")]
    Error {
        session_id: String,
        code: String,
        message: String,
    },
    #[serde(rename = "settingsUpdate")]
    SettingsUpdate {
        request_id: String,
        app_key: String,
        expected_revision: u64,
        changes: BTreeMap<String, Option<SettingValue>>,
    },
}

impl ClientMessage {
    pub fn into_report(self) -> Option<PlayerReport> {
        match self {
            Self::Artwork {
                session_id,
                source_url,
                url,
            } => Some(PlayerReport::Artwork {
                session_id,
                source_url,
                url,
            }),
            Self::ControlRequest {
                session_id,
                control,
            } => Some(PlayerReport::ControlRequest {
                session_id,
                control,
            }),
            Self::State {
                session_id,
                player_state,
                current_time,
                duration,
                idle_reason,
                volume,
                muted,
            } => Some(PlayerReport::State {
                session_id,
                player_state,
                current_time,
                duration,
                idle_reason,
                volume,
                muted,
            }),
            Self::Error {
                session_id,
                code,
                message,
            } => Some(PlayerReport::Error {
                session_id,
                code,
                message,
            }),
            Self::Register { .. } | Self::SettingsUpdate { .. } => None,
        }
    }
}

/// Every frame sent from the server to a player.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ServerMessage {
    Playback(PlayerCommand),
    SettingsSnapshot(SettingsSnapshotMessage),
    SettingsUpdateResult(SettingsUpdateResultMessage),
}

impl From<PlayerCommand> for ServerMessage {
    fn from(command: PlayerCommand) -> Self {
        Self::Playback(command)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsSnapshotMessage {
    #[serde(rename = "type")]
    message_type: &'static str,
    pub apps: Vec<AppSettingsPayload>,
}

impl SettingsSnapshotMessage {
    pub fn new(apps: Vec<AppSettingsPayload>) -> Self {
        Self {
            message_type: "settingsSnapshot",
            apps,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsUpdateResultMessage {
    #[serde(rename = "type")]
    message_type: &'static str,
    pub request_id: String,
    pub status: SettingsUpdateStatus,
    pub app: AppSettingsPayload,
}

impl SettingsUpdateResultMessage {
    pub fn new(request_id: String, status: SettingsUpdateStatus, app: AppSettingsPayload) -> Self {
        Self {
            message_type: "settingsUpdateResult",
            request_id,
            status,
            app,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SettingsUpdateStatus {
    Applied,
    Conflict,
    Rejected,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn load_command_serializes_camel_case_with_type_tag() {
        let command = PlayerCommand::Load {
            session_id: "s1".into(),
            media: PlaybackMediaPayload {
                streams: vec![PlaybackStreamPayload {
                    url: "https://example.com/manifest.mpd".into(),
                    content_type: "application/dash+xml".into(),
                    drm: None,
                }],
                stream_type: StreamType::Buffered,
                ..Default::default()
            },
        };
        let value = serde_json::to_value(&command).unwrap();
        assert_eq!(value["type"], "load");
        assert_eq!(value["sessionId"], "s1");
        assert_eq!(value["media"]["streamType"], "BUFFERED");
        assert_eq!(
            value["media"]["streams"][0]["url"],
            "https://example.com/manifest.mpd"
        );
        assert_eq!(value["media"]["autoplay"], true);
        assert_eq!(value["media"]["startTime"], 0.0);
        // drm omitted when absent
        assert!(value["media"]["streams"][0].get("drm").is_none());
        assert!(value["media"].get("metadata").is_none());
    }

    #[test]
    fn drm_payload_uses_key_system_identifiers() {
        let value = serde_json::to_value(DrmPayload {
            system: DrmSystem::Widevine,
            license_url: "https://lic".into(),
            headers: HashMap::new(),
        })
        .unwrap();
        assert_eq!(value["system"], "com.widevine.alpha");
        assert_eq!(value["licenseUrl"], "https://lic");
    }

    #[test]
    fn state_report_parses_camel_case() {
        let report: PlayerReport = serde_json::from_value(json!({
            "type": "state",
            "sessionId": "s1",
            "playerState": "PLAYING",
            "currentTime": 21.5
        }))
        .unwrap();
        match report {
            PlayerReport::State {
                session_id,
                player_state,
                current_time,
                ..
            } => {
                assert_eq!(session_id, "s1");
                assert_eq!(player_state, PlayerState::Playing);
                assert_eq!(current_time, 21.5);
            }
            _ => panic!("expected state report"),
        }
    }

    #[test]
    fn output_control_report_is_typed_and_session_scoped() {
        let message: ClientMessage = serde_json::from_value(json!({
            "type": "controlRequest",
            "sessionId": "s1",
            "control": { "command": "seek", "position": 42.5 }
        }))
        .unwrap();
        let report = message.into_report().expect("control report");
        assert_eq!(report.session_id(), "s1");
        assert!(matches!(
            report,
            PlayerReport::ControlRequest {
                control: PlayerControlRequest::Seek { position: 42.5 },
                ..
            }
        ));
    }

    #[test]
    fn error_report_parses() {
        let report: PlayerReport = serde_json::from_value(json!({
            "type": "error",
            "sessionId": "s1",
            "code": "LOAD_FAILED",
            "message": "boom"
        }))
        .unwrap();
        assert!(matches!(report, PlayerReport::Error { .. }));
        assert_eq!(report.session_id(), "s1");
    }

    #[test]
    fn registration_uses_nested_player_envelope() {
        let message: ClientMessage = serde_json::from_value(json!({
            "type": "register",
            "player": {
                "playerId": "p1",
                "name": "Kodi",
                "capabilities": { "videoCodecs": ["h264"] }
            }
        }))
        .unwrap();
        let ClientMessage::Register { player } = message else {
            panic!("expected registration");
        };
        assert_eq!(player.player_id, "p1");
        assert_eq!(player.protocol_version, 1);
        assert_eq!(player.capabilities.video_codecs, ["h264"]);
    }

    #[test]
    fn settings_update_supports_set_and_reset() {
        let message: ClientMessage = serde_json::from_value(json!({
            "type": "settingsUpdate",
            "requestId": "r1",
            "appKey": "youtube",
            "expectedRevision": 2,
            "changes": { "codec": "vp9", "other": null }
        }))
        .unwrap();
        let ClientMessage::SettingsUpdate { changes, .. } = message else {
            panic!("expected settings update");
        };
        assert_eq!(changes["codec"], Some(SettingValue::String("vp9".into())));
        assert_eq!(changes["other"], None);
    }
}
