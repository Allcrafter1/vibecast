//! End-to-end tests: a real Cast connection + hub driving a fake app,
//! fake player, and fake proxy registrar over an in-memory duplex stream.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use vibecast_cast::{message, namespace as ns, run_connection, AuthMaterial, ServerEvent};
use vibecast_messages::{PlayerState, Volume};
use vibecast_player_api::{LicenseHandler, ManifestHandler, Player, PlayerCommand, PlayerReport};
use vibecast_proto::CastCodec;
use vibecast_sdk::{
    AppContext, AppManifest, AppProvider, AppSession, AppSettingsSchema, LaunchCredentials,
    LaunchError, LoadRequest, MediaResolveError, PlaybackMedia, PlaybackStream, PlayerCapabilities,
    ReceiverContext, SettingDescriptor, SettingKey, SettingScope, StreamType,
};
use vibecast_security::CertificateBundle;
use vibecast_settings::{
    MemorySettingsPersistence, PlayerSettings, SettingsCatalog, SettingsService,
};

use crate::{AppRegistry, DeviceHub, DeviceHubHandle, DeviceIdentity, HubConfig, ProxyRegistrar};

// -- fakes -----------------------------------------------------------------

#[derive(Default)]
struct FakePlayer {
    commands: Mutex<Vec<PlayerCommand>>,
}

#[async_trait]
impl Player for FakePlayer {
    async fn send(&self, command: PlayerCommand) {
        self.commands.lock().unwrap().push(command);
    }
}

impl FakePlayer {
    fn commands(&self) -> Vec<PlayerCommand> {
        self.commands.lock().unwrap().clone()
    }
}

#[derive(Default)]
struct FakeProxy {
    events: Mutex<Vec<String>>,
    manifests: Mutex<std::collections::HashMap<String, Arc<dyn ManifestHandler>>>,
}

impl ProxyRegistrar for FakeProxy {
    fn register_license(&self, session_id: &str, _handler: Arc<dyn LicenseHandler>) -> String {
        self.events
            .lock()
            .unwrap()
            .push(format!("+license:{session_id}"));
        format!("http://proxy/license/{session_id}")
    }
    fn unregister_license(&self, session_id: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("-license:{session_id}"));
    }
    fn register_manifest(&self, session_id: &str, handler: Arc<dyn ManifestHandler>) -> String {
        self.manifests
            .lock()
            .unwrap()
            .insert(session_id.into(), handler);
        self.events
            .lock()
            .unwrap()
            .push(format!("+manifest:{session_id}"));
        format!("http://proxy/manifest/{session_id}")
    }
    fn unregister_manifest(&self, session_id: &str) {
        self.manifests.lock().unwrap().remove(session_id);
        self.events
            .lock()
            .unwrap()
            .push(format!("-manifest:{session_id}"));
    }
}

const FAKE_SETTING: SettingKey<String> = SettingKey::new("playerValue");

struct FakeApp {
    launched_setting: Arc<Mutex<Option<String>>>,
}

fn fake_manifest() -> AppManifest {
    let settings = AppSettingsSchema::with_display_name(
        "fake",
        "Fake App",
        vec![SettingDescriptor::String {
            key: FAKE_SETTING.as_str().to_string(),
            label: "Player value".to_string(),
            description: None,
            scope: SettingScope::AppPlayer,
            default: "default".to_string(),
            min_length: None,
            max_length: None,
        }],
    )
    .expect("fake settings schema");
    AppManifest::new("fake", &["APP1", "CC1AD845"], "Fake App", settings)
        .with_icon_url("https://example.test/fake.png")
        .with_namespaces(&[FAKE_NS])
}

#[async_trait]
impl AppProvider for FakeApp {
    fn manifest(&self) -> AppManifest {
        fake_manifest()
    }

    async fn launch(
        &self,
        ctx: &AppContext,
        _credentials: LaunchCredentials,
    ) -> Result<Arc<dyn AppSession>, LaunchError> {
        *self.launched_setting.lock().unwrap() = ctx
            .settings
            .snapshot()
            .get(FAKE_SETTING)
            .expect("fake setting has string type");
        Ok(Arc::new(FakeSession(
            std::sync::atomic::AtomicBool::new(false),
            Mutex::new(None),
            std::sync::atomic::AtomicBool::new(false),
        )))
    }
}

const FAKE_NS: &str = "urn:x-cast:test.fake";

struct FakeSession(
    std::sync::atomic::AtomicBool,
    Mutex<Option<PlaybackStream>>,
    std::sync::atomic::AtomicBool,
);

#[async_trait]
impl AppSession for FakeSession {
    fn allows_sender_reconnect_grace(&self) -> bool {
        self.2.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn handles_play_requests(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn on_output_control(&self, ctx: &AppContext, control: vibecast_sdk::OutputControl) {
        if control == vibecast_sdk::OutputControl::Play {
            let media = PlaybackMedia::new(
                ctx.session_id.clone(),
                vec![PlaybackStream::url(
                    "https://example.test/fresh",
                    "audio/mp4",
                )],
                StreamType::Buffered,
            );
            ctx.playback_controller().load(media).await;
        }
    }

    async fn resolve_media(
        &self,
        ctx: &AppContext,
        request: &LoadRequest,
    ) -> Result<PlaybackMedia, MediaResolveError> {
        if request.media.content_id == "fail" {
            return Err(MediaResolveError::content_unavailable("NO_CONTENT"));
        }
        let mut media = PlaybackMedia::new(
            ctx.session_id.clone(),
            vec![PlaybackStream::url(
                "https://cdn.example/manifest.mpd",
                "application/dash+xml",
            )],
            request.media.stream_type,
        );
        media.title = Some("Fake Title".into());
        media.duration = Some(120.0);
        media.start_time = request.current_time;
        Ok(media)
    }

    async fn on_message(
        &self,
        ctx: &AppContext,
        namespace: &str,
        data: &Value,
    ) -> vibecast_sdk::MessageDisposition {
        if namespace == FAKE_NS {
            match data.get("type").and_then(Value::as_str) {
                Some("KEEP_ON_DISCONNECT") => {
                    self.2.store(true, std::sync::atomic::Ordering::Relaxed);
                    ctx.send_custom(namespace, serde_json::json!({"type":"READY"}))
                        .await;
                }
                Some("PUSH_CACHE") | Some("REPEAT_CACHE") => {
                    let stream = {
                        let mut current = self.1.lock().unwrap();
                        if data["type"] == "PUSH_CACHE" || current.is_none() {
                            *current = Some(PlaybackStream::cached_url(
                                "http://127.0.0.1:1/never-requested",
                                "audio/mp4",
                            ));
                        }
                        current.clone().unwrap()
                    };
                    ctx.playback_controller()
                        .load(PlaybackMedia::new(
                            ctx.session_id.clone(),
                            vec![stream],
                            StreamType::Buffered,
                        ))
                        .await;
                }
                Some("HANDLE_PLAY") => {
                    self.0.store(true, std::sync::atomic::Ordering::Relaxed);
                    ctx.send_custom(namespace, serde_json::json!({"type":"READY"}))
                        .await;
                }
                Some("PUSH_MEDIA") => {
                    let mut media = PlaybackMedia::new(
                        ctx.session_id.clone(),
                        vec![PlaybackStream::url(
                            "https://cdn.example/video.mp4",
                            "video/mp4",
                        )],
                        StreamType::Buffered,
                    );
                    media.title = Some("App-driven media".into());
                    media.start_time = 7.0;
                    ctx.playback_controller().load(media).await;
                }
                Some("APP_PLAY") => ctx.playback_controller().play().await,
                Some("APP_PAUSE") => ctx.playback_controller().pause().await,
                Some("APP_SEEK") => ctx.playback_controller().seek(33.0).await,
                Some("APP_STOP") => ctx.playback_controller().stop().await,
                message_type => {
                    ctx.send_custom(
                        namespace,
                        serde_json::json!({"type": "PONG", "echo": message_type}),
                    )
                    .await;
                }
            }
            vibecast_sdk::MessageDisposition::Handled
        } else {
            vibecast_sdk::MessageDisposition::Unhandled
        }
    }
}

// -- harness ---------------------------------------------------------------

fn dummy_auth() -> AuthMaterial {
    AuthMaterial {
        bundle: CertificateBundle {
            peer_cert_pem: Vec::new(),
            peer_key_pem: Vec::new(),
            peer_cert_der: vec![1],
            device_cert_der: vec![1],
            intermediate_certs_der: Vec::new(),
            signature_sha1: Vec::new(),
            signature_sha256: Vec::new(),
            not_valid_before: 0,
            not_valid_after: i64::MAX,
            crl: None,
        },
        crl: None,
    }
}

struct Harness {
    client: Framed<DuplexStream, CastCodec>,
    hub: DeviceHubHandle,
    player: Arc<FakePlayer>,
    proxy: Arc<FakeProxy>,
    launched_setting: Arc<Mutex<Option<String>>>,
}

fn attenuation_volume() -> Volume {
    Volume {
        level: 1.0,
        muted: false,
        control_type: Some("attenuation".into()),
        step_interval: Some(0.05),
    }
}

async fn setup() -> Harness {
    let catalog = SettingsCatalog::new(vec![fake_manifest().settings]).expect("settings catalog");
    let service = SettingsService::new(catalog, Arc::new(MemorySettingsPersistence::default()))
        .await
        .expect("settings service");
    let player_settings = service.player("player-1").expect("player settings");
    let revision = player_settings
        .reader("fake")
        .await
        .expect("fake settings reader")
        .snapshot()
        .revision();
    player_settings
        .compare_and_set_value(
            "fake",
            revision,
            FAKE_SETTING,
            "player override".to_string(),
        )
        .await
        .expect("player setting update");
    setup_with_player_settings(player_settings).await
}

async fn setup_with_player_settings(player_settings: PlayerSettings) -> Harness {
    let (server_end, client_end) = tokio::io::duplex(64 * 1024);
    let (events_tx, mut events_rx) = mpsc::channel::<ServerEvent>(32);
    tokio::spawn(run_connection(
        server_end,
        1,
        Arc::from("peer"),
        Arc::new(dummy_auth()),
        events_tx,
    ));

    let player = Arc::new(FakePlayer::default());
    let proxy = Arc::new(FakeProxy::default());
    let launched_setting = Arc::new(Mutex::new(None));
    let hub = DeviceHub::new(HubConfig {
        identity: DeviceIdentity::new("Living Room".into(), "Chromecast".into(), "dev-1".into()),
        registry: AppRegistry::new(vec![Arc::new(FakeApp {
            launched_setting: launched_setting.clone(),
        })])
        .expect("registry"),
        player: player.clone(),
        proxy: proxy.clone(),
        http: reqwest::Client::new(),
        data_dir: std::env::temp_dir().join("vibecast-core-tests"),
        volume: attenuation_volume(),
        user_agent: String::new(),
        cast_device_capabilities: String::new(),
        capabilities: PlayerCapabilities::default(),
        player_settings,
    });
    let hub_handle = hub.handle();
    {
        let hub_handle = hub_handle.clone();
        tokio::spawn(async move {
            while let Some(event) = events_rx.recv().await {
                if hub_handle.send_server_event(event).await.is_err() {
                    break;
                }
            }
        });
    }
    tokio::spawn(hub.run());

    Harness {
        client: Framed::new(client_end, CastCodec),
        hub: hub_handle,
        player,
        proxy,
        launched_setting,
    }
}

async fn send(
    client: &mut Framed<DuplexStream, CastCodec>,
    namespace: &str,
    dest: &str,
    json: &str,
) {
    client
        .send(message::build_string(
            "sender-1",
            dest,
            namespace,
            json.to_string(),
        ))
        .await
        .unwrap();
}

async fn next_json(client: &mut Framed<DuplexStream, CastCodec>) -> Value {
    let message = client.next().await.unwrap().unwrap();
    serde_json::from_str(message.payload_utf8.as_deref().unwrap()).unwrap()
}

async fn launch(client: &mut Framed<DuplexStream, CastCodec>) -> String {
    launch_id(client, "APP1").await
}

async fn launch_id(client: &mut Framed<DuplexStream, CastCodec>, app_id: &str) -> String {
    send(
        client,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        client,
        ns::RECEIVER,
        "receiver-0",
        &serde_json::json!({"type":"LAUNCH","requestId":1,"appId":app_id}).to_string(),
    )
    .await;
    let message = client.next().await.unwrap().unwrap();
    assert_eq!(message.source_id, "receiver-0");
    assert_eq!(message.destination_id, "sender-1");
    let status: Value =
        serde_json::from_str(message.payload_utf8.as_deref().unwrap()).unwrap();
    assert_eq!(status["type"], "RECEIVER_STATUS");
    let app = &status["status"]["applications"][0];
    assert_eq!(app["appId"], app_id);
    assert_eq!(app["displayName"], "Fake App");
    assert_eq!(app["iconUrl"], "https://example.test/fake.png");
    assert_eq!(
        app["namespaces"],
        serde_json::json!([
            {"name": FAKE_NS},
            {"name": ns::MEDIA},
        ])
    );
    app["transportId"].as_str().unwrap().to_string()
}

// -- tests -----------------------------------------------------------------

#[tokio::test]
async fn current_cache_survives_eof_repeat_but_not_title_change_or_stop() {
    async fn push(h: &mut Harness, session: &str, kind: &str, loads: usize) -> String {
        send(
            &mut h.client,
            FAKE_NS,
            session,
            &serde_json::json!({"type":kind}).to_string(),
        )
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let urls: Vec<String> = h
                    .player
                    .commands()
                    .iter()
                    .filter_map(|c| match c {
                        PlayerCommand::Load { media, .. } => Some(media.streams[0].url.clone()),
                        _ => None,
                    })
                    .collect();
                if urls.len() == loads {
                    break urls.last().unwrap().clone();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }
    async fn wait_unregistered(h: &Harness, session: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while h.proxy.manifests.lock().unwrap().contains_key(session) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let mut h = setup().await;
    let session = launch(&mut h.client).await;
    let first = push(&mut h, &session, "PUSH_CACHE", 1).await;
    let token = first.rsplit('/').next().unwrap();
    assert!(token.starts_with("cache-"));
    let old = h.proxy.manifests.lock().unwrap()[&session].clone();
    h.hub
        .send_player_report(PlayerReport::State {
            session_id: session.clone(),
            player_state: PlayerState::Idle,
            current_time: 100.0,
            duration: Some(100.0),
            idle_reason: Some(vibecast_messages::IdleReason::Finished),
            volume: None,
            muted: None,
        })
        .await
        .unwrap();
    wait_unregistered(&h, &session).await;
    assert_eq!(push(&mut h, &session, "REPEAT_CACHE", 2).await, first);
    // A normal selection (including playlist traversal) gets a new single slot,
    // even if its upstream URL happens to be identical.
    let second = push(&mut h, &session, "PUSH_CACHE", 3).await;
    assert_ne!(first, second);
    assert_eq!(
        old.handle_cached_media(token, http::Method::GET, http::HeaderMap::new())
            .await
            .unwrap()
            .status,
        502
    );
    let current = h.proxy.manifests.lock().unwrap()[&session].clone();
    assert!(current
        .handle_cached_media(token, http::Method::GET, http::HeaderMap::new())
        .await
        .is_none());
    send(&mut h.client, FAKE_NS, &session, r#"{"type":"APP_STOP"}"#).await;
    wait_unregistered(&h, &session).await;
    assert_eq!(
        current
            .handle_cached_media(
                second.rsplit('/').next().unwrap(),
                http::Method::GET,
                http::HeaderMap::new()
            )
            .await
            .unwrap()
            .status,
        502
    );
}

#[tokio::test]
async fn app_owned_cast_play_loads_fresh_media_without_false_playing() {
    let mut harness = setup().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    let _ = next_json(&mut harness.client).await;
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"HANDLE_PLAY"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "READY");
    send(
        &mut harness.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"PLAY","requestId":71,"mediaSessionId":1}"#,
    )
    .await;
    let ack = next_json(&mut harness.client).await;
    assert_eq!(ack["requestId"], 71);
    assert_ne!(ack["status"][0]["playerState"], "PLAYING");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if harness
                .player
                .commands()
                .iter()
                .any(|c| matches!(c, PlayerCommand::Load { .. }))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!harness
        .player
        .commands()
        .iter()
        .any(|c| matches!(c, PlayerCommand::Play { .. })));
    harness
        .hub
        .send_player_report(PlayerReport::ControlRequest {
            session_id: transport.clone(),
            control: vibecast_player_api::PlayerControlRequest::Play,
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if harness
                .player
                .commands()
                .iter()
                .filter(|c| matches!(c, PlayerCommand::Load { .. }))
                .count()
                == 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!harness
        .player
        .commands()
        .iter()
        .any(|c| matches!(c, PlayerCommand::Play { .. })));
    // App-originated Play must bypass interception (no callback loop).
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"APP_PLAY"}"#,
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if harness
                .player
                .commands()
                .iter()
                .any(|c| matches!(c, PlayerCommand::Play { .. }))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn launch_injects_player_scoped_settings() {
    let mut harness = setup().await;
    launch(&mut harness.client).await;

    assert_eq!(
        harness.launched_setting.lock().unwrap().as_deref(),
        Some("player override")
    );
}

#[tokio::test]
async fn unavailable_app_settings_return_launch_error() {
    let service = SettingsService::new(
        SettingsCatalog::default(),
        Arc::new(MemorySettingsPersistence::default()),
    )
    .await
    .expect("settings service");
    let player_settings = service.player("player-1").expect("player settings");
    let mut harness = setup_with_player_settings(player_settings).await;

    send(
        &mut harness.client,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        &mut harness.client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"LAUNCH","requestId":7,"appId":"APP1"}"#,
    )
    .await;

    let response = next_json(&mut harness.client).await;
    assert_eq!(response["type"], "LAUNCH_ERROR");
    assert_eq!(response["requestId"], 7);
}

#[tokio::test]
async fn launch_load_and_play_end_to_end() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    let transport = launch(client).await;

    // Subscribe to the app transport; the hub replies with the current (empty) status.
    send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
    let connect_status = next_json(client).await;
    assert_eq!(connect_status["type"], "MEDIA_STATUS");
    assert_eq!(connect_status["status"], serde_json::json!([]));

    // LOAD -> IDLE/LOADING, then resolved LOADING, then BUFFERING.
    send(
        client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":2,"media":{"contentId":"abc","contentType":"video/mp4"}}"#,
    )
    .await;

    let loading = next_json(client).await;
    assert_eq!(loading["status"][0]["playerState"], "IDLE");
    assert_eq!(
        loading["status"][0]["extendedStatus"]["playerState"],
        "LOADING"
    );

    let resolved_loading = next_json(client).await;
    assert_eq!(
        resolved_loading["status"][0]["extendedStatus"]["playerState"],
        "LOADING"
    );

    let buffering = next_json(client).await;
    assert_eq!(buffering["status"][0]["playerState"], "BUFFERING");
    // The DASH stream URL was rewritten to the manifest proxy.
    let content_url = buffering["status"][0]["media"]["contentUrl"]
        .as_str()
        .unwrap();
    assert!(content_url.contains("/manifest/"), "url = {content_url}");
    assert_eq!(
        buffering["status"][0]["media"]["metadata"]["title"],
        "Fake Title"
    );

    // PLAY -> PLAYING.
    send(
        client,
        ns::MEDIA,
        &transport,
        r#"{"type":"PLAY","requestId":3,"mediaSessionId":2}"#,
    )
    .await;
    let playing = next_json(client).await;
    assert_eq!(playing["status"][0]["playerState"], "PLAYING");
    assert_eq!(playing["status"][0]["supportedMediaCommands"], 15);

    // Restore receiver volume before Load/Play; register the manifest proxy.
    let commands = harness.player.commands();
    assert!(matches!(
        commands.first(),
        Some(PlayerCommand::Volume { .. })
    ));
    assert!(matches!(commands.last(), Some(PlayerCommand::Play { .. })));
    if let Some(PlayerCommand::Load { media, .. }) = commands.get(1) {
        assert!(media.streams[0].url.contains("/manifest/"));
    }
    assert!(harness
        .proxy
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event.starts_with("+manifest:")));
}

#[tokio::test]
async fn default_media_rejects_queue_load_without_poisoning_next_load() {
    let mut harness = setup().await;
    let transport = launch_id(&mut harness.client, "CC1AD845").await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    let _ = next_json(&mut harness.client).await;
    let before = harness.player.commands().len();
    send(
        &mut harness.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"QUEUE_LOAD","requestId":71,"items":[]}"#,
    )
    .await;
    let rejected = next_json(&mut harness.client).await;
    assert_eq!(rejected["type"], "INVALID_REQUEST");
    assert_eq!(rejected["requestId"], 71);
    assert_eq!(harness.player.commands().len(), before);
    send(
        &mut harness.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":72,"media":{"contentId":"abc","contentType":"audio/mpeg"}}"#,
    )
    .await;
    for _ in 0..3 {
        let _ = next_json(&mut harness.client).await;
    }
    assert!(harness
        .player
        .commands()
        .iter()
        .any(|c| matches!(c, PlayerCommand::Load { .. })));
}

#[tokio::test]
async fn live_seek_is_rejected_without_player_command() {
    let mut harness = setup().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    let _ = next_json(&mut harness.client).await;
    send(&mut harness.client, ns::MEDIA, &transport,
        r#"{"type":"LOAD","requestId":72,"media":{"contentId":"abc","contentType":"audio/mpeg","streamType":"LIVE"}}"#).await;
    for _ in 0..3 {
        let _ = next_json(&mut harness.client).await;
    }
    let before = harness.player.commands().len();
    send(
        &mut harness.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"SEEK","requestId":73,"mediaSessionId":1,"currentTime":5}"#,
    )
    .await;
    let rejected = next_json(&mut harness.client).await;
    assert_eq!(rejected["type"], "INVALID_REQUEST");
    assert_eq!(rejected["reason"], "Seek position unavailable");
    assert_eq!(harness.player.commands().len(), before);
}

#[tokio::test]
async fn load_failure_sends_load_failed() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    let transport = launch(client).await;
    send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
    let _ = next_json(client).await; // connect status

    send(
        client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":9,"media":{"contentId":"fail","contentType":"video/mp4"}}"#,
    )
    .await;

    // IDLE/LOADING broadcast, then LOAD_FAILED to the sender, then an ERROR status.
    let loading = next_json(client).await;
    assert_eq!(
        loading["status"][0]["extendedStatus"]["playerState"],
        "LOADING"
    );

    let failed = next_json(client).await;
    assert_eq!(failed["type"], "LOAD_FAILED");
    assert_eq!(failed["requestId"], 9);
    assert_eq!(failed["reason"], "CONTENT_UNAVAILABLE");

    let idle = next_json(client).await;
    assert_eq!(idle["type"], "MEDIA_STATUS");
    assert_eq!(idle["status"][0]["idleReason"], "ERROR");
}

#[tokio::test]
async fn primary_player_report_broadcasts_status() {
    let mut harness = setup().await;
    let transport = {
        let client = &mut harness.client;
        let transport = launch(client).await;
        send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
        let _ = next_json(client).await; // connect status
                                         // LOAD so there is media to report against, then drain its 3 statuses.
        send(
            client,
            ns::MEDIA,
            &transport,
            r#"{"type":"LOAD","requestId":2,"media":{"contentId":"abc"}}"#,
        )
        .await;
        for _ in 0..3 {
            let _ = next_json(client).await;
        }
        transport
    };

    // A player state report from the player bridge.
    harness
        .hub
        .send_player_report(PlayerReport::State {
            session_id: transport.clone(),
            player_state: PlayerState::Playing,
            current_time: 33.5,
            duration: Some(120.0),
            idle_reason: None,
            volume: None,
            muted: None,
        })
        .await
        .unwrap();

    let status = next_json(&mut harness.client).await;
    assert_eq!(status["type"], "MEDIA_STATUS");
    assert_eq!(status["status"][0]["playerState"], "PLAYING");
    assert_eq!(status["status"][0]["currentTime"], 33.5);
}

#[tokio::test]
async fn receiver_status_lists_running_app() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    let transport = launch(client).await;
    // A sender subscribing to the app transport marks the app sender-connected.
    send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
    let _ = next_json(client).await; // connect status

    send(
        client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"GET_STATUS","requestId":5}"#,
    )
    .await;
    let status = next_json(client).await;
    assert_eq!(status["type"], "RECEIVER_STATUS");
    assert_eq!(status["requestId"], 5);
    assert_eq!(status["status"]["applications"][0]["appId"], "APP1");
    assert_eq!(status["status"]["applications"][0]["senderConnected"], true);
}

#[tokio::test]
async fn custom_namespace_message_is_handled_and_replies() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    let transport = launch(client).await;
    send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
    let _ = next_json(client).await; // connect status

    // A message on the app's custom namespace reaches on_message, which replies
    // to the sender via ctx.send_custom.
    send(client, FAKE_NS, &transport, r#"{"type":"PING"}"#).await;
    let reply = next_json(client).await;
    assert_eq!(reply["type"], "PONG");
    assert_eq!(reply["echo"], "PING");
}

#[tokio::test]
async fn app_driven_playback_uses_canonical_media_path() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    let transport = launch(client).await;
    send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
    let _ = next_json(client).await;

    send(client, FAKE_NS, &transport, r#"{"type":"PUSH_MEDIA"}"#).await;
    let loading = next_json(client).await;
    let buffering = next_json(client).await;
    assert_eq!(
        loading["status"][0]["extendedStatus"]["playerState"],
        "LOADING"
    );
    assert_eq!(loading["status"][0]["mediaSessionId"], 2);
    assert_eq!(buffering["status"][0]["playerState"], "BUFFERING");
    assert_eq!(buffering["status"][0]["currentTime"], 7.0);

    send(client, FAKE_NS, &transport, r#"{"type":"APP_PAUSE"}"#).await;
    assert_eq!(
        next_json(client).await["status"][0]["playerState"],
        "PAUSED"
    );
    send(client, FAKE_NS, &transport, r#"{"type":"APP_SEEK"}"#).await;
    assert_eq!(next_json(client).await["status"][0]["currentTime"], 33.0);
    send(client, FAKE_NS, &transport, r#"{"type":"APP_PLAY"}"#).await;
    assert_eq!(
        next_json(client).await["status"][0]["playerState"],
        "PLAYING"
    );
    send(client, FAKE_NS, &transport, r#"{"type":"APP_STOP"}"#).await;
    let stopped = next_json(client).await;
    assert_eq!(stopped["status"][0]["playerState"], "IDLE");
    assert_eq!(stopped["status"][0]["idleReason"], "CANCELLED");

    let commands = harness.player.commands();
    assert!(matches!(commands[0], PlayerCommand::Volume { .. }));
    assert!(matches!(commands[1], PlayerCommand::Load { .. }));
    assert!(matches!(commands[2], PlayerCommand::Pause { .. }));
    assert!(matches!(
        commands[3],
        PlayerCommand::Seek { position: 33.0, .. }
    ));
    assert!(matches!(commands[4], PlayerCommand::Play { .. }));
    assert!(matches!(commands[5], PlayerCommand::Stop { .. }));
}

#[tokio::test]
async fn receiver_stop_publishes_terminal_media_before_removing_app() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    let transport = launch(client).await;
    send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
    let _ = next_json(client).await;
    send(client, FAKE_NS, &transport, r#"{"type":"PUSH_MEDIA"}"#).await;
    let _ = next_json(client).await;
    let _ = next_json(client).await;
    send(client, FAKE_NS, &transport, r#"{"type":"APP_PLAY"}"#).await;
    assert_eq!(
        next_json(client).await["status"][0]["playerState"],
        "PLAYING"
    );
    send(
        client,
        ns::RECEIVER,
        "receiver-0",
        &serde_json::json!({
            "type":"STOP", "requestId":90, "sessionId":transport
        })
        .to_string(),
    )
    .await;
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), next_json(client))
        .await
        .unwrap();
    assert_eq!(terminal["type"], "MEDIA_STATUS");
    assert_eq!(terminal["status"][0]["playerState"], "IDLE");
    assert_eq!(terminal["status"][0]["idleReason"], "CANCELLED");
    let receiver = next_json(client).await;
    assert_eq!(
        receiver["status"]["applications"].as_array().unwrap().len(),
        0
    );
    assert!(harness
        .player
        .commands()
        .iter()
        .any(|c| matches!(c, PlayerCommand::Stop { .. })));
}

#[tokio::test]
async fn app_close_refreshes_platform_status_without_transport_disconnect() {
    for preserve in [false, true] {
        let mut harness = setup().await;
        let client = &mut harness.client;
        let transport = launch(client).await;
        if preserve {
            send(
                client,
                FAKE_NS,
                &transport,
                r#"{"type":"KEEP_ON_DISCONNECT"}"#,
            )
            .await;
            assert_eq!(next_json(client).await["type"], "READY");
        }
        send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
        let _ = next_json(client).await;
        send(client, FAKE_NS, &transport, r#"{"type":"PUSH_MEDIA"}"#).await;
        let _ = next_json(client).await;
        let _ = next_json(client).await;
        // The sender closes its app channel, not the still-subscribed platform
        // connection. No later socket disconnect or GET_STATUS should be required.
        send(client, ns::CONNECTION, &transport, r#"{"type":"CLOSE"}"#).await;
        let receiver = tokio::time::timeout(std::time::Duration::from_secs(2), next_json(client))
            .await
            .expect("platform observer must learn that the app stopped");
        assert_eq!(receiver["type"], "RECEIVER_STATUS");
        assert!(receiver["status"]["applications"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(harness
            .player
            .commands()
            .iter()
            .any(|c| matches!(c, PlayerCommand::Stop { .. })));
        // The same socket can immediately create a new app session.
        assert_ne!(launch(client).await, transport);
    }
}

/// Losing the control socket must not end an opted-in receiver-owned session.
#[tokio::test]
async fn opted_in_owner_disconnect_preserves_cache_and_allows_explicit_stop() {
    retained_session_disconnect(true, DisconnectAction::Reattach).await;
}

#[tokio::test]
async fn opted_in_last_subscriber_disconnect_preserves_cache_and_allows_explicit_stop() {
    retained_session_disconnect(false, DisconnectAction::Reattach).await;
}

#[tokio::test]
async fn owner_disconnect_grace_expires_despite_existing_observer_and_platform_probes() {
    retained_session_disconnect(true, DisconnectAction::Expire).await;
}

#[tokio::test]
async fn last_subscriber_disconnect_grace_expires() {
    retained_session_disconnect(false, DisconnectAction::Expire).await;
}

#[tokio::test]
async fn explicit_stop_during_grace_is_immediate() {
    retained_session_disconnect(true, DisconnectAction::Stop).await;
}

#[tokio::test]
async fn explicit_close_during_grace_is_immediate() {
    retained_session_disconnect(true, DisconnectAction::Close).await;
}

#[tokio::test]
async fn explicit_owner_close_stops_even_with_an_observer() {
    retained_session_disconnect(true, DisconnectAction::OwnerClose).await;
}

#[tokio::test]
async fn observer_disconnect_does_not_start_grace_while_owner_is_attached() {
    retained_session_disconnect(false, DisconnectAction::ObserverLost).await;
}

#[tokio::test]
async fn replacement_session_is_not_stopped_by_old_grace_deadline() {
    retained_session_disconnect(true, DisconnectAction::Replace).await;
}

#[derive(Clone, Copy)]
enum DisconnectAction {
    Reattach,
    Expire,
    Stop,
    Close,
    Replace,
    OwnerClose,
    ObserverLost,
}

async fn retained_session_disconnect(drop_owner: bool, action: DisconnectAction) {
    let mut h = setup().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        FAKE_NS,
        &transport,
        r#"{"type":"KEEP_ON_DISCONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut h.client).await["type"], "READY");
    send(
        &mut h.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PUSH_CACHE"}"#,
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !h.proxy.manifests.lock().unwrap().contains_key(&transport) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let cache_proxy = h.proxy.manifests.lock().unwrap()[&transport].clone();
    let (server_end, client_end) = tokio::io::duplex(64 * 1024);
    let (events_tx, mut events_rx) = mpsc::channel::<ServerEvent>(32);
    tokio::spawn(run_connection(
        server_end,
        2,
        Arc::from("observer"),
        Arc::new(dummy_auth()),
        events_tx,
    ));
    let hub = h.hub.clone();
    tokio::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            if hub.send_server_event(event).await.is_err() {
                break;
            }
        }
    });
    let mut observer = Framed::new(client_end, CastCodec);
    send(
        &mut observer,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        &mut observer,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut observer).await["type"], "MEDIA_STATUS");
    if matches!(action, DisconnectAction::OwnerClose) {
        send(
            &mut h.client,
            ns::CONNECTION,
            &transport,
            r#"{"type":"CLOSE"}"#,
        )
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let status = next_json(&mut observer).await;
                if status["type"] == "RECEIVER_STATUS" {
                    assert!(status["status"]["applications"]
                        .as_array()
                        .unwrap()
                        .is_empty());
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(h
            .player
            .commands()
            .iter()
            .any(|c| matches!(c, PlayerCommand::Stop { .. })));
        return;
    }
    if matches!(action, DisconnectAction::ObserverLost) {
        send(
            &mut h.client,
            ns::CONNECTION,
            &transport,
            r#"{"type":"CONNECT"}"#,
        )
        .await;
        assert_eq!(next_json(&mut h.client).await["type"], "MEDIA_STATUS");
    }
    let mut survivor = if drop_owner {
        drop(h.client);
        observer
    } else {
        drop(observer);
        h.client
    };
    let status = tokio::time::timeout(std::time::Duration::from_secs(2), next_json(&mut survivor))
        .await
        .unwrap();
    assert_eq!(status["type"], "RECEIVER_STATUS");
    assert_eq!(
        status["status"]["applications"][0]["transportId"],
        transport
    );
    assert!(!h
        .player
        .commands()
        .iter()
        .any(|c| matches!(c, PlayerCommand::Stop { .. })));
    assert!(Arc::ptr_eq(
        &cache_proxy,
        &h.proxy.manifests.lock().unwrap()[&transport]
    ));
    if matches!(action, DisconnectAction::ObserverLost) {
        tokio::time::sleep(std::time::Duration::from_millis(10100)).await;
        assert!(!h
            .player
            .commands()
            .iter()
            .any(|c| matches!(c, PlayerCommand::Stop { .. })));
        h.hub.shutdown().await;
        return;
    }
    if matches!(action, DisconnectAction::Expire) {
        // Discovery/status traffic at the end of the grace must not reset it.
        tokio::time::sleep(std::time::Duration::from_secs(9)).await;
        send(
            &mut survivor,
            ns::CONNECTION,
            "receiver-0",
            r#"{"type":"CONNECT"}"#,
        )
        .await;
        send(
            &mut survivor,
            ns::RECEIVER,
            "receiver-0",
            r#"{"type":"GET_STATUS","requestId":80}"#,
        )
        .await;
        assert_eq!(
            next_json(&mut survivor).await["status"]["applications"][0]["transportId"],
            transport
        );
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let status = next_json(&mut survivor).await;
                if status["type"] == "RECEIVER_STATUS" {
                    assert!(status["status"]["applications"]
                        .as_array()
                        .unwrap()
                        .is_empty());
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(h
            .player
            .commands()
            .iter()
            .any(|c| matches!(c, PlayerCommand::Stop { .. })));
        assert!(!h.proxy.manifests.lock().unwrap().contains_key(&transport));
        return;
    }
    if matches!(action, DisconnectAction::Replace) {
        send(
            &mut survivor,
            ns::RECEIVER,
            "receiver-0",
            r#"{"type":"LAUNCH","requestId":81,"appId":"APP1"}"#,
        )
        .await;
        let replacement = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let status = next_json(&mut survivor).await;
                if status["type"] == "RECEIVER_STATUS" {
                    break status["status"]["applications"][0]["transportId"]
                        .as_str()
                        .unwrap()
                        .to_string();
                }
            }
        })
        .await
        .unwrap();
        assert_ne!(replacement, transport);
        tokio::time::sleep(std::time::Duration::from_millis(10100)).await;
        send(
            &mut survivor,
            ns::RECEIVER,
            "receiver-0",
            r#"{"type":"GET_STATUS","requestId":82}"#,
        )
        .await;
        assert_eq!(
            next_json(&mut survivor).await["status"]["applications"][0]["transportId"],
            replacement
        );
        h.hub.shutdown().await;
        return;
    }
    if matches!(action, DisconnectAction::Reattach) {
        // Reattach to the same app and repeat without replacing its cache URL.
        send(
            &mut survivor,
            ns::CONNECTION,
            &transport,
            r#"{"type":"CONNECT"}"#,
        )
        .await;
        assert_eq!(next_json(&mut survivor).await["type"], "MEDIA_STATUS");
        send(
            &mut survivor,
            FAKE_NS,
            &transport,
            r#"{"type":"REPEAT_CACHE"}"#,
        )
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let urls: Vec<_> = h
                    .player
                    .commands()
                    .into_iter()
                    .filter_map(|c| match c {
                        PlayerCommand::Load { media, .. } => Some(media.streams[0].url.clone()),
                        _ => None,
                    })
                    .collect();
                if urls.len() == 2 {
                    assert_eq!(urls[0], urls[1]);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The old expiry must not stop a successfully reattached app later.
        tokio::time::sleep(std::time::Duration::from_millis(10100)).await;
        assert!(!h
            .player
            .commands()
            .iter()
            .any(|c| matches!(c, PlayerCommand::Stop { .. })));
    }
    if matches!(action, DisconnectAction::Close) {
        send(
            &mut survivor,
            ns::CONNECTION,
            &transport,
            r#"{"type":"CLOSE"}"#,
        )
        .await;
    } else {
        send(
            &mut survivor,
            ns::RECEIVER,
            "receiver-0",
            &serde_json::json!({"type":"STOP", "requestId":90, "sessionId":transport}).to_string(),
        )
        .await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let status = next_json(&mut survivor).await;
            if status["type"] == "RECEIVER_STATUS" {
                assert!(status["status"]["applications"]
                    .as_array()
                    .unwrap()
                    .is_empty());
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(h
        .player
        .commands()
        .iter()
        .any(|c| matches!(c, PlayerCommand::Stop { .. })));
    assert!(!h.proxy.manifests.lock().unwrap().contains_key(&transport));
}

/// Non-opted-in apps keep the existing owner-disconnect cleanup policy.
#[tokio::test]
async fn owner_disconnect_notifies_a_separate_observer_and_allows_takeover() {
    let mut harness = setup().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    let _ = next_json(&mut harness.client).await;
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PUSH_MEDIA"}"#,
    )
    .await;
    let _ = next_json(&mut harness.client).await;
    let _ = next_json(&mut harness.client).await;

    let (server_end, client_end) = tokio::io::duplex(64 * 1024);
    let (events_tx, mut events_rx) = mpsc::channel::<ServerEvent>(32);
    tokio::spawn(run_connection(
        server_end,
        2,
        Arc::from("observer"),
        Arc::new(dummy_auth()),
        events_tx,
    ));
    let hub = harness.hub.clone();
    tokio::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            if hub.send_server_event(event).await.is_err() {
                break;
            }
        }
    });
    let mut observer = Framed::new(client_end, CastCodec);
    send(
        &mut observer,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        &mut observer,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut observer).await["type"], "MEDIA_STATUS");
    drop(harness.client);
    let terminal =
        tokio::time::timeout(std::time::Duration::from_secs(2), next_json(&mut observer))
            .await
            .expect("observer must receive terminal media state");
    assert_eq!(terminal["type"], "MEDIA_STATUS");
    assert_eq!(terminal["status"][0]["playerState"], "IDLE");
    assert_eq!(terminal["status"][0]["idleReason"], "CANCELLED");
    let receiver = next_json(&mut observer).await;
    assert!(receiver["status"]["applications"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_ne!(launch(&mut observer).await, transport);
    assert!(harness
        .player
        .commands()
        .iter()
        .any(|c| matches!(c, PlayerCommand::Stop { .. })));
}

/// An app session whose `resolve_license` reverses the challenge instead of
/// forwarding (proving the override is used, no network involved).
struct LicenseSession;

#[async_trait]
impl AppSession for LicenseSession {
    async fn resolve_media(
        &self,
        _ctx: &AppContext,
        _request: &LoadRequest,
    ) -> Result<PlaybackMedia, MediaResolveError> {
        Err(MediaResolveError::internal("UNUSED"))
    }

    async fn resolve_license(
        &self,
        _ctx: &AppContext,
        request: vibecast_sdk::LicenseRequest,
        _route: vibecast_sdk::LicenseRoute,
        _forward: &dyn vibecast_sdk::LicenseForwarder,
    ) -> vibecast_sdk::LicenseResponse {
        let mut body = request.body;
        body.reverse();
        vibecast_sdk::LicenseResponse {
            body,
            content_type: "application/xprotobuf".into(),
            status: 200,
        }
    }
}

#[tokio::test]
async fn app_resolve_license_override_is_used() {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use vibecast_player_api::{LicenseHandler, LicenseRequest as WireLicenseRequest, RouteId};

    use crate::proxy::{LicenseRoute, SessionProxy};

    let app: Arc<dyn AppSession> = Arc::new(LicenseSession);
    let ctx = AppContext::new(
        "s",
        "s",
        "APP1",
        reqwest::Client::new(),
        ReceiverContext::new("Living Room", "Chromecast", "dev-1", PathBuf::from("/tmp")),
        Arc::new(vibecast_sdk::NoopSenderChannel),
    );
    let license_routes = HashMap::from([(
        RouteId::license(0),
        LicenseRoute {
            system: vibecast_sdk::DrmSystem::ClearKey,
            upstream_url: "https://unused.example/license".into(),
            headers: http::HeaderMap::new(),
        },
    )]);
    let proxy = SessionProxy::new(app, ctx, HashMap::new(), license_routes);

    let request = WireLicenseRequest {
        session_id: "s".into(),
        body: b"abc".to_vec(),
        content_type: "application/octet-stream".into(),
        route_id: Some(RouteId::license(0)),
        headers: http::HeaderMap::new(),
    };
    let response = proxy.handle_license(request).await.unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"cba");
    assert_eq!(response.content_type, "application/xprotobuf");
}

#[tokio::test]
async fn platform_get_device_info() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    send(
        client,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        client,
        ns::DISCOVERY,
        "receiver-0",
        r#"{"type":"GET_DEVICE_INFO","requestId":3}"#,
    )
    .await;
    let reply = next_json(client).await;
    assert_eq!(reply["type"], "DEVICE_INFO");
    assert_eq!(reply["friendlyName"], "Living Room");
    assert_eq!(reply["deviceId"], "dev-1");
}

#[tokio::test]
async fn platform_setup_returns_eureka_info() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    send(
        client,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        client,
        ns::SETUP,
        "receiver-0",
        r#"{"type":"eureka_info","requestId":8}"#,
    )
    .await;
    let reply = next_json(client).await;
    assert_eq!(reply["type"], "eureka_info");
    assert_eq!(reply["response_code"], 200);
    assert_eq!(reply["data"]["device_info"]["ssdp_udn"], "dev-1");
}

#[tokio::test]
async fn platform_app_availability_marks_registered_app() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    send(
        client,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"GET_APP_AVAILABILITY","requestId":4,"appId":["APP1"]}"#,
    )
    .await;
    let reply = next_json(client).await;
    assert_eq!(reply["type"], "GET_APP_AVAILABILITY");
    assert_eq!(reply["availability"]["APP1"], "APP_AVAILABLE");
}

#[tokio::test]
async fn platform_set_volume_broadcasts_receiver_status() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    send(
        client,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"SET_VOLUME","requestId":6,"volume":{"level":0.5}}"#,
    )
    .await;
    let reply = next_json(client).await;
    assert_eq!(reply["type"], "RECEIVER_STATUS");
    assert_eq!(reply["status"]["volume"]["level"], 0.5);
    // muted was omitted, so it stays at its prior value.
    assert_eq!(reply["status"]["volume"]["muted"], false);
}

#[tokio::test]
async fn platform_volume_subscription_survives_app_connect() {
    let mut harness = setup().await;
    let client = &mut harness.client;
    let transport = launch(client).await;
    send(client, ns::CONNECTION, &transport, r#"{"type":"CONNECT"}"#).await;
    // CONNECT generates no reply. Barrier through GET_STATUS drains prior work.
    send(
        client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"GET_STATUS","requestId":50}"#,
    )
    .await;
    loop {
        if next_json(client).await["requestId"] == 50 {
            break;
        }
    }
    send(
        client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"SET_VOLUME","requestId":51,"volume":{"level":0.42}}"#,
    )
    .await;
    let reply = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let reply = next_json(client).await;
            if reply["type"] == "RECEIVER_STATUS" && reply["requestId"] == 51 {
                break reply;
            }
        }
    })
    .await
    .expect("platform status must arrive during an app session");
    assert_eq!(reply["status"]["volume"]["level"], 0.42);
    send(
        client,
        ns::MEDIA,
        &transport,
        r#"{"type":"SET_VOLUME","requestId":52,"mediaSessionId":1,"volume":{"level":0.43}}"#,
    )
    .await;
    let reply = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let reply = next_json(client).await;
            if reply["type"] == "RECEIVER_STATUS" {
                break reply;
            }
        }
    })
    .await
    .expect("media volume changes must update platform observers too");
    assert_eq!(reply["status"]["volume"]["level"], 0.43);
}
