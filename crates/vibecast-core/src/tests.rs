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
    LaunchError, LoadRequest, MediaResolveError, OutputControl, PlaybackMedia, PlaybackStream,
    PlayerCapabilities, ReceiverContext, SenderAttachment, SettingDescriptor, SettingKey,
    SettingScope, StreamType,
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
    exclusive_sender_takeover: bool,
    sender_attachments: Arc<Mutex<Vec<SenderAttachment>>>,
    output_controls: Arc<Mutex<Vec<OutputControl>>>,
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
            self.exclusive_sender_takeover,
            self.sender_attachments.clone(),
            self.output_controls.clone(),
        )))
    }
}

const FAKE_NS: &str = "urn:x-cast:test.fake";

struct FakeSession(
    std::sync::atomic::AtomicBool,
    Mutex<Option<PlaybackStream>>,
    std::sync::atomic::AtomicBool,
    bool,
    Arc<Mutex<Vec<SenderAttachment>>>,
    Arc<Mutex<Vec<OutputControl>>>,
);

#[async_trait]
impl AppSession for FakeSession {
    async fn on_sender_connected(
        &self,
        _ctx: &AppContext,
        _sender_id: &str,
        attachment: SenderAttachment,
    ) {
        self.4.lock().unwrap().push(attachment);
    }

    async fn on_playback_update(&self, ctx: &AppContext, state: vibecast_sdk::PlaybackState) {
        if state.media_recovery_attempts > 0 {
            ctx.send_custom(
                FAKE_NS,
                serde_json::json!({
                    "type": "RECOVERY_STATE",
                    "attempts": state.media_recovery_attempts,
                    "error": state.idle_reason == Some(vibecast_messages::IdleReason::Error),
                }),
            )
            .await;
        }
    }

    fn exclusive_sender_takeover(&self) -> bool {
        self.3
    }

    fn claims_sender_ownership(&self, namespace: &str, data: &Value) -> bool {
        namespace == FAKE_NS && data["type"] == "CLAIM"
    }

    fn joins_sender_ownership(&self, namespace: &str, data: &Value) -> bool {
        namespace == ns::MEDIA && data["type"] == "QUEUE_GET_ITEM_IDS"
    }

    fn allows_sender_reconnect_grace(&self) -> bool {
        self.2.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn handles_play_requests(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn handles_output_control(&self, control: &OutputControl) -> bool {
        (self.3 && matches!(control, OutputControl::Next | OutputControl::Previous))
            || (matches!(control, OutputControl::Play) && self.handles_play_requests())
    }

    fn supports_queue_navigation(&self) -> bool {
        self.3
    }

    async fn on_output_control(&self, ctx: &AppContext, control: OutputControl) {
        self.5.lock().unwrap().push(control);
        if control == OutputControl::Play {
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
        if request.media.content_id == "slow" {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
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
                    if let vibecast_sdk::StreamSource::CachedUrl { cache, .. } = &stream.source {
                        if let Some(attempts) = data["recoveryAttempts"].as_u64() {
                            cache.set_recovery_attempts(attempts as u8);
                        }
                    }
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
                Some("FLOOD_CALLBACK") => {
                    // More than both bounded mailboxes combined: the callback
                    // waits for hub capacity while hub notifications fill jobs.
                    for _ in 0..512 {
                        ctx.playback_controller().pause().await;
                    }
                }
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
    sender_attachments: Arc<Mutex<Vec<SenderAttachment>>>,
    output_controls: Arc<Mutex<Vec<OutputControl>>>,
    hub_abort: tokio::task::AbortHandle,
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
    setup_with_options(player_settings, false).await
}

async fn setup_exclusive() -> Harness {
    let catalog = SettingsCatalog::new(vec![fake_manifest().settings]).expect("settings catalog");
    let service = SettingsService::new(catalog, Arc::new(MemorySettingsPersistence::default()))
        .await
        .expect("settings service");
    setup_with_options(
        service.player("player-exclusive").expect("player settings"),
        true,
    )
    .await
}

async fn setup_with_options(
    player_settings: PlayerSettings,
    exclusive_sender_takeover: bool,
) -> Harness {
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
    let sender_attachments = Arc::new(Mutex::new(Vec::new()));
    let output_controls = Arc::new(Mutex::new(Vec::new()));
    let hub = DeviceHub::new(HubConfig {
        identity: DeviceIdentity::new("Living Room".into(), "Chromecast".into(), "dev-1".into()),
        registry: AppRegistry::new(vec![Arc::new(FakeApp {
            launched_setting: launched_setting.clone(),
            exclusive_sender_takeover,
            sender_attachments: sender_attachments.clone(),
            output_controls: output_controls.clone(),
        })])
        .expect("registry"),
        player: player.clone(),
        proxy: proxy.clone(),
        http: reqwest::Client::new(),
        media_http: reqwest::Client::new(),
        data_dir: std::env::temp_dir()
            .join(format!("vibecast-core-tests-{}", uuid::Uuid::new_v4())),
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
    let hub_abort = tokio::spawn(hub.run()).abort_handle();

    Harness {
        client: Framed::new(client_end, CastCodec),
        hub: hub_handle,
        player,
        proxy,
        launched_setting,
        sender_attachments,
        output_controls,
        hub_abort,
    }
}

async fn send(
    client: &mut Framed<DuplexStream, CastCodec>,
    namespace: &str,
    dest: &str,
    json: &str,
) {
    send_as(client, "sender-1", namespace, dest, json).await;
}

async fn send_as(
    client: &mut Framed<DuplexStream, CastCodec>,
    source: &str,
    namespace: &str,
    dest: &str,
    json: &str,
) {
    client
        .send(message::build_string(
            source,
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

async fn next_json_of_type(
    client: &mut Framed<DuplexStream, CastCodec>,
    expected_type: &str,
) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let value = next_json(client).await;
            if value["type"] == expected_type {
                return value;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected_type}"))
}

async fn next_json_for_request(
    client: &mut Framed<DuplexStream, CastCodec>,
    request_id: i64,
) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let value = next_json(client).await;
            if value["requestId"] == request_id {
                return value;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for request {request_id}"))
}

async fn connect_peer(
    hub: &DeviceHubHandle,
    id: u64,
    peer: &str,
) -> Framed<DuplexStream, CastCodec> {
    let (server_end, client_end) = tokio::io::duplex(64 * 1024);
    let (events_tx, mut events_rx) = mpsc::channel::<ServerEvent>(32);
    tokio::spawn(run_connection(
        server_end,
        id,
        Arc::from(peer.to_owned()),
        Arc::new(dummy_auth()),
        events_tx,
    ));
    let hub = hub.clone();
    tokio::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            if hub.send_server_event(event).await.is_err() {
                break;
            }
        }
    });
    Framed::new(client_end, CastCodec)
}

async fn wait_for_sender_attachments(
    attachments: &Arc<Mutex<Vec<SenderAttachment>>>,
    expected: &[SenderAttachment],
) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if attachments.lock().unwrap().as_slice() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sender attachment callbacks did not reach the expected state");
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
    let status: Value = serde_json::from_str(message.payload_utf8.as_deref().unwrap()).unwrap();
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
async fn stale_media_controls_cannot_modify_a_new_selection() {
    let mut h = setup().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    next_json(&mut h.client).await;
    let mut ids = Vec::new();
    for _ in 0..2 {
        send(
            &mut h.client,
            FAKE_NS,
            &transport,
            r#"{"type":"PUSH_MEDIA"}"#,
        )
        .await;
        next_json(&mut h.client).await;
        let status = next_json(&mut h.client).await;
        ids.push(status["status"][0]["mediaSessionId"].as_i64().unwrap());
    }
    assert_ne!(ids[0], ids[1]);
    let before = h.player.commands().len();
    for (n, kind) in ["PLAY", "PAUSE", "SEEK", "STOP", "SET_VOLUME"]
        .iter()
        .enumerate()
    {
        let request = serde_json::json!({
            "type": kind, "requestId": 50 + n, "mediaSessionId": ids[0],
            "currentTime": 40, "volume": {"level": 0.1},
        });
        send(&mut h.client, ns::MEDIA, &transport, &request.to_string()).await;
        let reply = next_json(&mut h.client).await;
        assert_eq!(
            reply["type"], "INVALID_REQUEST",
            "stale {kind} was accepted"
        );
        assert_eq!(reply["requestId"], 50 + n);
        assert_eq!(h.player.commands().len(), before);
    }
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        &serde_json::json!({
            "type": "PAUSE", "requestId": 90, "mediaSessionId": ids[1],
        })
        .to_string(),
    )
    .await;
    let reply = next_json(&mut h.client).await;
    assert_eq!(reply["status"][0]["playerState"], "PAUSED");
    h.hub.shutdown().await;
}

#[tokio::test]
async fn failed_cache_eof_is_error_before_cast_broadcast_and_stays_error() {
    let mut h = setup().await;
    let session = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &session,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    next_json(&mut h.client).await;
    send(
        &mut h.client,
        FAKE_NS,
        &session,
        r#"{"type":"PUSH_CACHE","recoveryAttempts":2}"#,
    )
    .await;
    next_json(&mut h.client).await;
    next_json(&mut h.client).await;
    let recovery = next_json(&mut h.client).await;
    assert_eq!(recovery["type"], "RECOVERY_STATE");
    assert_eq!(recovery["attempts"], 2);
    let proxy = h.proxy.manifests.lock().unwrap()[&session].clone();
    let token = h
        .player
        .commands()
        .into_iter()
        .find_map(|command| match command {
            PlayerCommand::Load { media, .. } => {
                Some(media.streams[0].url.rsplit('/').next().unwrap().to_owned())
            }
            _ => None,
        })
        .unwrap();
    // The fake source is a closed localhost port; it fails without external traffic.
    let response = proxy
        .handle_cached_media(&token, http::Method::GET, http::HeaderMap::new())
        .await
        .unwrap();
    assert_eq!(response.status, 502);
    for _ in 0..2 {
        h.hub
            .send_player_report(PlayerReport::State {
                media_session_id: None,
                session_id: session.clone(),
                player_state: PlayerState::Idle,
                current_time: 55.2,
                duration: Some(230.0),
                idle_reason: Some(vibecast_messages::IdleReason::Finished),
                volume: None,
                muted: None,
            })
            .await
            .unwrap();
        let status = next_json(&mut h.client).await;
        assert_eq!(status["status"][0]["idleReason"], "ERROR");
        assert_eq!(status["status"][0]["currentTime"], 55.2);
        let recovery = next_json(&mut h.client).await;
        assert_eq!(recovery["type"], "RECOVERY_STATE");
        assert_eq!(
            recovery["attempts"], 2,
            "eviction must not replenish retries"
        );
        assert_eq!(recovery["error"], true);
    }
    assert!(!h.proxy.manifests.lock().unwrap().contains_key(&session));
    h.hub.shutdown().await;
}

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
            media_session_id: None,
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
            media_session_id: None,
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
async fn obsolete_load_cannot_replace_new_selection_or_restart_after_stop() {
    for stop in [false, true] {
        let mut h = setup().await;
        let transport = launch(&mut h.client).await;
        send(
            &mut h.client,
            ns::CONNECTION,
            &transport,
            r#"{"type":"CONNECT"}"#,
        )
        .await;
        next_json(&mut h.client).await;
        send(
            &mut h.client,
            ns::MEDIA,
            &transport,
            r#"{"type":"LOAD","requestId":20,"currentTime":1,"media":{"contentId":"slow"}}"#,
        )
        .await;
        next_json(&mut h.client).await; // first LOAD entered resolution
        if stop {
            send(&mut h.client, FAKE_NS, &transport, r#"{"type":"APP_STOP"}"#).await;
        } else {
            send(
                &mut h.client,
                ns::MEDIA,
                &transport,
                r#"{"type":"LOAD","requestId":20,"currentTime":7,"media":{"contentId":"fast"}}"#,
            )
            .await;
        }
        // Reused request IDs must not let the older result through either.
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        let loads: Vec<_> = h
            .player
            .commands()
            .into_iter()
            .filter_map(|c| match c {
                PlayerCommand::Load { media, .. } => Some(media.start_time),
                _ => None,
            })
            .collect();
        assert_eq!(loads, if stop { vec![] } else { vec![7.0] });
        h.hub.shutdown().await;
    }
}

#[tokio::test]
async fn controls_during_resolution_apply_to_the_new_load() {
    for resume in [false, true] {
        let mut h = setup().await;
        let transport = launch(&mut h.client).await;
        send(
            &mut h.client,
            ns::CONNECTION,
            &transport,
            r#"{"type":"CONNECT"}"#,
        )
        .await;
        next_json(&mut h.client).await;
        if resume {
            send(
                &mut h.client,
                FAKE_NS,
                &transport,
                r#"{"type":"HANDLE_PLAY"}"#,
            )
            .await;
            next_json(&mut h.client).await;
        }
        send(
            &mut h.client,
            ns::MEDIA,
            &transport,
            r#"{"type":"LOAD","requestId":20,"media":{"contentId":"slow"}}"#,
        )
        .await;
        let loading = next_json(&mut h.client).await;
        let media_id = loading["status"][0]["mediaSessionId"].as_i64().unwrap();
        for kind in ["PAUSE", "SEEK"] {
            send(
                &mut h.client,
                ns::MEDIA,
                &transport,
                &serde_json::json!({
                    "type": kind, "requestId": 21, "mediaSessionId": media_id, "currentTime": 37.0,
                })
                .to_string(),
            )
            .await;
            next_json(&mut h.client).await;
        }
        if resume {
            send(
                &mut h.client,
                ns::MEDIA,
                &transport,
                &serde_json::json!({
                    "type": "PLAY", "requestId": 22, "mediaSessionId": media_id,
                })
                .to_string(),
            )
            .await;
            next_json(&mut h.client).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let commands = h.player.commands();
        let loaded = commands
            .iter()
            .find_map(|command| match command {
                PlayerCommand::Load { media, .. } => Some(media),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            loaded.autoplay, resume,
            "pending playback intent was lost when resolution completed"
        );
        assert_eq!(loaded.start_time, 37.0, "pending Seek was lost");
        assert!(
            !commands.iter().any(|command| matches!(
                command,
                PlayerCommand::Play { .. }
                    | PlayerCommand::Pause { .. }
                    | PlayerCommand::Seek { .. }
            )),
            "pending controls must not affect the previous decoder load"
        );
        h.hub.shutdown().await;
    }
}

#[tokio::test]
async fn old_decoder_report_cannot_finish_a_pending_load() {
    let mut h = setup().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    next_json(&mut h.client).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":20,"media":{"contentId":"slow"}}"#,
    )
    .await;
    next_json(&mut h.client).await;
    h.hub
        .send_player_report(PlayerReport::State {
            media_session_id: None,
            session_id: transport.clone(),
            player_state: PlayerState::Idle,
            current_time: 111.0,
            duration: Some(111.0),
            idle_reason: Some(vibecast_messages::IdleReason::Finished),
            volume: None,
            muted: None,
        })
        .await
        .unwrap();
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"GET_STATUS","requestId":25}"#,
    )
    .await;
    let status = next_json(&mut h.client).await;
    assert_eq!(
        status["requestId"], 25,
        "old EOF was broadcast for the pending selection"
    );
    assert_eq!(status["status"][0]["currentTime"], 0.0);
    assert!(status["status"][0].get("idleReason").is_none());
    h.hub.shutdown().await;
}

#[tokio::test]
async fn late_decoder_reports_after_stop_cannot_republish_or_overwrite_terminal_state() {
    let mut h = setup().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    next_json(&mut h.client).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":1,"media":{"contentId":"fast"}}"#,
    )
    .await;
    let loading = next_json(&mut h.client).await;
    let media_id = loading["status"][0]["mediaSessionId"].as_i64().unwrap();
    next_json(&mut h.client).await;
    next_json(&mut h.client).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        &serde_json::json!({
            "type": "STOP", "requestId": 2, "mediaSessionId": media_id,
        })
        .to_string(),
    )
    .await;
    assert_eq!(
        next_json(&mut h.client).await["status"][0]["idleReason"],
        "CANCELLED"
    );
    for reason in [
        Some(vibecast_messages::IdleReason::Cancelled),
        Some(vibecast_messages::IdleReason::Finished),
        Some(vibecast_messages::IdleReason::Error),
        None,
    ] {
        h.hub
            .send_player_report(PlayerReport::State {
                media_session_id: None,
                session_id: transport.clone(),
                player_state: if reason.is_some() {
                    PlayerState::Idle
                } else {
                    PlayerState::Playing
                },
                current_time: 42.0,
                duration: Some(99.0),
                idle_reason: reason,
                volume: None,
                muted: None,
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"GET_STATUS","requestId":3}"#,
    )
    .await;
    let status = next_json(&mut h.client).await;
    assert_eq!(
        status["requestId"], 3,
        "late report must not be republished as a fresh app state"
    );
    assert_eq!(status["status"][0]["idleReason"], "CANCELLED");
    assert_eq!(status["status"][0]["currentTime"], 0.0);
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":4,"media":{"contentId":"fast"}}"#,
    )
    .await;
    for _ in 0..3 {
        next_json(&mut h.client).await;
    }
    h.hub
        .send_player_report(PlayerReport::State {
            media_session_id: None,
            session_id: transport.clone(),
            player_state: PlayerState::Playing,
            current_time: 7.0,
            duration: Some(90.0),
            idle_reason: None,
            volume: None,
            muted: None,
        })
        .await
        .unwrap();
    let resumed = next_json(&mut h.client).await;
    assert_eq!(resumed["status"][0]["playerState"], "PLAYING");
    assert_eq!(resumed["status"][0]["currentTime"], 7.0);
    h.hub.shutdown().await;
}

#[tokio::test]
async fn aborting_hub_unregisters_session_proxy() {
    let mut h = setup().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    next_json(&mut h.client).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":20,"media":{"contentId":"fast"}}"#,
    )
    .await;
    for _ in 0..3 {
        next_json(&mut h.client).await;
    }
    assert!(h.proxy.manifests.lock().unwrap().contains_key(&transport));
    h.hub_abort.abort();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while h.proxy.manifests.lock().unwrap().contains_key(&transport) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("forced shutdown must release proxy registrations");
}

#[tokio::test]
async fn saturated_callback_cannot_deadlock_hub_control_or_shutdown() {
    let mut h = setup().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    next_json(&mut h.client).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":20,"media":{"contentId":"fast"}}"#,
    )
    .await;
    for _ in 0..3 {
        next_json(&mut h.client).await;
    }
    send(
        &mut h.client,
        FAKE_NS,
        &transport,
        r#"{"type":"FLOOD_CALLBACK"}"#,
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let response = next_json(&mut h.client).await;
            if response["type"] == "RECEIVER_STATUS"
                && response["status"]["applications"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
            {
                break;
            }
        }
    })
    .await
    .expect("callback overload must tear down the session and unblock the hub");
    assert!(h.player.commands().iter().any(|command| matches!(command,
        PlayerCommand::Stop { session_id } if session_id == &transport)));
    assert!(!h.proxy.manifests.lock().unwrap().contains_key(&transport));
    send(
        &mut h.client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"GET_STATUS","requestId":99}"#,
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while next_json(&mut h.client).await["requestId"] != 99 {}
    })
    .await
    .expect("receiver controls must remain responsive after callback overload");
    tokio::time::timeout(std::time::Duration::from_secs(1), h.hub.shutdown())
        .await
        .expect("overloaded callback must not block shutdown");
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
        r#"{"type":"SEEK","requestId":73,"mediaSessionId":2,"currentTime":5}"#,
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
async fn failed_replacement_load_stops_the_previous_decoder() {
    let mut h = setup().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    next_json(&mut h.client).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":1,"media":{"contentId":"fast"}}"#,
    )
    .await;
    for _ in 0..3 {
        next_json(&mut h.client).await;
    }
    let before = h.player.commands().len();
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"LOAD","requestId":2,"media":{"contentId":"fail"}}"#,
    )
    .await;
    for _ in 0..3 {
        next_json(&mut h.client).await;
    }
    assert!(
        h.player.commands()[before..]
            .iter()
            .any(|command| matches!(command,
                PlayerCommand::Stop { session_id } if session_id == &transport
            )),
        "old playback must not outlive a failed replacement and lost media state"
    );
    // Stop acknowledgement must not erase LOAD_FAILED/ERROR or look like a
    // second resolver failure to the app.
    h.hub
        .send_player_report(PlayerReport::State {
            media_session_id: None,
            session_id: transport.clone(),
            player_state: PlayerState::Idle,
            current_time: 0.0,
            duration: None,
            idle_reason: Some(vibecast_messages::IdleReason::Cancelled),
            volume: None,
            muted: None,
        })
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"GET_STATUS","requestId":3}"#,
    )
    .await;
    let terminal = next_json(&mut h.client).await;
    assert_eq!(terminal["requestId"], 3);
    assert_eq!(terminal["status"][0]["idleReason"], "ERROR");
    h.hub.shutdown().await;
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
            media_session_id: None,
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
async fn correlated_old_reports_and_controls_cannot_finish_or_stop_a_replacement() {
    let mut h = setup().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    next_json(&mut h.client).await;
    let mut ids = Vec::new();
    for request in [1, 2] {
        send(
            &mut h.client,
            ns::MEDIA,
            &transport,
            &serde_json::json!({
                "type":"LOAD", "requestId":request, "media":{"contentId":"fast"},
            })
            .to_string(),
        )
        .await;
        ids.push(
            next_json(&mut h.client).await["status"][0]["mediaSessionId"]
                .as_i64()
                .unwrap(),
        );
        next_json(&mut h.client).await;
        next_json(&mut h.client).await;
    }
    assert_ne!(ids[0], ids[1]);
    let dispatched: Vec<_> = h
        .player
        .commands()
        .iter()
        .filter_map(|c| match c {
            PlayerCommand::Load { media, .. } => media.media_session_id,
            _ => None,
        })
        .collect();
    assert_eq!(dispatched, ids);
    let before = h.player.commands().len();
    h.hub
        .send_player_report(PlayerReport::State {
            session_id: transport.clone(),
            media_session_id: Some(ids[0]),
            player_state: PlayerState::Idle,
            current_time: 99.0,
            duration: Some(99.0),
            idle_reason: Some(vibecast_messages::IdleReason::Finished),
            volume: None,
            muted: None,
        })
        .await
        .unwrap();
    h.hub
        .send_player_report(PlayerReport::Error {
            session_id: transport.clone(),
            media_session_id: Some(ids[0]),
            code: "OLD".into(),
            message: "old load failed".into(),
        })
        .await
        .unwrap();
    for control in [
        vibecast_player_api::PlayerControlRequest::Stop,
        vibecast_player_api::PlayerControlRequest::Seek { position: 99.0 },
    ] {
        h.hub
            .send_player_report(PlayerReport::ControlRequest {
                session_id: transport.clone(),
                media_session_id: Some(ids[0]),
                control,
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"GET_STATUS","requestId":3}"#,
    )
    .await;
    let state = next_json(&mut h.client).await;
    assert_eq!(
        state["requestId"], 3,
        "old report was published as replacement state"
    );
    assert_eq!(state["status"][0]["playerState"], "BUFFERING");
    assert_eq!(h.player.commands().len(), before);
    for (media_session_id, position) in [(Some(ids[1]), 7.0), (None, 8.0)] {
        h.hub
            .send_player_report(PlayerReport::State {
                session_id: transport.clone(),
                media_session_id,
                player_state: PlayerState::Playing,
                current_time: position,
                duration: Some(90.0),
                idle_reason: None,
                volume: None,
                muted: None,
            })
            .await
            .unwrap();
        assert_eq!(
            next_json(&mut h.client).await["status"][0]["currentTime"],
            position
        );
    }
    // Output-wide volume remains usable even on a delayed old-title report.
    h.hub
        .send_player_report(PlayerReport::ControlRequest {
            session_id: transport.clone(),
            media_session_id: Some(ids[0]),
            control: vibecast_player_api::PlayerControlRequest::Volume {
                level: 0.75,
                muted: false,
            },
        })
        .await
        .unwrap();
    assert_eq!(
        next_json(&mut h.client).await["status"]["volume"]["level"],
        0.75
    );
    h.hub
        .send_player_report(PlayerReport::State {
            session_id: transport.clone(),
            media_session_id: Some(ids[0]),
            player_state: PlayerState::Idle,
            current_time: 99.0,
            duration: None,
            idle_reason: Some(vibecast_messages::IdleReason::Finished),
            volume: Some(0.25),
            muted: None,
        })
        .await
        .unwrap();
    let receiver = next_json(&mut h.client).await;
    assert_eq!(receiver["type"], "RECEIVER_STATUS");
    assert_eq!(receiver["status"]["volume"]["level"], 0.25);
    send(
        &mut h.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"GET_STATUS","requestId":4}"#,
    )
    .await;
    let media = next_json(&mut h.client).await;
    assert_eq!(media["status"][0]["currentTime"], 8.0);
    assert_eq!(media["status"][0]["volume"]["level"], 0.25);
    h.hub.shutdown().await;
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
async fn last_observer_disconnect_does_not_expire_while_launch_owner_is_attached() {
    retained_session_disconnect(false, DisconnectAction::ObserverLost).await;
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
                if status["type"] == "RECEIVER_STATUS" && status["requestId"] == 81 {
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

#[tokio::test]
async fn stalled_observer_does_not_block_active_owner_or_player_reports() {
    let mut h = setup_exclusive().await;
    let transport = launch(&mut h.client).await;
    send(
        &mut h.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut h.client).await["type"], "MEDIA_STATUS");
    let (mut owner_tx, mut owner_rx) = h.client.split();
    let drain = tokio::spawn(async move {
        while let Some(Ok(message)) = owner_rx.next().await {
            let value: Value =
                serde_json::from_str(message.payload_utf8.as_deref().unwrap()).unwrap();
            if value["type"] == "PONG" {
                return;
            }
        }
        panic!("active owner was disconnected by a stalled observer");
    });

    let (server, client) = tokio::io::duplex(256);
    let (events_tx, mut events_rx) = mpsc::channel(32);
    tokio::spawn(run_connection(
        server,
        2,
        Arc::from("stalled-observer"),
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
    let mut observer = Framed::new(client, CastCodec);
    send(
        &mut observer,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    // Confirm subscription, then stop consuming any observer messages.
    assert_eq!(next_json(&mut observer).await["type"], "MEDIA_STATUS");
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        for n in 0..300 {
            h.hub
                .send_player_report(PlayerReport::State {
                    media_session_id: None,
                    session_id: transport.clone(),
                    player_state: PlayerState::Playing,
                    current_time: n as f64,
                    duration: Some(600.0),
                    idle_reason: None,
                    volume: None,
                    muted: None,
                })
                .await
                .unwrap();
        }
        owner_tx
            .send(message::build_string(
                "sender-1",
                &transport,
                FAKE_NS,
                r#"{"type":"PING"}"#.into(),
            ))
            .await
            .unwrap();
        drain.await.unwrap();
        h.hub.shutdown().await;
    })
    .await
    .expect("a slow observer must not block reports, owner control or shutdown");
    assert!(!h
        .player
        .commands()
        .iter()
        .any(|c| matches!(c, PlayerCommand::Stop { .. })));
}

#[tokio::test]
async fn replacing_running_app_announces_removal_before_launch_response() {
    let mut harness = setup_exclusive().await;
    let previous = launch(&mut harness.client).await;
    // Chrome may already observe a mobile-owned app when it starts its own
    // route. It needs a removal event before the replacement launch response.
    send(
        &mut harness.client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"LAUNCH","requestId":42,"appId":"APP1"}"#,
    )
    .await;
    let ended = next_json(&mut harness.client).await;
    assert_eq!(ended["type"], "RECEIVER_STATUS");
    assert_eq!(ended["requestId"], 0);
    assert!(ended["status"]["applications"]
        .as_array()
        .unwrap()
        .is_empty());
    let response = harness.client.next().await.unwrap().unwrap();
    assert_eq!(response.destination_id, "sender-1");
    let response: Value = serde_json::from_str(response.payload_utf8.as_deref().unwrap()).unwrap();
    assert_eq!(response["requestId"], 42);
    let replacement = response["status"]["applications"][0]["transportId"]
        .as_str()
        .unwrap();
    assert_ne!(replacement, previous);
    send(
        &mut harness.client,
        ns::CONNECTION,
        replacement,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    send(
        &mut harness.client,
        FAKE_NS,
        replacement,
        r#"{"type":"CLAIM"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "PONG");
}

#[tokio::test]
async fn exclusive_app_transfers_control_to_the_new_physical_sender() {
    let mut harness = setup_exclusive().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");

    let (server_end, client_end) = tokio::io::duplex(64 * 1024);
    let (events_tx, mut events_rx) = mpsc::channel::<ServerEvent>(32);
    tokio::spawn(run_connection(
        server_end,
        2,
        Arc::from("new-owner"),
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
    let mut new_owner = Framed::new(client_end, CastCodec);
    send(
        &mut new_owner,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        &mut new_owner,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;

    assert_eq!(next_json(&mut new_owner).await["type"], "MEDIA_STATUS");
    // Reproduce automatic observer CONNECT/GET_STATUS retries. They must
    // neither kick the owner nor consume the app handshake.
    for _ in 0..3 {
        send(
            &mut new_owner,
            ns::CONNECTION,
            &transport,
            r#"{"type":"CONNECT"}"#,
        )
        .await;
        assert_eq!(next_json(&mut new_owner).await["type"], "MEDIA_STATUS");
        send(
            &mut new_owner,
            ns::MEDIA,
            &transport,
            r#"{"type":"GET_STATUS","requestId":4}"#,
        )
        .await;
        assert_eq!(next_json(&mut new_owner).await["type"], "MEDIA_STATUS");
        send(
            &mut harness.client,
            FAKE_NS,
            &transport,
            r#"{"type":"PING"}"#,
        )
        .await;
        assert_eq!(next_json(&mut harness.client).await["type"], "PONG");
    }
    send(&mut new_owner, FAKE_NS, &transport, r#"{"type":"CLAIM"}"#).await;
    let ended = next_json(&mut harness.client).await;
    assert_eq!(ended["type"], "RECEIVER_STATUS");
    assert!(ended["status"]["applications"]
        .as_array()
        .unwrap()
        .is_empty());
    let close = harness.client.next().await.unwrap().unwrap();
    assert_eq!(close.namespace, ns::CONNECTION);
    assert_eq!(close.destination_id, "sender-1");
    let close: Value = serde_json::from_str(close.payload_utf8.as_deref().unwrap()).unwrap();
    assert_eq!(close["type"], "CLOSE");
    assert_eq!(next_json(&mut new_owner).await["type"], "PONG");

    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"OLD_OWNER_COMMAND"}"#,
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.client.next())
            .await
            .is_err(),
        "superseded owner must not receive an app response"
    );
    // An automatic reconnect after CLOSE must not claim the session back.
    for _ in 0..3 {
        send(
            &mut harness.client,
            ns::CONNECTION,
            &transport,
            r#"{"type":"CONNECT"}"#,
        )
        .await;
        assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    }
    let volume_commands_before = harness
        .player
        .commands()
        .iter()
        .filter(|command| matches!(command, PlayerCommand::Volume { .. }))
        .count();
    send(
        &mut harness.client,
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"SET_VOLUME","requestId":90,"volume":{"level":0.11}}"#,
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.client.next())
            .await
            .is_err(),
        "superseded owner must not receive a volume response"
    );
    assert_eq!(
        harness
            .player
            .commands()
            .iter()
            .filter(|command| matches!(command, PlayerCommand::Volume { .. }))
            .count(),
        volume_commands_before,
        "superseded owner must not alter output volume"
    );
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CLOSE"}"#,
    )
    .await;
    send(
        &mut harness.client,
        ns::RECEIVER,
        "receiver-0",
        &serde_json::json!({
            "type": "STOP",
            "requestId": 91,
            "sessionId": transport,
        })
        .to_string(),
    )
    .await;

    send(
        &mut new_owner,
        FAKE_NS,
        &transport,
        r#"{"type":"NEW_OWNER_COMMAND"}"#,
    )
    .await;
    assert_eq!(next_json(&mut new_owner).await["type"], "PONG");

    drop(harness.client);
    assert_eq!(next_json(&mut new_owner).await["type"], "RECEIVER_STATUS");
    assert!(!harness
        .player
        .commands()
        .iter()
        .any(|command| matches!(command, PlayerCommand::Stop { .. })));

    send(
        &mut new_owner,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CLOSE"}"#,
    )
    .await;
    let stopped = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let status = next_json(&mut new_owner).await;
            if status["type"] == "RECEIVER_STATUS" {
                break status;
            }
        }
    })
    .await
    .expect("closing the current owner must stop the session");
    assert!(stopped["status"]["applications"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn exclusive_owner_reconnects_on_a_new_socket_without_taking_over() {
    let mut harness = setup_exclusive().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    wait_for_sender_attachments(&harness.sender_attachments, &[SenderAttachment::Initial]).await;
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"KEEP_ON_DISCONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "READY");
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PUSH_MEDIA"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");

    drop(harness.client);
    // Let the hub observe the physical disconnect before the replacement
    // socket arrives. The peer host and Cast source id stay the same.
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let mut reconnected = connect_peer(&harness.hub, 2, "peer").await;
    send(
        &mut reconnected,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        &mut reconnected,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut reconnected).await["type"], "MEDIA_STATUS");
    wait_for_sender_attachments(
        &harness.sender_attachments,
        &[SenderAttachment::Initial, SenderAttachment::Reconnected],
    )
    .await;
    send(&mut reconnected, FAKE_NS, &transport, r#"{"type":"PING"}"#).await;
    assert_eq!(next_json(&mut reconnected).await["type"], "PONG");
    assert!(!harness
        .player
        .commands()
        .iter()
        .any(|command| matches!(command, PlayerCommand::Stop { .. })));

    // Reconnect grace only covers a lost network socket. An explicit app
    // CLOSE from the reattached owner must still terminate immediately.
    send(
        &mut reconnected,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CLOSE"}"#,
    )
    .await;
    let stopped = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let status = next_json(&mut reconnected).await;
            if status["type"] == "RECEIVER_STATUS" {
                break status;
            }
        }
    })
    .await
    .expect("explicit CLOSE from the reconnected owner must stop immediately");
    assert!(stopped["status"]["applications"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(harness
        .player
        .commands()
        .iter()
        .any(|command| matches!(command, PlayerCommand::Stop { .. })));
}

#[tokio::test]
async fn queue_bootstrap_takes_over_but_superseded_queue_probe_cannot_take_back() {
    let mut harness = setup_exclusive().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    wait_for_sender_attachments(&harness.sender_attachments, &[SenderAttachment::Initial]).await;

    // iOS commonly enters an existing route through a media queue bootstrap,
    // without the stronger app-specific ownership handshake. Reuse the same
    // generic source id on a different peer to prove source ids alone are not
    // mistaken for a network reconnect.
    let mut ipad = connect_peer(&harness.hub, 2, "ipad-peer").await;
    send(
        &mut ipad,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send(
        &mut ipad,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut ipad).await["type"], "MEDIA_STATUS");
    send(
        &mut ipad,
        ns::MEDIA,
        &transport,
        r#"{"type":"GET_STATUS","requestId":40}"#,
    )
    .await;
    assert_eq!(next_json(&mut ipad).await["requestId"], 40);
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PING"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "PONG");
    assert_eq!(
        harness.sender_attachments.lock().unwrap().as_slice(),
        &[SenderAttachment::Initial],
        "passive CONNECT and GET_STATUS must remain observer-only"
    );

    send(
        &mut ipad,
        ns::MEDIA,
        &transport,
        r#"{"type":"QUEUE_GET_ITEM_IDS","requestId":41}"#,
    )
    .await;
    let ended = next_json(&mut harness.client).await;
    assert_eq!(ended["type"], "RECEIVER_STATUS");
    assert!(ended["status"]["applications"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(next_json(&mut harness.client).await["type"], "CLOSE");
    assert_eq!(next_json(&mut ipad).await["type"], "QUEUE_ITEM_IDS");
    wait_for_sender_attachments(
        &harness.sender_attachments,
        &[SenderAttachment::Initial, SenderAttachment::Takeover],
    )
    .await;

    // The displaced sender may auto-open its old app channel and repeat the
    // same queue probe. That is not a fresh user takeover and must be ignored.
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    send(
        &mut harness.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"QUEUE_GET_ITEM_IDS","requestId":42}"#,
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), harness.client.next())
            .await
            .is_err(),
        "a superseded controller's automatic queue probe must not reclaim ownership"
    );
    send(&mut ipad, FAKE_NS, &transport, r#"{"type":"PING"}"#).await;
    assert_eq!(next_json(&mut ipad).await["type"], "PONG");

    // A deliberate strong handshake is still allowed to take the route back,
    // and must cancel the now-displaced iPad controller in the same way.
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"CLAIM"}"#,
    )
    .await;
    let ended = next_json(&mut ipad).await;
    assert_eq!(ended["type"], "RECEIVER_STATUS");
    assert!(ended["status"]["applications"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(next_json(&mut ipad).await["type"], "CLOSE");
    assert_eq!(next_json(&mut harness.client).await["type"], "PONG");
    wait_for_sender_attachments(
        &harness.sender_attachments,
        &[
            SenderAttachment::Initial,
            SenderAttachment::Takeover,
            SenderAttachment::Takeover,
        ],
    )
    .await;
}

#[tokio::test]
async fn displaced_controller_needs_a_fresh_app_connect_before_weak_rejoin() {
    let mut harness = setup_exclusive().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PUSH_MEDIA"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    let playing = next_json(&mut harness.client).await;
    let media_id = playing["status"][0]["mediaSessionId"].as_i64().unwrap();

    let mut replacement = connect_peer(&harness.hub, 2, "replacement-peer").await;
    send_as(
        &mut replacement,
        "sender-2",
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send_as(
        &mut replacement,
        "sender-2",
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut replacement).await["type"], "MEDIA_STATUS");
    send_as(
        &mut replacement,
        "sender-2",
        FAKE_NS,
        &transport,
        r#"{"type":"CLAIM"}"#,
    )
    .await;
    assert_eq!(
        next_json(&mut harness.client).await["type"],
        "RECEIVER_STATUS"
    );
    assert_eq!(next_json(&mut harness.client).await["type"], "CLOSE");
    let _ = next_json_of_type(&mut replacement, "PONG").await;

    drop(harness.client);
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let mut returned = connect_peer(&harness.hub, 3, "peer").await;
    send(
        &mut returned,
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;

    // Merely opening a new physical socket is not enough. A weak queue probe
    // must be ignored until that logical sender explicitly CONNECTs to this
    // app transport.
    send(
        &mut returned,
        ns::MEDIA,
        &transport,
        r#"{"type":"QUEUE_GET_ITEM_IDS","requestId":60}"#,
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), returned.next())
            .await
            .is_err()
    );
    send(&mut replacement, FAKE_NS, &transport, r#"{"type":"PING"}"#).await;
    let _ = next_json_of_type(&mut replacement, "PONG").await;

    send(
        &mut returned,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut returned).await["type"], "MEDIA_STATUS");
    let before = harness.player.commands().len();
    for (request_id, kind) in [(61, "PAUSE"), (62, "PLAY"), (63, "SEEK")] {
        send(
            &mut returned,
            ns::MEDIA,
            &transport,
            &serde_json::json!({
                "type": kind,
                "requestId": request_id,
                "mediaSessionId": media_id,
                "currentTime": 20.0,
            })
            .to_string(),
        )
        .await;
    }
    send(
        &mut returned,
        ns::MEDIA,
        &transport,
        r#"{"type":"GET_STATUS","requestId":64}"#,
    )
    .await;
    assert_eq!(next_json(&mut returned).await["requestId"], 64);
    assert_eq!(
        harness.player.commands().len(),
        before,
        "a freshly connected but still superseded controller must not operate current media"
    );

    // A fresh app-level CONNECT is deliberately the user-presence boundary for
    // iOS' weak queue bootstrap. It may now retake ownership without the
    // app-specific strong handshake, and must kick the replacement controller.
    send(
        &mut returned,
        ns::MEDIA,
        &transport,
        r#"{"type":"QUEUE_GET_ITEM_IDS","requestId":65}"#,
    )
    .await;
    assert_eq!(next_json(&mut replacement).await["type"], "RECEIVER_STATUS");
    assert_eq!(next_json(&mut replacement).await["type"], "CLOSE");
    assert_eq!(next_json(&mut returned).await["type"], "QUEUE_ITEM_IDS");
}

#[tokio::test]
async fn reconnect_grace_starts_only_after_the_last_owner_socket_disconnects() {
    let mut harness = setup_exclusive().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"KEEP_ON_DISCONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "READY");
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PUSH_MEDIA"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");

    let owner_source = "a4ba9c7e-dab2-4e23-8777-31dc82502f87";
    let sdk_connect =
        r#"{"type":"CONNECT","senderInfo":{"sdkType":2,"platform":3,"model":"iPad"}}"#;
    let mut second_owner = connect_peer(&harness.hub, 2, "peer").await;
    send_as(
        &mut second_owner,
        owner_source,
        ns::CONNECTION,
        "receiver-0",
        sdk_connect,
    )
    .await;
    send_as(
        &mut second_owner,
        owner_source,
        ns::CONNECTION,
        &transport,
        sdk_connect,
    )
    .await;
    assert_eq!(next_json(&mut second_owner).await["type"], "MEDIA_STATUS");
    wait_for_sender_attachments(
        &harness.sender_attachments,
        &[SenderAttachment::Initial, SenderAttachment::Initial],
    )
    .await;

    let mut observer = connect_peer(&harness.hub, 3, "observer-peer").await;
    send_as(
        &mut observer,
        "sender-0",
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;

    drop(harness.client);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    send_as(
        &mut second_owner,
        owner_source,
        FAKE_NS,
        &transport,
        r#"{"type":"PING"}"#,
    )
    .await;
    let _ = next_json_of_type(&mut second_owner, "PONG").await;
    assert!(!harness
        .player
        .commands()
        .iter()
        .any(|command| matches!(command, PlayerCommand::Stop { .. })));

    drop(second_owner);
    // If losing the first socket had incorrectly started the ten-second test
    // grace, the app would already be gone here. Correct behavior measures the
    // deadline from the loss of this final proven owner socket.
    tokio::time::sleep(std::time::Duration::from_millis(9_200)).await;
    send_as(
        &mut observer,
        "sender-0",
        ns::RECEIVER,
        "receiver-0",
        r#"{"type":"GET_STATUS","requestId":70}"#,
    )
    .await;
    let current = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let status = next_json(&mut observer).await;
            if status["requestId"] == 70 {
                break status;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        current["status"]["applications"][0]["transportId"],
        transport
    );

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let status = next_json(&mut observer).await;
            if status["type"] == "RECEIVER_STATUS"
                && status["status"]["applications"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
            {
                break;
            }
        }
    })
    .await
    .expect("the final owner socket must start reconnect grace and eventually stop the app");
    assert!(harness
        .player
        .commands()
        .iter()
        .any(|command| matches!(command, PlayerCommand::Stop { .. })));
}

#[tokio::test]
async fn takeover_closes_every_socket_of_the_previous_owner_device() {
    let mut harness = setup_exclusive().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");

    let service_source = "cbe183d7-0c46-48b8-830a-6ea872003b1b";
    let sdk_connect = r#"{"type":"CONNECT","senderInfo":{"sdkType":2,"platform":1}}"#;
    let mut service_socket = connect_peer(&harness.hub, 2, "peer").await;
    send_as(
        &mut service_socket,
        service_source,
        ns::CONNECTION,
        "receiver-0",
        sdk_connect,
    )
    .await;
    send_as(
        &mut service_socket,
        service_source,
        ns::CONNECTION,
        &transport,
        sdk_connect,
    )
    .await;
    assert_eq!(next_json(&mut service_socket).await["type"], "MEDIA_STATUS");
    wait_for_sender_attachments(
        &harness.sender_attachments,
        &[SenderAttachment::Initial, SenderAttachment::Initial],
    )
    .await;

    let ipad_source = "cec43ed0-6d3d-4d5c-ad69-b5467ef91910";
    let ipad_connect = r#"{"type":"CONNECT","senderInfo":{"sdkType":2,"platform":3}}"#;
    let mut ipad = connect_peer(&harness.hub, 3, "ipad-peer").await;
    send_as(
        &mut ipad,
        ipad_source,
        ns::CONNECTION,
        "receiver-0",
        ipad_connect,
    )
    .await;
    send_as(
        &mut ipad,
        ipad_source,
        ns::CONNECTION,
        &transport,
        ipad_connect,
    )
    .await;
    assert_eq!(next_json(&mut ipad).await["type"], "MEDIA_STATUS");
    send_as(
        &mut ipad,
        ipad_source,
        ns::MEDIA,
        &transport,
        r#"{"type":"QUEUE_GET_ITEM_IDS","requestId":80}"#,
    )
    .await;

    for previous in [&mut harness.client, &mut service_socket] {
        let ended = next_json(previous).await;
        assert_eq!(ended["type"], "RECEIVER_STATUS");
        assert!(ended["status"]["applications"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(next_json(previous).await["type"], "CLOSE");
    }
    assert_eq!(next_json(&mut ipad).await["type"], "QUEUE_ITEM_IDS");
    wait_for_sender_attachments(
        &harness.sender_attachments,
        &[
            SenderAttachment::Initial,
            SenderAttachment::Initial,
            SenderAttachment::Takeover,
        ],
    )
    .await;
}

#[tokio::test]
async fn weak_queue_takeover_requires_evidence_of_an_active_cast_sender() {
    let mut harness = setup_exclusive().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");

    let mut observer = connect_peer(&harness.hub, 2, "observer-peer").await;
    send_as(
        &mut observer,
        "sender-0",
        ns::CONNECTION,
        "receiver-0",
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    send_as(
        &mut observer,
        "sender-0",
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut observer).await["type"], "MEDIA_STATUS");
    send_as(
        &mut observer,
        "sender-0",
        ns::MEDIA,
        &transport,
        r#"{"type":"QUEUE_GET_ITEM_IDS","requestId":90}"#,
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), observer.next())
            .await
            .is_err(),
        "generic sender-0 without SDK evidence must remain a status observer"
    );
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PING"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "PONG");

    let ipad_source = "cec43ed0-6d3d-4d5c-ad69-b5467ef91910";
    let ipad_connect =
        r#"{"type":"CONNECT","senderInfo":{"sdkType":2,"platform":3,"model":"iPad"}}"#;
    let mut ipad = connect_peer(&harness.hub, 3, "ipad-peer").await;
    send_as(
        &mut ipad,
        ipad_source,
        ns::CONNECTION,
        "receiver-0",
        ipad_connect,
    )
    .await;
    send_as(
        &mut ipad,
        ipad_source,
        ns::CONNECTION,
        &transport,
        ipad_connect,
    )
    .await;
    assert_eq!(next_json(&mut ipad).await["type"], "MEDIA_STATUS");
    send_as(
        &mut ipad,
        ipad_source,
        ns::MEDIA,
        &transport,
        r#"{"type":"QUEUE_GET_ITEM_IDS","requestId":91}"#,
    )
    .await;
    assert_eq!(
        next_json(&mut harness.client).await["type"],
        "RECEIVER_STATUS"
    );
    assert_eq!(next_json(&mut harness.client).await["type"], "CLOSE");
    assert_eq!(next_json(&mut ipad).await["type"], "QUEUE_ITEM_IDS");
    wait_for_sender_attachments(
        &harness.sender_attachments,
        &[SenderAttachment::Initial, SenderAttachment::Takeover],
    )
    .await;
}

#[tokio::test]
async fn exclusive_app_keeps_multiple_logical_senders_on_one_connection() {
    let mut harness = setup_exclusive().await;
    let transport = launch(&mut harness.client).await;
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");

    send_as(
        &mut harness.client,
        "sender-2",
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "MEDIA_STATUS");
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"STILL_OWNER"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "PONG");
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

#[tokio::test]
async fn secondary_controller_operates_current_media_without_claiming_ownership() {
    let mut harness = setup_exclusive().await;
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
    let status = next_json(&mut harness.client).await;
    let media_id = status["status"][0]["mediaSessionId"].as_i64().unwrap();

    let (server_end, client_end) = tokio::io::duplex(64 * 1024);
    let (events_tx, mut events_rx) = mpsc::channel::<ServerEvent>(32);
    tokio::spawn(run_connection(
        server_end,
        2,
        Arc::from("ha-observer"),
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

    // A connection must subscribe to this exact app transport before controlling it.
    for subscribed in [false, true] {
        if subscribed {
            send(
                &mut observer,
                ns::CONNECTION,
                &transport,
                r#"{"type":"CONNECT"}"#,
            )
            .await;
            let _ = next_json(&mut observer).await;
        }
        let before = harness.player.commands().len();
        for request in [
            serde_json::json!({"type":"PAUSE","requestId":50,"mediaSessionId":if subscribed { media_id+1 } else { media_id }}),
            serde_json::json!({"type":"PLAY","requestId":51}),
            serde_json::json!({"type":"LOAD","requestId":53,"media":{"contentId":"replacement","contentType":"audio/mpeg"}}),
        ] {
            send(&mut observer, ns::MEDIA, &transport, &request.to_string()).await;
        }
        send(
            &mut observer,
            ns::MEDIA,
            &transport,
            r#"{"type":"GET_STATUS","requestId":59}"#,
        )
        .await;
        assert_eq!(next_json(&mut observer).await["requestId"], 59);
        assert_eq!(harness.player.commands().len(), before);
    }

    for (request_id, kind, expected) in [
        (60, "PAUSE", "PAUSED"),
        (61, "PLAY", "PLAYING"),
        (62, "SEEK", "PLAYING"),
    ] {
        let request = serde_json::json!({"type":kind,"requestId":request_id,"mediaSessionId":media_id,"currentTime":33.0});
        send(&mut observer, ns::MEDIA, &transport, &request.to_string()).await;
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(1), next_json(&mut observer))
                .await
                .expect("secondary controller must receive command acknowledgement, not time out");
        assert_eq!(response["requestId"], request_id);
        assert_eq!(response["status"][0]["playerState"], expected);
        assert_eq!(
            next_json(&mut harness.client).await["requestId"],
            request_id
        );
    }
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PING"}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["type"], "PONG");
    let commands = harness.player.commands();
    assert!(commands
        .iter()
        .any(|c| matches!(c, PlayerCommand::Pause { .. })));
    assert!(commands
        .iter()
        .any(|c| matches!(c, PlayerCommand::Play { .. })));
    assert!(commands
        .iter()
        .any(|c| matches!(c, PlayerCommand::Seek { position: 33.0, .. })));

    // A real takeover still revokes the previous owner, even if it automatically
    // reconnects as an apparent observer with the current media session id.
    send(&mut observer, FAKE_NS, &transport, r#"{"type":"CLAIM"}"#).await;
    assert_eq!(
        next_json(&mut harness.client).await["type"],
        "RECEIVER_STATUS"
    );
    assert_eq!(next_json(&mut harness.client).await["type"], "CLOSE");
    assert_eq!(next_json(&mut observer).await["type"], "PONG");
    send(
        &mut harness.client,
        ns::CONNECTION,
        &transport,
        r#"{"type":"CONNECT"}"#,
    )
    .await;
    let _ = next_json(&mut harness.client).await;
    let before = harness.player.commands().len();
    let before_controls = harness.output_controls.lock().unwrap().len();
    send(
        &mut harness.client,
        ns::MEDIA,
        &transport,
        &serde_json::json!({"type":"PAUSE","requestId":70,"mediaSessionId":media_id}).to_string(),
    )
    .await;
    send(
        &mut harness.client,
        ns::MEDIA,
        &transport,
        &serde_json::json!({
            "type": "QUEUE_UPDATE",
            "requestId": 72,
            "mediaSessionId": media_id,
            "jump": 1,
        })
        .to_string(),
    )
    .await;
    send(
        &mut harness.client,
        ns::MEDIA,
        &transport,
        r#"{"type":"GET_STATUS","requestId":71}"#,
    )
    .await;
    assert_eq!(next_json(&mut harness.client).await["requestId"], 71);
    assert_eq!(harness.player.commands().len(), before);
    assert_eq!(
        harness.output_controls.lock().unwrap().len(),
        before_controls
    );
}

#[tokio::test]
async fn home_assistant_observer_controls_transport_and_real_app_queue_without_takeover() {
    let mut harness = setup_exclusive().await;
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
    let status = next_json(&mut harness.client).await;
    let media_id = status["status"][0]["mediaSessionId"].as_i64().unwrap();
    wait_for_sender_attachments(&harness.sender_attachments, &[SenderAttachment::Initial]).await;

    let mut observer = connect_peer(&harness.hub, 2, "ha-observer").await;
    let connect = r#"{"type":"CONNECT","userAgent":"PyChromecast/14"}"#;
    send_as(
        &mut observer,
        "sender-0",
        ns::CONNECTION,
        "receiver-0",
        connect,
    )
    .await;
    send_as(
        &mut observer,
        "sender-0",
        ns::CONNECTION,
        &transport,
        connect,
    )
    .await;
    let observer_status = next_json(&mut observer).await;
    assert_eq!(observer_status["type"], "MEDIA_STATUS");
    assert_eq!(observer_status["status"][0]["mediaSessionId"], media_id);
    let commands = observer_status["status"][0]["supportedMediaCommands"]
        .as_i64()
        .unwrap();
    assert_eq!(
        commands & vibecast_messages::media_command::QUEUE_NAVIGATION,
        vibecast_messages::media_command::QUEUE_NAVIGATION
    );

    send_as(
        &mut observer,
        "sender-0",
        ns::MEDIA,
        &transport,
        &serde_json::json!({
            "type": "QUEUE_UPDATE",
            "requestId": 79,
            "mediaSessionId": media_id,
            "jump": 2,
        })
        .to_string(),
    )
    .await;
    let invalid = next_json_for_request(&mut observer, 79).await;
    assert_eq!(invalid["type"], "INVALID_REQUEST");
    assert!(harness.output_controls.lock().unwrap().is_empty());

    for (request_id, jump, expected) in [
        (80, 1, OutputControl::Next),
        (81, -1, OutputControl::Previous),
    ] {
        send_as(
            &mut observer,
            "sender-0",
            ns::MEDIA,
            &transport,
            &serde_json::json!({
                "type": "QUEUE_UPDATE",
                "requestId": request_id,
                "mediaSessionId": media_id,
                "jump": jump,
            })
            .to_string(),
        )
        .await;
        let response = next_json_for_request(&mut observer, request_id).await;
        assert_eq!(response["type"], "MEDIA_STATUS");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if harness.output_controls.lock().unwrap().last() == Some(&expected) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queue navigation did not reach the active app");
    }

    for (request_id, volume, expected_level, expected_muted) in [
        (
            82,
            serde_json::json!({"level": 0.35, "muted": false}),
            0.35,
            false,
        ),
        (83, serde_json::json!({"muted": true}), 0.35, true),
    ] {
        send_as(
            &mut observer,
            "sender-0",
            ns::RECEIVER,
            "receiver-0",
            &serde_json::json!({
                "type": "SET_VOLUME",
                "requestId": request_id,
                "volume": volume,
            })
            .to_string(),
        )
        .await;
        let response = next_json_for_request(&mut observer, request_id).await;
        assert_eq!(response["status"]["volume"]["level"], expected_level);
        assert_eq!(response["status"]["volume"]["muted"], expected_muted);
    }

    for (request_id, command, expected_state) in [
        (84, "PAUSE", "PAUSED"),
        (85, "PLAY", "PLAYING"),
        (86, "STOP", "IDLE"),
    ] {
        send_as(
            &mut observer,
            "sender-0",
            ns::MEDIA,
            &transport,
            &serde_json::json!({
                "type": command,
                "requestId": request_id,
                "mediaSessionId": media_id,
            })
            .to_string(),
        )
        .await;
        let response = next_json_for_request(&mut observer, request_id).await;
        assert_eq!(response["status"][0]["playerState"], expected_state);
    }

    // Status/control integration is not an ownership handshake: the original
    // sender remains attached and can still use its app namespace.
    assert_eq!(
        harness.sender_attachments.lock().unwrap().as_slice(),
        &[SenderAttachment::Initial]
    );
    send(
        &mut harness.client,
        FAKE_NS,
        &transport,
        r#"{"type":"PING"}"#,
    )
    .await;
    assert_eq!(
        next_json_of_type(&mut harness.client, "PONG").await["echo"],
        "PING"
    );

    let commands = harness.player.commands();
    assert!(commands.iter().any(|command| matches!(command,
        PlayerCommand::Volume { level, muted, .. } if *level == 0.35 && *muted)));
    assert!(commands
        .iter()
        .any(|command| matches!(command, PlayerCommand::Pause { .. })));
    assert!(commands
        .iter()
        .any(|command| matches!(command, PlayerCommand::Play { .. })));
    assert!(commands
        .iter()
        .any(|command| matches!(command, PlayerCommand::Stop { .. })));
}
