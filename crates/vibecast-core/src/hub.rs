//! The device hub: a single-task actor owning the transport registry,
//! subscriptions, receiver-0 platform state, and all app sessions.
//!
//! The hub task is a fast state machine: it owns all routing state and mutates
//! it without locks. Anything that can block — `resolve_media` and every app
//! callback (`on_message`, `on_sender_connected`, `on_playback_update`,
//! `on_stop`) — runs off the mailbox. `resolve_media` is spawned and its result
//! fed back internally; the other callbacks run on a per-session ordered task,
//! so a slow app never stalls Cast routing.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use vibecast_cast::{namespace as ns, ConnectionHandle, ServerEvent};
use vibecast_messages::{
    extract_request_id, ApplicationStatus, CastNamespace, ConnectionMessage, DeviceInfoResponse,
    GetDeviceInfoRequest, IdleReason, InvalidRequestResponse, LaunchErrorResponse, LaunchRequest,
    LoadFailedResponse, LoadRequest, MediaInvalidRequestResponse, MediaRequest,
    MultizoneGetStatusRequest, MultizoneStatusResponse, PlayerState, QueueItemIdsResponse,
    ReceiverRequest, ReceiverStatus, ReceiverStatusResponse, SetupRequest, SetupResponse, Volume,
};
use vibecast_player_api::{Player, PlayerCommand, PlayerControlRequest, PlayerReport};
use vibecast_proto::CastMessage;
use vibecast_sdk::{
    AppContext, AppSession, LaunchCredentials, MediaResolveError, MessageDisposition,
    NoopSenderChannel, OutputControl, PlaybackController, PlaybackMedia, PlaybackState,
    PlayerCapabilities, ReceiverContext, SenderChannel,
};
use vibecast_settings::PlayerSettings;

use crate::coordinator::{loading_media_info, media_info, Coordinator};
use crate::identity::DeviceIdentity;
use crate::proxy::{collect_routes, rewrite_streams, to_payload, SessionProxy};
use crate::registry::AppRegistry;
use vibecast_player_api::ProxyRegistrar;

const RECEIVER_0: &str = "receiver-0";
const SENDER_RECONNECT_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// A per-callback [`SenderChannel`] that writes custom-namespace app messages
/// directly to the relevant connection(s), never through the hub mailbox (which
/// is what awaits the app callback).
struct HubSender {
    transport_id: String,
    bound: Option<(ConnectionHandle, String)>,
    subscribers: Vec<ConnectionHandle>,
}

#[async_trait]
impl SenderChannel for HubSender {
    async fn send_custom(&self, namespace: &str, data: Value) {
        match &self.bound {
            Some((handle, sender_id)) => {
                let delivered = handle
                    .send_json(&self.transport_id, sender_id, namespace, &data)
                    .await
                    .is_ok();
                tracing::debug!(
                    namespace,
                    delivered,
                    "custom app response sent to requesting sender"
                );
            }
            None => self.broadcast_custom(namespace, data).await,
        }
    }

    async fn broadcast_custom(&self, namespace: &str, data: Value) {
        for handle in &self.subscribers {
            let _ = handle
                .send_json(&self.transport_id, "*", namespace, &data)
                .await;
        }
    }
}

/// Routes app-driven playback back through the hub's canonical media state
/// machine instead of letting an app talk directly to a concrete player.
struct HubPlaybackController {
    tx: mpsc::Sender<HubEvent>,
    session_id: String,
}

#[async_trait]
impl PlaybackController for HubPlaybackController {
    async fn load(&self, media: PlaybackMedia) {
        let _ = self
            .tx
            .send(HubEvent::AppPlayback(AppPlaybackEvent {
                session_id: self.session_id.clone(),
                command: AppPlaybackCommand::Load(media),
            }))
            .await;
    }

    async fn play(&self) {
        self.send(AppPlaybackCommand::Play).await;
    }

    async fn pause(&self) {
        self.send(AppPlaybackCommand::Pause).await;
    }

    async fn seek(&self, position: f64) {
        self.send(AppPlaybackCommand::Seek(position)).await;
    }

    async fn stop(&self) {
        self.send(AppPlaybackCommand::Stop).await;
    }
}

impl HubPlaybackController {
    async fn send(&self, command: AppPlaybackCommand) {
        let _ = self
            .tx
            .send(HubEvent::AppPlayback(AppPlaybackEvent {
                session_id: self.session_id.clone(),
                command,
            }))
            .await;
    }
}

/// An event driving the hub actor. Internal to the crate: external callers use
/// [`DeviceHubHandle`], so the internal `MediaResolved` feedback variant is
/// never part of the public API.
enum HubEvent {
    /// A transport event from the Cast TLS server.
    Server(ServerEvent),
    /// A player report from the player bridge.
    Report(PlayerReport),
    /// The result of an app's `resolve_media` (internal feedback).
    MediaResolved(MediaResolved),
    /// Playback initiated by an app-specific control channel.
    AppPlayback(AppPlaybackEvent),
    /// Stop all app sessions cleanly, then acknowledge (graceful shutdown).
    Shutdown(tokio::sync::oneshot::Sender<()>),
}

struct AppPlaybackEvent {
    session_id: String,
    command: AppPlaybackCommand,
}

enum AppPlaybackCommand {
    Load(PlaybackMedia),
    Play,
    Pause,
    Seek(f64),
    Stop,
}

/// The (spawned) result of resolving media for one LOAD request.
struct MediaResolved {
    session_id: String,
    request_id: i64,
    connection_id: u64,
    sender_id: String,
    result: Result<PlaybackMedia, MediaResolveError>,
}

/// The device hub has shut down and no longer accepts events.
#[derive(Debug, thiserror::Error)]
#[error("device hub is closed")]
pub struct HubClosed;

/// A cheap, cloneable handle for feeding the [`DeviceHub`] typed events.
///
/// This is the only way external code drives the hub, so internal scheduling
/// details (media-resolution feedback, per-session jobs) stay private.
#[derive(Clone)]
pub struct DeviceHubHandle {
    tx: mpsc::Sender<HubEvent>,
}

impl DeviceHubHandle {
    /// Deliver a Cast transport event (connect, message, disconnect).
    ///
    /// # Errors
    /// Returns [`HubClosed`] if the hub has stopped.
    pub async fn send_server_event(&self, event: ServerEvent) -> Result<(), HubClosed> {
        self.tx
            .send(HubEvent::Server(event))
            .await
            .map_err(|_| HubClosed)
    }

    /// Deliver a player player report.
    ///
    /// # Errors
    /// Returns [`HubClosed`] if the hub has stopped.
    pub async fn send_player_report(&self, report: PlayerReport) -> Result<(), HubClosed> {
        self.tx
            .send(HubEvent::Report(report))
            .await
            .map_err(|_| HubClosed)
    }

    /// Stop all app sessions cleanly and wait for the hub to acknowledge.
    ///
    /// Returns once teardown completes or the hub has already stopped.
    pub async fn shutdown(&self) {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        if self.tx.send(HubEvent::Shutdown(ack_tx)).await.is_ok() {
            let _ = ack_rx.await;
        }
    }
}

/// A unit of app-callback work run on a session's dedicated, ordered task so a
/// slow app (HTTP auth, token exchange, ...) never blocks the hub mailbox. Jobs
/// for one session run in the order the hub enqueues them.
enum AppJob {
    VolumeUpdate {
        ctx: AppContext,
        level: f64,
        muted: bool,
    },
    /// A sender connected to the app transport.
    SenderConnected { ctx: AppContext, sender_id: String },
    /// A custom-namespace message arrived.
    Message {
        ctx: AppContext,
        namespace: String,
        data: Value,
    },
    /// Canonical playback state changed.
    PlaybackUpdate {
        ctx: AppContext,
        state: PlaybackState,
    },
    /// Queue navigation requested at the physical output.
    OutputControl {
        ctx: AppContext,
        control: OutputControl,
    },
    /// The session is being torn down (final job).
    Stop { ctx: AppContext },
}

/// Drive one app session's callbacks in order, off the hub mailbox.
async fn run_app_session(app: Arc<dyn AppSession>, mut jobs: mpsc::Receiver<AppJob>) {
    while let Some(job) = jobs.recv().await {
        match job {
            AppJob::VolumeUpdate { ctx, level, muted } => {
                app.on_volume_update(&ctx, level, muted).await
            }
            AppJob::SenderConnected { ctx, sender_id } => {
                app.on_sender_connected(&ctx, &sender_id).await;
            }
            AppJob::Message {
                ctx,
                namespace,
                data,
            } => {
                if app.on_message(&ctx, &namespace, &data).await == MessageDisposition::Unhandled {
                    tracing::debug!(namespace = %namespace, "app left message unhandled");
                }
            }
            AppJob::PlaybackUpdate { ctx, state } => app.on_playback_update(&ctx, state).await,
            AppJob::OutputControl { ctx, control } => app.on_output_control(&ctx, control).await,
            AppJob::Stop { ctx } => {
                app.on_stop(&ctx).await;
                break;
            }
        }
    }
}

/// A running app session registered as a Cast transport (id == transport id).
struct Session {
    audio_cache: Option<Arc<crate::audio_cache::AudioCache>>,
    app_id: String,
    app_key: String,
    display_name: String,
    icon_url: Option<String>,
    status_text: String,
    namespaces: Vec<String>,
    app: Arc<dyn AppSession>,
    ctx: AppContext,
    coordinator: Coordinator,
    /// Ordered mailbox for this session's app callbacks (run off the hub task).
    jobs: mpsc::Sender<AppJob>,
}

/// Construction parameters for the hub.
pub struct HubConfig {
    /// Device identity.
    pub identity: DeviceIdentity,
    /// App registry.
    pub registry: AppRegistry,
    /// Player commands sink (the player bridge).
    pub player: Arc<dyn Player>,
    /// Session proxy registration (the player bridge).
    pub proxy: Arc<dyn ProxyRegistrar>,
    /// Shared HTTP client for apps and the license/manifest proxy.
    pub http: reqwest::Client,
    /// Base data directory.
    pub data_dir: PathBuf,
    /// Initial receiver volume.
    pub volume: Volume,
    /// User-Agent placed in each app session's `ReceiverContext`.
    pub user_agent: String,
    /// `CAST-DEVICE-CAPABILITIES` header value for app sessions.
    pub cast_device_capabilities: String,
    /// Capabilities of the player bound to this receiver.
    pub capabilities: PlayerCapabilities,
    /// Live settings scoped to the player bound to this receiver.
    pub player_settings: PlayerSettings,
}

/// The device hub actor.
pub struct DeviceHub {
    identity: DeviceIdentity,
    registry: AppRegistry,
    player: Arc<dyn Player>,
    proxy: Arc<dyn ProxyRegistrar>,
    http: reqwest::Client,
    data_dir: PathBuf,
    volume: Volume,
    user_agent: String,
    cast_device_capabilities: String,
    capabilities: PlayerCapabilities,
    player_settings: PlayerSettings,
    connections: HashMap<u64, ConnectionHandle>,
    /// `(connection, sender)` -> transport id.
    subscriptions: HashMap<(u64, String), String>,
    // Platform and app channels coexist for the same Cast sender ID.
    platform_subscriptions: HashSet<(u64, String)>,
    /// session id (== transport id) -> session.
    sessions: HashMap<String, Session>,
    session_owners: HashMap<String, u64>,
    sender_disconnect_deadlines: HashMap<String, tokio::time::Instant>,
    self_tx: mpsc::Sender<HubEvent>,
    events: Option<mpsc::Receiver<HubEvent>>,
}

fn volume_path(base: &std::path::Path, device_id: &str) -> PathBuf {
    let key: String = device_id.bytes().map(|b| format!("{b:02x}")).collect();
    base.join(format!("volume-{key}.json"))
}

fn prepare_volume_for_session(volume: &mut Volume) {
    if volume.level == 0.0 {
        volume.level = 0.1;
        volume.muted = false;
    }
}

impl DeviceHub {
    /// Build a hub. Feed it with [`handle`](Self::handle) and drive it with
    /// [`run`](Self::run).
    #[must_use]
    pub fn new(config: HubConfig) -> Self {
        let (tx, rx) = mpsc::channel(128);
        let volume = std::fs::read(volume_path(&config.data_dir, &config.identity.device_id))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Volume>(&bytes).ok())
            .filter(|v| v.level.is_finite() && (0.0..=1.0).contains(&v.level))
            .unwrap_or(config.volume);
        Self {
            identity: config.identity,
            registry: config.registry,
            player: config.player,
            proxy: config.proxy,
            http: config.http,
            data_dir: config.data_dir,
            volume,
            user_agent: config.user_agent,
            cast_device_capabilities: config.cast_device_capabilities,
            capabilities: config.capabilities,
            player_settings: config.player_settings,
            connections: HashMap::new(),
            subscriptions: HashMap::new(),
            platform_subscriptions: HashSet::new(),
            sessions: HashMap::new(),
            session_owners: HashMap::new(),
            sender_disconnect_deadlines: HashMap::new(),
            self_tx: tx,
            events: Some(rx),
        }
    }

    /// A handle for feeding the hub Cast transport events and player reports.
    #[must_use]
    pub fn handle(&self) -> DeviceHubHandle {
        DeviceHubHandle {
            tx: self.self_tx.clone(),
        }
    }

    /// Run the hub until it is shut down or the event channel closes.
    ///
    /// A [`DeviceHubHandle::shutdown`] tears down every session and then stops
    /// the loop, so the task completes and can be awaited during shutdown.
    pub async fn run(mut self) {
        let mut events = self.events.take().expect("run called once");
        loop {
            let deadline = self.sender_disconnect_deadlines.values().copied().min();
            tokio::select! {
                // A queued status request/late CONNECT must not starve an
                // already expired deadline. No detached timers can outlive us.
                biased;
                _ = async {
                    if let Some(deadline) = deadline {
                        tokio::time::sleep_until(deadline).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    let now = tokio::time::Instant::now();
                    let expired: Vec<_> = self.sender_disconnect_deadlines.iter()
                        .filter(|(_, deadline)| **deadline <= now)
                        .map(|(session, _)| session.clone()).collect();
                    for session_id in expired {
                        tracing::info!(%session_id, "sender reconnect grace expired; stopping app");
                        self.stop_session(&session_id).await;
                    }
                    let response = self.receiver_status(0);
                    self.broadcast(RECEIVER_0, ns::RECEIVER, &response).await;
                }
                event = events.recv() => {
                    let Some(event) = event else { break; };
                    let stop = matches!(event, HubEvent::Shutdown(_));
                    self.dispatch(event).await;
                    if stop { break; }
                }
            }
        }
    }

    async fn dispatch(&mut self, event: HubEvent) {
        match event {
            HubEvent::Server(ServerEvent::Connected(handle)) => {
                self.connections.insert(handle.id(), handle);
            }
            HubEvent::Server(ServerEvent::Disconnected { id, .. }) => {
                let owned: HashSet<String> = self
                    .session_owners
                    .iter()
                    .filter(|(_, owner)| **owner == id)
                    .map(|(session, _)| session.clone())
                    .collect();
                let mut affected_sessions: HashSet<String> = self
                    .subscriptions
                    .iter()
                    .filter(|((conn, _), _)| *conn == id)
                    .map(|(_, transport)| transport.clone())
                    .collect();
                affected_sessions.extend(owned.iter().cloned());
                self.session_owners.retain(|_, owner| *owner != id);
                self.subscriptions.retain(|(conn, _), _| *conn != id);
                self.platform_subscriptions.retain(|(conn, _)| *conn != id);
                self.connections.remove(&id);
                for session_id in affected_sessions {
                    let still_subscribed = self
                        .subscriptions
                        .values()
                        .any(|transport| transport == &session_id);
                    if !owned.contains(&session_id) && still_subscribed {
                        continue;
                    }
                    if self
                        .sessions
                        .get(&session_id)
                        .is_some_and(|s| s.app.allows_sender_reconnect_grace())
                    {
                        // Additional disappearing sockets must not extend an
                        // existing deadline. Platform discovery is not control.
                        self.sender_disconnect_deadlines
                            .entry(session_id.clone())
                            .or_insert_with(|| {
                                tokio::time::Instant::now() + SENDER_RECONNECT_GRACE
                            });
                        tracing::info!(%session_id, grace_seconds = 10, "waiting for sender reconnect");
                        continue;
                    }
                    tracing::info!(%session_id, "stopping app after sender transport disconnect");
                    self.stop_session(&session_id).await;
                }
                let response = self.receiver_status(0);
                self.broadcast(RECEIVER_0, ns::RECEIVER, &response).await;
            }
            HubEvent::Server(ServerEvent::Message { handle, message }) => {
                self.on_message(&handle, message).await;
            }
            HubEvent::Report(report) => self.on_report(report).await,
            HubEvent::MediaResolved(resolved) => self.on_media_resolved(resolved).await,
            HubEvent::AppPlayback(event) => self.on_app_playback(event).await,
            HubEvent::Shutdown(ack) => {
                for session_id in self.sessions.keys().cloned().collect::<Vec<_>>() {
                    self.stop_session(&session_id).await;
                }
                let _ = ack.send(());
            }
        }
    }

    async fn on_message(&mut self, handle: &ConnectionHandle, message: CastMessage) {
        let destination = message.destination_id.clone();
        if destination == RECEIVER_0 {
            self.handle_platform(handle, message).await;
        } else if self.sessions.contains_key(&destination) {
            self.handle_session_message(handle, message).await;
        } else {
            tracing::debug!(dest = %destination, "message for unknown transport");
        }
    }

    // -- receiver-0 platform ------------------------------------------------

    async fn handle_platform(&mut self, handle: &ConnectionHandle, message: CastMessage) {
        let Some(payload) = parse_payload(&message) else {
            return;
        };
        let conn_id = handle.id();
        let source = message.source_id.clone();
        match message.namespace.as_str() {
            ns::CONNECTION => match serde_json::from_value::<ConnectionMessage>(payload) {
                Ok(ConnectionMessage::Connect(_)) => {
                    self.platform_subscriptions.insert((conn_id, source));
                }
                _ => {
                    self.platform_subscriptions.remove(&(conn_id, source));
                }
            },
            ns::RECEIVER => self.handle_receiver(conn_id, &source, payload).await,
            ns::DISCOVERY => {
                if let Ok(request) = serde_json::from_value::<GetDeviceInfoRequest>(payload) {
                    let response = DeviceInfoResponse::new(
                        request.request_id,
                        self.identity.device_id.clone(),
                        self.identity.device_model.clone(),
                        self.identity.friendly_name.clone(),
                    );
                    self.send_to(conn_id, RECEIVER_0, &source, ns::DISCOVERY, &response)
                        .await;
                }
            }
            ns::MULTIZONE => {
                if let Ok(request) = serde_json::from_value::<MultizoneGetStatusRequest>(payload) {
                    let response = MultizoneStatusResponse::empty(request.request_id);
                    self.send_to(conn_id, RECEIVER_0, &source, ns::MULTIZONE, &response)
                        .await;
                }
            }
            ns::SETUP => {
                if let Ok(request) = serde_json::from_value::<SetupRequest>(payload) {
                    let response = SetupResponse::ok(
                        request.request_id,
                        self.identity.friendly_name.clone(),
                        self.identity.ssdp_udn.clone(),
                    );
                    self.send_to(conn_id, RECEIVER_0, &source, ns::SETUP, &response)
                        .await;
                }
            }
            other => tracing::warn!(namespace = %other, "unhandled platform namespace"),
        }
    }

    async fn handle_receiver(&mut self, conn_id: u64, source: &str, payload: serde_json::Value) {
        let request = match serde_json::from_value::<ReceiverRequest>(payload.clone()) {
            Ok(request) => request,
            Err(_) => {
                let response = InvalidRequestResponse::new(
                    extract_request_id(&payload),
                    "Invalid receiver request",
                );
                self.send_to(conn_id, RECEIVER_0, source, ns::RECEIVER, &response)
                    .await;
                return;
            }
        };

        match request {
            ReceiverRequest::GetStatus(r) => {
                let response = self.receiver_status(r.request_id);
                self.send_to(conn_id, RECEIVER_0, source, ns::RECEIVER, &response)
                    .await;
            }
            ReceiverRequest::GetAppAvailability(r) => {
                let response =
                    vibecast_messages::AppAvailabilityResponse::available(r.request_id, &r.app_id);
                self.send_to(conn_id, RECEIVER_0, source, ns::RECEIVER, &response)
                    .await;
            }
            ReceiverRequest::Launch(r) => self.handle_launch(conn_id, source, r).await,
            ReceiverRequest::Stop(r) => {
                tracing::info!(session_id = %r.session_id, "explicit receiver STOP");
                self.stop_session(&r.session_id).await;
                self.reply_and_publish_receiver_status(conn_id, source, r.request_id)
                    .await;
            }
            ReceiverRequest::SetVolume(r) => {
                self.volume.apply_update(&r.volume);
                for (id, session) in &mut self.sessions {
                    session.coordinator.volume = self.volume.clone();
                    self.player
                        .send(PlayerCommand::Volume {
                            session_id: id.clone(),
                            level: self.volume.level,
                            muted: self.volume.muted,
                        })
                        .await;
                }
                self.publish_volume().await;
                self.reply_and_publish_receiver_status(conn_id, source, r.request_id)
                    .await;
            }
        }
    }

    async fn handle_launch(&mut self, conn_id: u64, source: &str, request: LaunchRequest) {
        prepare_volume_for_session(&mut self.volume);
        tracing::info!(
            app_id = %request.app_id,
            request_id = request.request_id,
            "LAUNCH request"
        );
        let Some(registered) = self.registry.get(&request.app_id) else {
            tracing::warn!(app_id = %request.app_id, "LAUNCH_ERROR: app not registered");
            let response =
                LaunchErrorResponse::new(request.request_id, "Application not available");
            self.send_to(conn_id, RECEIVER_0, source, ns::RECEIVER, &response)
                .await;
            return;
        };
        let manifest = &registered.manifest;
        let settings = match self.player_settings.reader(manifest.app_key).await {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(
                    %error,
                    app_id = %request.app_id,
                    app_key = %manifest.app_key,
                    "LAUNCH_ERROR: app settings unavailable"
                );
                let response =
                    LaunchErrorResponse::new(request.request_id, "Application launch failed");
                self.send_to(conn_id, RECEIVER_0, source, ns::RECEIVER, &response)
                    .await;
                return;
            }
        };

        // LAUNCH replaces the current app: stop existing sessions first.
        for session_id in self.sessions.keys().cloned().collect::<Vec<_>>() {
            self.stop_session(&session_id).await;
        }

        let session_id = Uuid::new_v4().to_string();
        self.session_owners.insert(session_id.clone(), conn_id);
        let (credentials, credentials_type) = request.resolved_credentials();
        let app_key = manifest.app_key;
        let data_dir = self.data_dir.join("apps").join(app_key);
        let _ = std::fs::create_dir_all(&data_dir);

        // The stored session context carries only data + a no-op sender; the
        // hub builds a fresh, sender-bound context per callback that can send.
        let ctx = AppContext::new(
            session_id.clone(),
            session_id.clone(),
            request.app_id.clone(),
            self.http.clone(),
            ReceiverContext {
                friendly_name: self.identity.friendly_name.clone(),
                device_model: self.identity.device_model.clone(),
                device_id: self.identity.device_id.clone(),
                data_dir,
                user_agent: self.user_agent.clone(),
                cast_device_capabilities: self.cast_device_capabilities.clone(),
                capabilities: self.capabilities.clone(),
            },
            Arc::new(NoopSenderChannel),
        )
        .with_settings(settings)
        .with_playback_controller(Arc::new(HubPlaybackController {
            tx: self.self_tx.clone(),
            session_id: session_id.clone(),
        }));

        let app: Arc<dyn AppSession> = match registered
            .provider
            .launch(
                &ctx,
                LaunchCredentials {
                    credentials,
                    credentials_type,
                },
            )
            .await
        {
            Ok(session) => {
                tracing::info!(
                    app_id = %request.app_id,
                    app_key = %app_key,
                    session_id = %session_id,
                    "app launched"
                );
                session
            }
            Err(error) => {
                tracing::warn!(%error, app_id = %request.app_id, "app launch failed");
                let response =
                    LaunchErrorResponse::new(request.request_id, "Application launch failed");
                self.send_to(conn_id, RECEIVER_0, source, ns::RECEIVER, &response)
                    .await;
                return;
            }
        };

        app.on_volume_update(&ctx, self.volume.level, self.volume.muted)
            .await;
        self.player
            .send(PlayerCommand::Volume {
                session_id: session_id.clone(),
                level: self.volume.level,
                muted: self.volume.muted,
            })
            .await;

        let mut namespaces: Vec<String> = manifest
            .namespaces
            .iter()
            .filter(|name| **name != ns::MEDIA)
            .map(|name| (*name).to_string())
            .collect();
        namespaces.sort();
        namespaces.push(ns::MEDIA.to_string());

        // Per-session callback task: app callbacks run here, in order, so a slow
        // app never blocks the hub mailbox.
        let (jobs_tx, jobs_rx) = mpsc::channel(32);
        tokio::spawn(run_app_session(app.clone(), jobs_rx));

        let session = Session {
            audio_cache: None,
            app_id: request.app_id.clone(),
            app_key: app_key.to_string(),
            display_name: manifest.display_name.to_string(),
            icon_url: manifest.icon_url.map(str::to_string),
            status_text: manifest.display_name.to_string(),
            namespaces,
            app,
            ctx,
            coordinator: Coordinator::new(self.volume.clone()),
            jobs: jobs_tx,
        };
        self.sessions.insert(session_id, session);
        self.publish_volume().await;

        self.reply_and_publish_receiver_status(conn_id, source, request.request_id)
            .await;
    }

    async fn stop_session(&mut self, session_id: &str) {
        self.sender_disconnect_deadlines.remove(session_id);
        self.session_owners.remove(session_id);
        let Some(mut session) = self.sessions.remove(session_id) else {
            return;
        };
        if let Some(cache) = session.audio_cache.take() {
            cache.cancel();
        }
        tracing::info!(session_id = %session_id, app_key = %session.app_key, "stopping session");
        // Observers (e.g. Home Assistant) retain their last MEDIA_STATUS even
        // after the application disappears. Publish the terminal state while
        // its subscriptions still exist; later player reports are too late.
        session.coordinator.set_idle(Some(IdleReason::Cancelled));
        let terminal = session.coordinator.status_response(0);
        self.broadcast(session_id, ns::MEDIA, &terminal).await;
        if session.coordinator.playback_media.is_some() {
            self.player
                .send(PlayerCommand::Stop {
                    session_id: session_id.to_string(),
                })
                .await;
        }
        self.unregister_proxies(session_id);
        // Build the teardown context while this session's subscribers are still
        // registered, so on_stop can broadcast a final message. Enqueue it as the
        // session's final job; dropping `session` then closes its task once the
        // job has drained.
        let ctx = self.callback_context(&session, None);
        let _ = session.jobs.send(AppJob::Stop { ctx }).await;
        self.subscriptions
            .retain(|_, transport| transport != session_id);
    }

    fn receiver_status(&self, request_id: i64) -> ReceiverStatusResponse {
        let applications: Vec<ApplicationStatus> = self
            .sessions
            .iter()
            .map(|(session_id, session)| {
                let sender_connected = self
                    .subscriptions
                    .values()
                    .any(|transport| transport == session_id);
                ApplicationStatus {
                    app_id: session.app_id.clone(),
                    display_name: session.display_name.clone(),
                    session_id: session_id.clone(),
                    transport_id: session_id.clone(),
                    status_text: session.status_text.clone(),
                    namespaces: session
                        .namespaces
                        .iter()
                        .map(|name| CastNamespace { name: name.clone() })
                        .collect(),
                    is_idle_screen: false,
                    app_type: Some("WEB".to_string()),
                    icon_url: session.icon_url.clone(),
                    launched_from_cloud: Some(false),
                    sender_connected: Some(sender_connected),
                    universal_app_id: Some(session.app_id.clone()),
                }
            })
            .collect();
        ReceiverStatusResponse::new(
            request_id,
            ReceiverStatus {
                applications,
                volume: self.volume.clone(),
                is_active_input: Some(true),
                is_stand_by: Some(false),
            },
        )
    }

    async fn reply_and_publish_receiver_status(&self, conn_id: u64, source: &str, request_id: i64) {
        // Command responses must target the sender that owns the request.
        // Chromium's launch state machine does not treat a wildcard status as
        // the correlated LAUNCH response, even though mobile senders commonly
        // accept it. Other platform observers still need an unsolicited status
        // update, but it must not leave a duplicate response queued for the
        // requesting connection.
        let response = self.receiver_status(request_id);
        self.send_to(conn_id, RECEIVER_0, source, ns::RECEIVER, &response)
            .await;
        let update = self.receiver_status(0);
        self.broadcast_except_connection(RECEIVER_0, ns::RECEIVER, &update, Some(conn_id))
            .await;
    }

    // -- app session transports --------------------------------------------

    async fn handle_session_message(&mut self, handle: &ConnectionHandle, message: CastMessage) {
        let Some(payload) = parse_payload(&message) else {
            return;
        };
        let conn_id = handle.id();
        let transport = message.destination_id.clone();
        let source = message.source_id.clone();

        match message.namespace.as_str() {
            ns::CONNECTION => match serde_json::from_value::<ConnectionMessage>(payload) {
                Ok(ConnectionMessage::Connect(_)) => {
                    self.subscriptions
                        .insert((conn_id, source.clone()), transport.clone());
                    if self
                        .sender_disconnect_deadlines
                        .remove(&transport)
                        .is_some()
                    {
                        self.session_owners.insert(transport.clone(), conn_id);
                        tracing::info!(session_id = %transport, "sender reattached within reconnect grace");
                    }
                    let (ctx, response, jobs) = match self.sessions.get(&transport) {
                        Some(session) => (
                            self.callback_context(session, Some((conn_id, source.clone()))),
                            session.coordinator.status_response(0),
                            session.jobs.clone(),
                        ),
                        None => return,
                    };
                    self.send_to(conn_id, &transport, &source, ns::MEDIA, &response)
                        .await;
                    let _ = jobs
                        .send(AppJob::SenderConnected {
                            ctx,
                            sender_id: source,
                        })
                        .await;
                }
                Ok(ConnectionMessage::Close(_)) => {
                    self.subscriptions.remove(&(conn_id, source));
                    let still_subscribed = self
                        .subscriptions
                        .values()
                        .any(|target| target == &transport);
                    let grace_owner_left = self.session_owners.get(&transport) == Some(&conn_id)
                        && self
                            .sessions
                            .get(&transport)
                            .is_some_and(|s| s.app.allows_sender_reconnect_grace());
                    if !still_subscribed || grace_owner_left {
                        tracing::info!(session_id = %transport, "explicit owner or last-sender app CLOSE");
                        self.stop_session(&transport).await;
                    }
                    // CLOSE only leaves the app channel. The platform socket
                    // can remain open, so no Disconnected event will refresh it.
                    let response = self.receiver_status(0);
                    self.broadcast(RECEIVER_0, ns::RECEIVER, &response).await;
                }
                Err(_) => {
                    self.subscriptions.remove(&(conn_id, source));
                }
            },
            ns::MEDIA => {
                self.handle_media(conn_id, &transport, &source, payload)
                    .await
            }
            other => {
                let (ctx, jobs) = match self.sessions.get(&transport) {
                    Some(session) => (
                        self.callback_context(session, Some((conn_id, source.clone()))),
                        session.jobs.clone(),
                    ),
                    None => return,
                };
                let _ = jobs
                    .send(AppJob::Message {
                        ctx,
                        namespace: other.to_string(),
                        data: payload,
                    })
                    .await;
            }
        }
    }

    async fn handle_media(
        &mut self,
        conn_id: u64,
        transport: &str,
        source: &str,
        payload: serde_json::Value,
    ) {
        let request = match serde_json::from_value::<MediaRequest>(payload.clone()) {
            Ok(request) => request,
            Err(_) => {
                let response = MediaInvalidRequestResponse::new(
                    extract_request_id(&payload),
                    "Invalid media request",
                );
                self.send_to(conn_id, transport, source, ns::MEDIA, &response)
                    .await;
                return;
            }
        };

        match request {
            MediaRequest::Load(load) => {
                let metadata = load.media.metadata.as_ref();
                tracing::info!(
                    session_id = %transport,
                    stream_type = ?load.media.stream_type,
                    is_live = ?load.media.is_live_media,
                    has_duration = load.media.duration.is_some(),
                    has_title = metadata.is_some_and(|m| m.title.is_some()),
                    has_subtitle = metadata.is_some_and(|m| m.subtitle.is_some()),
                    image_count = metadata.map_or(0, |m| m.images.len()),
                    "media LOAD field summary"
                );
                self.media_load(conn_id, transport, source, load).await;
            }
            MediaRequest::Play(r) => {
                tracing::debug!(session_id = %transport, request_id = r.request_id, "PLAY");
                self.request_play(transport, r.request_id).await;
            }
            MediaRequest::Pause(r) => {
                tracing::debug!(session_id = %transport, request_id = r.request_id, "PAUSE");
                self.pause(transport, r.request_id).await;
            }
            MediaRequest::Seek(r) => {
                let position = r.current_time;
                let live = self.sessions.get(transport).is_some_and(|session| {
                    session
                        .coordinator
                        .current_media
                        .as_ref()
                        .is_some_and(|media| {
                            media.stream_type == vibecast_messages::StreamType::Live
                        })
                });
                if live || !position.is_finite() || position < 0.0 {
                    let response =
                        MediaInvalidRequestResponse::new(r.request_id, "Seek position unavailable");
                    self.send_to(conn_id, transport, source, ns::MEDIA, &response)
                        .await;
                    return;
                }
                tracing::debug!(session_id = %transport, request_id = r.request_id, position, "SEEK");
                self.seek(transport, r.request_id, position).await;
            }
            MediaRequest::Stop(r) => {
                tracing::debug!(session_id = %transport, request_id = r.request_id, "STOP");
                self.stop_playback(transport, r.request_id).await;
            }
            MediaRequest::SetVolume(r) => {
                let (level, muted) = match self.sessions.get_mut(transport) {
                    Some(session) => {
                        session.coordinator.volume.apply_update(&r.volume);
                        (
                            session.coordinator.volume.level,
                            session.coordinator.volume.muted,
                        )
                    }
                    None => return,
                };
                let response = match self.sessions.get(transport) {
                    Some(session) => session.coordinator.status_response(r.request_id),
                    None => return,
                };
                self.broadcast(transport, ns::MEDIA, &response).await;
                self.player
                    .send(PlayerCommand::Volume {
                        session_id: transport.to_string(),
                        level,
                        muted,
                    })
                    .await;
                self.notify_app(transport).await;
                self.volume.level = level;
                self.volume.muted = muted;
                self.publish_volume().await;
                let receiver_status = self.receiver_status(0);
                self.broadcast(RECEIVER_0, ns::RECEIVER, &receiver_status)
                    .await;
            }
            MediaRequest::GetStatus(r) => {
                if let Some(session) = self.sessions.get(transport) {
                    let response = session.coordinator.status_response(r.request_id);
                    self.send_to(conn_id, transport, source, ns::MEDIA, &response)
                        .await;
                }
            }
            MediaRequest::QueueGetItemIds(r) => {
                let item_ids = match self.sessions.get(transport) {
                    Some(session)
                        if session.coordinator.current_media.is_some()
                            && session.coordinator.player_state != PlayerState::Idle =>
                    {
                        vec![session.coordinator.media_session_id]
                    }
                    _ => Vec::new(),
                };
                let response = QueueItemIdsResponse::new(r.request_id, item_ids);
                self.send_to(conn_id, transport, source, ns::MEDIA, &response)
                    .await;
            }
            MediaRequest::QueueLoad(r) => {
                if self
                    .sessions
                    .get(transport)
                    .is_some_and(|session| session.app_id == "CC1AD845")
                {
                    let response = MediaInvalidRequestResponse::new(
                        r.request_id,
                        "QUEUE_LOAD is not supported; use a single-item LOAD",
                    );
                    self.send_to(conn_id, transport, source, ns::MEDIA, &response)
                        .await;
                    return;
                }
                let response =
                    vibecast_messages::MediaStatusResponse::new(r.request_id, Vec::new());
                self.send_to(conn_id, transport, source, ns::MEDIA, &response)
                    .await;
            }
        }
    }

    /// Apply a coordinator mutation and broadcast the resulting status. The
    /// caller then drives the player and notifies the app.
    async fn transition(
        &mut self,
        transport: &str,
        request_id: i64,
        mutate: impl FnOnce(&mut Coordinator),
    ) {
        let response = match self.sessions.get_mut(transport) {
            Some(session) => {
                mutate(&mut session.coordinator);
                session.coordinator.status_response(request_id)
            }
            None => return,
        };
        self.broadcast(transport, ns::MEDIA, &response).await;
    }

    async fn request_play(&mut self, session_id: &str, request_id: i64) {
        if let Some(session) = self.sessions.get(session_id) {
            if session.app.handles_play_requests() {
                let jobs = session.jobs.clone();
                let ctx = session.ctx.clone();
                // Acknowledge the actual state, not optimistic PLAYING for an
                // empty decoder. The app will load fresh media when necessary.
                let response = session.coordinator.status_response(request_id);
                self.broadcast(session_id, ns::MEDIA, &response).await;
                let _ = jobs
                    .send(AppJob::OutputControl {
                        ctx,
                        control: OutputControl::Play,
                    })
                    .await;
                return;
            }
        }
        self.play(session_id, request_id).await;
    }

    async fn play(&mut self, session_id: &str, request_id: i64) {
        self.transition(session_id, request_id, |coordinator| {
            coordinator.player_state = PlayerState::Playing;
            coordinator.idle_reason = None;
        })
        .await;
        self.player
            .send(PlayerCommand::Play {
                session_id: session_id.to_string(),
            })
            .await;
        self.notify_app(session_id).await;
    }

    async fn pause(&mut self, session_id: &str, request_id: i64) {
        self.transition(session_id, request_id, |coordinator| {
            coordinator.player_state = PlayerState::Paused;
            coordinator.idle_reason = None;
        })
        .await;
        self.player
            .send(PlayerCommand::Pause {
                session_id: session_id.to_string(),
            })
            .await;
        self.notify_app(session_id).await;
    }

    async fn seek(&mut self, session_id: &str, request_id: i64, position: f64) {
        self.transition(session_id, request_id, |coordinator| {
            coordinator.current_time = position;
            coordinator.idle_reason = None;
        })
        .await;
        self.player
            .send(PlayerCommand::Seek {
                session_id: session_id.to_string(),
                position,
            })
            .await;
        self.notify_app(session_id).await;
    }

    async fn stop_playback(&mut self, session_id: &str, request_id: i64) {
        self.clear_audio_cache(session_id);
        self.transition(session_id, request_id, |coordinator| {
            coordinator.set_idle(Some(IdleReason::Cancelled));
        })
        .await;
        self.player
            .send(PlayerCommand::Stop {
                session_id: session_id.to_string(),
            })
            .await;
        if let Some(session) = self.sessions.get_mut(session_id) {
            session.coordinator.clear_media();
        }
        self.unregister_proxies(session_id);
        self.notify_app(session_id).await;
    }

    async fn media_load(
        &mut self,
        conn_id: u64,
        transport: &str,
        source: &str,
        load: Box<LoadRequest>,
    ) {
        tracing::info!(
            session_id = %transport,
            request_id = load.request_id,
            content_id = %load.media.content_id,
            stream_type = ?load.media.stream_type,
            "LOAD"
        );
        // Phase 1: broadcast IDLE + LOADING with the request's media info.
        let response = match self.sessions.get_mut(transport) {
            Some(session) => {
                let coordinator = &mut session.coordinator;
                coordinator.media_session_id += 1;
                let loading = loading_media_info(&load);
                coordinator.current_media = Some(loading.clone());
                coordinator.player_state = PlayerState::Idle;
                coordinator.idle_reason = None;
                coordinator.current_time = 0.0;
                coordinator.loading_response(load.request_id, &loading)
            }
            None => return,
        };
        self.broadcast(transport, ns::MEDIA, &response).await;

        // Phase 2: resolve media off the mailbox and feed the result back. The
        // context is broadcast-capable so apps can push custom messages while
        // resolving (e.g. TV4's legacy snapshot).
        let (app, ctx) = match self.sessions.get(transport) {
            Some(session) => (session.app.clone(), self.callback_context(session, None)),
            None => return,
        };
        let self_tx = self.self_tx.clone();
        let session_id = transport.to_string();
        let sender_id = source.to_string();
        let request_id = load.request_id;
        tokio::spawn(async move {
            let result = app.resolve_media(&ctx, &load).await;
            let _ = self_tx
                .send(HubEvent::MediaResolved(MediaResolved {
                    session_id,
                    request_id,
                    connection_id: conn_id,
                    sender_id,
                    result,
                }))
                .await;
        });
    }

    async fn on_media_resolved(&mut self, resolved: MediaResolved) {
        let MediaResolved {
            session_id,
            request_id,
            connection_id,
            sender_id,
            result,
        } = resolved;
        if !self.sessions.contains_key(&session_id) {
            tracing::debug!(session_id = %session_id, "media resolved for stopped session; dropping");
            return; // session stopped while resolving
        }

        let media = match result {
            Ok(media) => media,
            Err(failure) => {
                self.fail_load(&session_id, connection_id, &sender_id, request_id, &failure)
                    .await;
                return;
            }
        };
        if media.streams.is_empty() {
            let failure = MediaResolveError::internal("INVALID_APP_MEDIA");
            self.fail_load(&session_id, connection_id, &sender_id, request_id, &failure)
                .await;
            return;
        }

        self.start_resolved_media(&session_id, request_id, media)
            .await;
    }

    async fn on_app_playback(&mut self, event: AppPlaybackEvent) {
        let AppPlaybackEvent {
            session_id,
            command,
        } = event;
        if !self.sessions.contains_key(&session_id) {
            tracing::debug!(%session_id, "app playback command for stopped session; dropping");
            return;
        }

        match command {
            AppPlaybackCommand::Load(media) if media.streams.is_empty() => {
                tracing::warn!(%session_id, "app tried to load media without streams");
            }
            AppPlaybackCommand::Load(media) => {
                if let Some(session) = self.sessions.get_mut(&session_id) {
                    session.coordinator.media_session_id += 1;
                }
                self.start_resolved_media(&session_id, 0, media).await;
            }
            AppPlaybackCommand::Play => self.play(&session_id, 0).await,
            AppPlaybackCommand::Pause => self.pause(&session_id, 0).await,
            AppPlaybackCommand::Seek(position) => self.seek(&session_id, 0, position).await,
            AppPlaybackCommand::Stop => self.stop_playback(&session_id, 0).await,
        }
    }

    async fn start_resolved_media(
        &mut self,
        session_id: &str,
        request_id: i64,
        mut media: PlaybackMedia,
    ) {
        media.session_id = session_id.to_string();
        let media_session_id = self
            .sessions
            .get(session_id)
            .map(|session| session.coordinator.media_session_id)
            .unwrap_or(0);
        let media = self.attach_proxies(session_id, media_session_id, media);

        tracing::info!(
            session_id = %session_id,
            request_id,
            streams = media.streams.len(),
            stream_type = ?media.stream_type,
            "media resolved"
        );

        // Phase 3 + 4: broadcast resolved LOADING, then BUFFERING, then load.
        let (loading, buffering) = match self.sessions.get_mut(session_id) {
            Some(session) => {
                let coordinator = &mut session.coordinator;
                let info = media_info(&media);
                coordinator.playback_media = Some(media.clone());
                coordinator.current_media = Some(info.clone());
                coordinator.current_time = media.start_time;
                let loading = coordinator.loading_response(request_id, &info);
                coordinator.player_state = PlayerState::Buffering;
                coordinator.idle_reason = None;
                let buffering = coordinator.status_response(request_id);
                (loading, buffering)
            }
            None => return,
        };
        self.broadcast(session_id, ns::MEDIA, &loading).await;
        self.broadcast(session_id, ns::MEDIA, &buffering).await;

        self.player
            .send(PlayerCommand::Load {
                session_id: session_id.to_string(),
                media: to_payload(&media),
            })
            .await;
        self.notify_app(session_id).await;
    }

    fn attach_proxies(
        &mut self,
        session_id: &str,
        media_session_id: i64,
        mut media: PlaybackMedia,
    ) -> PlaybackMedia {
        // Exactly one current progressive object per output; speculative queue
        // resolution never reaches this point. A repeat carries the same hint.
        let requested = media
            .streams
            .first()
            .filter(|s| s.drm.is_none())
            .and_then(|s| {
                if let vibecast_sdk::StreamSource::CachedUrl { url, cache } = &s.source {
                    Some((url.clone(), cache.clone()))
                } else {
                    None
                }
            });
        if let Some(session) = self.sessions.get_mut(session_id) {
            let same = match (&session.audio_cache, &requested) {
                (Some(current), Some((_, hint))) => current.hint == *hint && !hint.is_failed(),
                _ => false,
            };
            if !same {
                if let Some(old) = session.audio_cache.take() {
                    old.cancel();
                }
                session.audio_cache = requested.map(|(url, hint)| {
                    Arc::new(crate::audio_cache::AudioCache::new(
                        self.http.clone(),
                        url,
                        hint,
                    ))
                });
            }
        }
        let (manifest_routes, license_routes) = collect_routes(&media);
        let audio_cache = self
            .sessions
            .get(session_id)
            .and_then(|s| s.audio_cache.clone());
        let has_manifest = !manifest_routes.is_empty() || audio_cache.is_some();
        let has_license = !license_routes.is_empty();
        if !has_manifest && !has_license {
            return media;
        }
        let Some(session) = self.sessions.get(session_id) else {
            return media;
        };
        let mut proxy = SessionProxy::new(
            session.app.clone(),
            self.callback_context(session, None),
            manifest_routes,
            license_routes,
        );
        proxy.audio_cache = audio_cache.clone();
        let proxy = Arc::new(proxy);
        let manifest_base =
            has_manifest.then(|| self.proxy.register_manifest(session_id, proxy.clone()));
        let license_base =
            has_license.then(|| self.proxy.register_license(session_id, proxy.clone()));
        rewrite_streams(
            &mut media,
            manifest_base.as_deref(),
            license_base.as_deref(),
            media_session_id,
        );
        if let (Some(cache), Some(base)) = (audio_cache, manifest_base) {
            media.streams[0].source =
                vibecast_sdk::StreamSource::Url(format!("{base}/{}", cache.token));
        }
        media
    }

    fn clear_audio_cache(&mut self, session_id: &str) {
        if let Some(cache) = self
            .sessions
            .get_mut(session_id)
            .and_then(|s| s.audio_cache.take())
        {
            cache.cancel();
        }
    }

    async fn fail_load(
        &mut self,
        session_id: &str,
        conn_id: u64,
        sender_id: &str,
        request_id: i64,
        failure: &MediaResolveError,
    ) {
        tracing::warn!(
            session = %session_id,
            reason = %failure.reason(),
            detail = ?failure.detail_code,
            retryable = failure.retryable,
            "load failed"
        );
        let (failed, status) = match self.sessions.get_mut(session_id) {
            Some(session) => {
                let coordinator = &mut session.coordinator;
                coordinator.set_idle(Some(IdleReason::Error));
                coordinator.clear_media();
                (
                    LoadFailedResponse::new(request_id, failure.reason()),
                    coordinator.status_response(request_id),
                )
            }
            None => return,
        };
        self.unregister_proxies(session_id);
        self.clear_audio_cache(session_id);
        self.send_to(conn_id, session_id, sender_id, ns::MEDIA, &failed)
            .await;
        self.broadcast(session_id, ns::MEDIA, &status).await;
        self.notify_app(session_id).await;
    }

    // -- player reports ---------------------------------------------------

    async fn publish_volume(&mut self) {
        let path = volume_path(&self.data_dir, &self.identity.device_id);
        let tmp = path.with_extension("tmp");
        let result = std::fs::create_dir_all(&self.data_dir).and_then(|_| {
            std::fs::write(
                &tmp,
                serde_json::to_vec(&self.volume).expect("finite volume"),
            )?;
            std::fs::rename(&tmp, &path)
        });
        if let Err(error) = result {
            tracing::warn!(%error, "cannot persist receiver volume");
        }
        for session in self.sessions.values_mut() {
            session.coordinator.volume = self.volume.clone();
            let _ = session
                .jobs
                .send(AppJob::VolumeUpdate {
                    ctx: session.ctx.clone(),
                    level: self.volume.level,
                    muted: self.volume.muted,
                })
                .await;
        }
    }

    async fn on_report(&mut self, report: PlayerReport) {
        let session_id = report.session_id().to_string();
        if !self.sessions.contains_key(&session_id) {
            return;
        }
        match report {
            PlayerReport::Artwork {
                source_url, url, ..
            } => {
                let Some(session) = self.sessions.get_mut(&session_id) else {
                    return;
                };
                if session.coordinator.update_artwork(&source_url, &url) {
                    let response = session.coordinator.status_response(0);
                    self.broadcast(&session_id, ns::MEDIA, &response).await;
                }
            }
            PlayerReport::ControlRequest { control, .. } => {
                self.on_output_control(&session_id, control).await;
            }
            PlayerReport::State {
                player_state,
                current_time,
                duration,
                idle_reason,
                volume,
                muted,
                ..
            } => {
                let before = self.volume.clone();
                if let Some(level) = volume.filter(|v| v.is_finite()) {
                    self.volume.level = level.clamp(0.0, 1.0);
                }
                if let Some(muted) = muted {
                    self.volume.muted = muted;
                }
                if self.volume != before {
                    self.publish_volume().await;
                    let response = self.receiver_status(0);
                    self.broadcast(RECEIVER_0, ns::RECEIVER, &response).await;
                }
                tracing::debug!(
                    session_id = %session_id,
                    state = ?player_state,
                    current_time,
                    ?duration,
                    ?idle_reason,
                    "player report"
                );
                self.apply_state(
                    &session_id,
                    player_state,
                    current_time,
                    duration,
                    idle_reason,
                )
                .await;
            }
            PlayerReport::Error { code, message, .. } => {
                tracing::warn!(session = %session_id, %code, %message, "player error");
                let (current_time, duration) = match self.sessions.get(&session_id) {
                    Some(session) => (
                        session.coordinator.current_time,
                        session
                            .coordinator
                            .current_media
                            .as_ref()
                            .and_then(|m| m.duration),
                    ),
                    None => return,
                };
                self.apply_state(
                    &session_id,
                    PlayerState::Idle,
                    current_time,
                    duration,
                    Some(IdleReason::Error),
                )
                .await;
            }
        }
    }

    async fn on_output_control(&mut self, session_id: &str, control: PlayerControlRequest) {
        match control {
            PlayerControlRequest::Play => self.request_play(session_id, 0).await,
            PlayerControlRequest::Pause => self.pause(session_id, 0).await,
            PlayerControlRequest::Stop => self.stop_playback(session_id, 0).await,
            PlayerControlRequest::Seek { position } if position.is_finite() => {
                self.seek(session_id, 0, position.max(0.0)).await
            }
            PlayerControlRequest::Seek { .. } => {}
            PlayerControlRequest::Volume { level, muted } if level.is_finite() => {
                self.volume.level = level.clamp(0.0, 1.0);
                self.volume.muted = muted;
                self.publish_volume().await;
                let response = self.receiver_status(0);
                self.broadcast(RECEIVER_0, ns::RECEIVER, &response).await;
            }
            PlayerControlRequest::Volume { .. } => {}
            PlayerControlRequest::Next | PlayerControlRequest::Previous => {
                let control = if matches!(control, PlayerControlRequest::Next) {
                    OutputControl::Next
                } else {
                    OutputControl::Previous
                };
                if let Some((jobs, ctx)) = self
                    .sessions
                    .get(session_id)
                    .map(|session| (session.jobs.clone(), session.ctx.clone()))
                {
                    let _ = jobs.send(AppJob::OutputControl { ctx, control }).await;
                }
            }
        }
    }

    async fn apply_state(
        &mut self,
        session_id: &str,
        player_state: PlayerState,
        current_time: f64,
        duration: Option<f64>,
        idle_reason: Option<IdleReason>,
    ) {
        let response = match self.sessions.get_mut(session_id) {
            Some(session) => {
                let coordinator = &mut session.coordinator;
                coordinator.player_state = player_state;
                coordinator.current_time = current_time;
                coordinator.idle_reason = idle_reason;
                if let Some(duration) = duration {
                    if let Some(media) = &mut coordinator.current_media {
                        media.duration = Some(duration);
                    }
                    if let Some(media) = &mut coordinator.playback_media {
                        media.duration = Some(duration);
                    }
                }
                coordinator.status_response(0)
            }
            None => return,
        };
        if player_state == PlayerState::Idle {
            // EOF is not eviction: repeat-one reopens the same bytes. Errors,
            // user Stop, a new title and session teardown release them.
            if idle_reason != Some(IdleReason::Finished) {
                self.clear_audio_cache(session_id);
            }
            self.unregister_proxies(session_id);
        }
        self.broadcast(session_id, ns::MEDIA, &response).await;
        self.notify_app(session_id).await;
    }

    // -- helpers ------------------------------------------------------------

    /// Current connection handles subscribed to a transport (for broadcasts).
    fn subscriber_handles(&self, transport: &str) -> Vec<ConnectionHandle> {
        let connection_ids: HashSet<u64> = self
            .subscriptions
            .iter()
            .filter(|(_, target)| target.as_str() == transport)
            .map(|((conn, _), _)| *conn)
            .collect();
        connection_ids
            .iter()
            .filter_map(|id| self.connections.get(id).cloned())
            .collect()
    }

    /// Build a per-callback app context whose custom-message sender writes to
    /// the bound sender (if any) or broadcasts to the transport's subscribers.
    fn callback_context(&self, session: &Session, bound: Option<(u64, String)>) -> AppContext {
        let transport_id = session.ctx.transport_id.clone();
        let subscribers = self.subscriber_handles(&transport_id);
        let bound = bound.and_then(|(conn_id, sender_id)| {
            self.connections
                .get(&conn_id)
                .map(|handle| (handle.clone(), sender_id))
        });
        let sender = Arc::new(HubSender {
            transport_id: transport_id.clone(),
            bound,
            subscribers,
        });
        AppContext::new(
            session.ctx.session_id.clone(),
            transport_id,
            session.ctx.app_id.clone(),
            session.ctx.http.clone(),
            session.ctx.receiver.clone(),
            sender,
        )
        .with_settings(session.ctx.settings.clone())
        .with_playback_controller(session.ctx.playback_controller())
    }

    async fn notify_app(&self, session_id: &str) {
        let (jobs, ctx, state) = match self.sessions.get(session_id) {
            Some(session) => {
                let coordinator = &session.coordinator;
                let state = PlaybackState {
                    player_state: coordinator.player_state,
                    current_time: coordinator.current_time,
                    duration: coordinator.current_media.as_ref().and_then(|m| m.duration),
                    idle_reason: coordinator.idle_reason,
                };
                (
                    session.jobs.clone(),
                    self.callback_context(session, None),
                    state,
                )
            }
            None => return,
        };
        let _ = jobs.send(AppJob::PlaybackUpdate { ctx, state }).await;
    }

    fn unregister_proxies(&self, session_id: &str) {
        self.proxy.unregister_manifest(session_id);
        self.proxy.unregister_license(session_id);
    }

    async fn send_to<T: Serialize>(
        &self,
        conn_id: u64,
        source: &str,
        dest: &str,
        namespace: &str,
        message: &T,
    ) {
        let Some(handle) = self.connections.get(&conn_id) else {
            return;
        };
        let Ok(value) = serde_json::to_value(message) else {
            return;
        };
        let _ = handle.send_json(source, dest, namespace, &value).await;
    }

    async fn broadcast<T: Serialize>(&self, transport: &str, namespace: &str, message: &T) {
        self.broadcast_except_connection(transport, namespace, message, None)
            .await;
    }

    async fn broadcast_except_connection<T: Serialize>(
        &self,
        transport: &str,
        namespace: &str,
        message: &T,
        excluded_conn_id: Option<u64>,
    ) {
        let Ok(value) = serde_json::to_value(message) else {
            return;
        };
        let connection_ids: HashSet<u64> = if transport == RECEIVER_0 {
            self.platform_subscriptions
                .iter()
                .map(|(conn, _)| *conn)
                .collect()
        } else {
            self.subscriptions
                .iter()
                .filter(|(_, target)| target.as_str() == transport)
                .map(|((conn, _), _)| *conn)
                .collect()
        };
        for conn_id in connection_ids {
            if Some(conn_id) == excluded_conn_id {
                continue;
            }
            if let Some(handle) = self.connections.get(&conn_id) {
                let _ = handle.send_json(transport, "*", namespace, &value).await;
            }
        }
    }
}

fn parse_payload(message: &CastMessage) -> Option<serde_json::Value> {
    let text = message.payload_utf8.as_deref()?;
    serde_json::from_str(text).ok()
}

#[cfg(test)]
mod volume_tests {
    use super::*;
    #[test]
    fn zero_promotes_only_at_session_start() {
        let mut volume = Volume {
            level: 0.0,
            muted: true,
            ..Default::default()
        };
        prepare_volume_for_session(&mut volume);
        assert_eq!(volume.level, 0.1);
        assert!(!volume.muted);
        volume.level = 0.5;
        prepare_volume_for_session(&mut volume);
        assert_eq!(volume.level, 0.5);
    }
    #[test]
    fn device_volume_paths_are_distinct_and_safe() {
        let base = std::path::Path::new("/tmp");
        let a = volume_path(base, "../a");
        assert_eq!(a.parent(), Some(base));
        assert_ne!(a, volume_path(base, "other"));
        let volume = Volume {
            level: 0.5,
            muted: true,
            ..Default::default()
        };
        let restored: Volume =
            serde_json::from_slice(&serde_json::to_vec(&volume).unwrap()).unwrap();
        assert_eq!(restored, volume);
    }
}
