//! Bundled YouTube app using the captured MDX/Lounge control flow.

#![forbid(unsafe_code)]

mod lounge;
mod resolver;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use vibecast_sdk::{
    AppContext, AppManifest, AppProvider, AppSession, AppSettingsReader, AppSettingsSchema,
    ChoiceOption, LaunchCredentials, LaunchError, LoadRequest, MediaResolveError,
    MessageDisposition, OutputControl, PlaybackController, PlaybackMedia, PlaybackState,
    SenderAttachment, SettingDescriptor, SettingScope,
};

use lounge::{LoungeCommand, LoungeConnection, LoungeIdentity, LoungeRunExit};
use resolver::{
    NavigationItem, NavigationRequest, PreferredVideoCodec, ResolveError, Resolver,
    PREFERRED_VIDEO_CODEC_KEY,
};

const APP_IDS: &[&str] = &["233637DE", "2DB7CC49"];
const YOUTUBE_MUSIC_APP_ID: &str = "2DB7CC49";
const MDX_NAMESPACE: &str = "urn:x-cast:com.google.youtube.mdx";
const MEDIA_NAMESPACE: &str = "urn:x-cast:com.google.cast.media";
const CUSTOM_DATA_NAMESPACE: &str = "urn:x-cast:com.google.cast.customdata";
const ICON_URL: &str = "https://www.gstatic.com/youtube/img/branding/favicon/favicon_144x144.png";

#[derive(Clone, PartialEq, Eq)]
struct CredentialTransferToken {
    video_id: String,
    token: String,
}

/// Opaque playlist continuation data supplied by YouTube. It can contain
/// account/session material, so values must never be formatted or logged.
#[derive(Clone, Default, PartialEq, Eq)]
struct PlaylistParams(String);

impl PlaylistParams {
    fn new(value: String) -> Option<Self> {
        (!value.trim().is_empty()).then_some(Self(value))
    }

    fn value(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for PlaylistParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PlaylistParams([REDACTED])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureClass {
    CredentialsRequired,
    Permanent,
    Transient,
}

struct CommittedContext {
    ownership_epoch: u64,
    selection_serial: u64,
    video_id: String,
    index: usize,
    list_id: Option<String>,
    params: Option<PlaylistParams>,
    credential: Option<CredentialTransferToken>,
}

enum WorkerEvent {
    ContextCommitted(CommittedContext),
    ResolutionFailed {
        ownership_epoch: u64,
        selection_serial: u64,
        video_id: String,
        class: ResolutionFailureClass,
        acknowledged: tokio::sync::oneshot::Sender<()>,
    },
}

impl CredentialTransferToken {
    fn new(video_id: String, token: String) -> Self {
        Self { video_id, token }
    }

    fn applies_to(&self, video_id: &str) -> bool {
        self.video_id == video_id
    }

    fn value(&self) -> &str {
        &self.token
    }
}

impl std::fmt::Debug for CredentialTransferToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialTransferToken")
            .field("video_id", &self.video_id)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

/// YouTube app provider.
#[derive(Debug, Default)]
pub struct YouTube;

impl YouTube {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AppProvider for YouTube {
    fn manifest(&self) -> AppManifest {
        let settings = AppSettingsSchema::with_display_name(
            "youtube",
            "YouTube",
            vec![SettingDescriptor::Choice {
                key: PREFERRED_VIDEO_CODEC_KEY.as_str().to_owned(),
                label: "Preferred video codec".to_owned(),
                description: Some(
                    "Choose which video codec YouTube should prefer when available.".to_owned(),
                ),
                scope: SettingScope::AppPlayer,
                default: "auto".to_owned(),
                choices: vec![
                    ChoiceOption::new("auto", "Automatic"),
                    ChoiceOption::new("av1", "AV1"),
                    ChoiceOption::new("vp9", "VP9"),
                    ChoiceOption::new("h264", "H.264"),
                ],
            }],
        )
        .expect("static YouTube settings must be valid");
        AppManifest::new("youtube", APP_IDS, "YouTube", settings)
            .with_icon_url(ICON_URL)
            .with_namespaces(&[CUSTOM_DATA_NAMESPACE, MDX_NAMESPACE])
    }

    async fn launch(
        &self,
        ctx: &AppContext,
        _credentials: LaunchCredentials,
    ) -> Result<Arc<dyn AppSession>, LaunchError> {
        let resolver = Resolver::new(ctx.http.clone());
        let playback = ctx.playback_controller();
        let capabilities = ctx.receiver.capabilities.clone();

        let (command_tx, command_rx) = mpsc::channel(32);
        let (output_control_tx, output_control_rx) = mpsc::channel(8);
        let (playback_tx, playback_rx) = mpsc::channel(32);
        let (volume_tx, volume_rx) = watch::channel((1.0, false));
        let (identity_tx, identity) = watch::channel(None);
        let (takeover_tx, takeover_rx) = mpsc::channel(2);
        let (worker_event_tx, worker_event_rx) = mpsc::channel(16);
        let (ownership_generation_tx, _) = watch::channel(0_u64);
        let mdx_requested = Arc::new(AtomicBool::new(false));
        let legacy_status_scheduled = Arc::new(AtomicBool::new(false));
        let (cancel, _) = watch::channel(false);
        tokio::spawn(run_commands_with_events(
            command_rx,
            resolver,
            playback,
            capabilities,
            ctx.settings.clone(),
            worker_event_tx,
            cancel.subscribe(),
        ));
        tokio::spawn(run_lounge(
            ctx.http.clone(),
            ctx.receiver.clone(),
            ctx.app_id == YOUTUBE_MUSIC_APP_ID,
            command_tx,
            playback_rx,
            output_control_rx,
            identity_tx,
            volume_rx,
            worker_event_rx,
            cancel.subscribe(),
            takeover_rx,
        ));

        Ok(Arc::new(YouTubeSession {
            resolver: Resolver::new(ctx.http.clone()),
            capabilities: ctx.receiver.capabilities.clone(),
            identity,
            mdx_requested,
            legacy_status_scheduled,
            ownership_generation_tx,
            takeover_tx,
            playback_tx,
            output_control_tx,
            volume_tx,
            cancel,
        }))
    }
}

struct YouTubeSession {
    resolver: Resolver,
    capabilities: vibecast_sdk::PlayerCapabilities,
    identity: watch::Receiver<Option<LoungeIdentity>>,
    mdx_requested: Arc<AtomicBool>,
    legacy_status_scheduled: Arc<AtomicBool>,
    ownership_generation_tx: watch::Sender<u64>,
    takeover_tx: mpsc::Sender<tokio::sync::oneshot::Sender<()>>,
    playback_tx: mpsc::Sender<PlaybackState>,
    output_control_tx: mpsc::Sender<OutputControl>,
    volume_tx: watch::Sender<(f64, bool)>,
    cancel: watch::Sender<bool>,
}

#[async_trait]
impl AppSession for YouTubeSession {
    async fn resolve_media(
        &self,
        ctx: &AppContext,
        request: &LoadRequest,
    ) -> Result<PlaybackMedia, MediaResolveError> {
        let settings = ctx.settings.snapshot();
        let preferred_video_codec = PreferredVideoCodec::from_snapshot(&settings);
        let video_id = resolver::extract_video_id(&request.media.content_id)
            .ok_or_else(|| MediaResolveError::invalid_request("INVALID_YOUTUBE_VIDEO_ID"))?;
        let media = self
            .resolver
            .resolve(
                &video_id,
                request.current_time,
                &self.capabilities,
                preferred_video_codec,
                None,
            )
            .await
            .map_err(map_resolve_error)?;
        prepare_audio_refresh(
            &media,
            &self.resolver,
            &video_id,
            &self.capabilities,
            preferred_video_codec,
            0,
            None,
        );
        Ok(media)
    }

    async fn on_message(
        &self,
        ctx: &AppContext,
        namespace: &str,
        data: &serde_json::Value,
    ) -> MessageDisposition {
        if namespace != MDX_NAMESPACE {
            return MessageDisposition::Unhandled;
        }
        let message_type = data.get("type").and_then(serde_json::Value::as_str);
        if message_type != Some("getMdxSessionStatus") {
            return MessageDisposition::Unhandled;
        }
        tracing::debug!(
            identity_ready = self.identity.borrow().is_some(),
            request_id_present = data.get("requestId").is_some(),
            "YouTube MDX session status requested"
        );
        self.mdx_requested.store(true, Ordering::Release);
        send_mdx_session_status_when_ready(
            ctx.clone(),
            self.identity.clone(),
            self.cancel.subscribe(),
            self.ownership_generation_tx.subscribe(),
            *self.ownership_generation_tx.borrow(),
            data.get("requestId").cloned(),
        );
        MessageDisposition::Handled
    }

    async fn on_sender_connected(
        &self,
        ctx: &AppContext,
        _sender_id: &str,
        attachment: SenderAttachment,
    ) {
        if attachment != SenderAttachment::Initial {
            // Status requests and fallback delivery belong to one physical
            // sender connection. A returning owner needs the same Lounge
            // identity sent again; a takeover receives the rotated identity.
            self.mdx_requested.store(false, Ordering::Release);
            self.legacy_status_scheduled.store(false, Ordering::Release);
        }
        if attachment == SenderAttachment::Takeover {
            self.ownership_generation_tx
                .send_modify(|value| *value += 1);
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            match self.takeover_tx.send(ready_tx).await {
                Err(_) => tracing::warn!("could not rotate YouTube Lounge ownership"),
                Ok(()) => match tokio::time::timeout(std::time::Duration::from_secs(15), ready_rx)
                    .await
                {
                    Ok(Ok(())) => tracing::info!(connection_id = ?ctx.sender_connection_id(),
                        "YouTube Lounge ownership transferred"),
                    Ok(Err(_)) => tracing::warn!("YouTube Lounge ownership rotation was cancelled"),
                    Err(_) => tracing::warn!("timed out rotating YouTube Lounge ownership"),
                },
            }
        }
        // Older/mobile senders can wait for an unsolicited status, while the
        // current desktop sender asks explicitly. Send at most one delayed
        // compatibility status so an explicit request cannot be raced by a
        // burst from every logical Cast sender connection.
        if self.legacy_status_scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        let ctx = ctx.clone();
        let identity = self.identity.clone();
        let cancel = self.cancel.subscribe();
        let ownership_generation = self.ownership_generation_tx.subscribe();
        let expected_generation = *ownership_generation.borrow();
        let mdx_requested = self.mdx_requested.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if !mdx_requested.load(Ordering::Acquire) {
                send_mdx_session_status_when_ready(
                    ctx,
                    identity,
                    cancel,
                    ownership_generation,
                    expected_generation,
                    None,
                );
            }
        });
    }

    fn exclusive_sender_takeover(&self) -> bool {
        true
    }

    fn claims_sender_ownership(&self, namespace: &str, data: &serde_json::Value) -> bool {
        namespace == MDX_NAMESPACE && data["type"] == "getMdxSessionStatus"
    }

    fn joins_sender_ownership(&self, namespace: &str, data: &serde_json::Value) -> bool {
        // The iOS RemoteMediaClient joins an existing YouTube receiver by
        // bootstrapping its queue and does not always ask for MDX status first.
        // Core treats this as a guarded, weak claim: detached/superseded
        // controllers cannot use it to steal ownership back automatically.
        namespace == MEDIA_NAMESPACE && data["type"] == "QUEUE_GET_ITEM_IDS"
    }

    async fn on_playback_update(&self, _ctx: &AppContext, state: PlaybackState) {
        let _ = self.playback_tx.try_send(state);
    }

    async fn on_volume_update(&self, _ctx: &AppContext, level: f64, muted: bool) {
        self.volume_tx.send_if_modified(|value| {
            if *value == (level, muted) {
                return false;
            }
            *value = (level, muted);
            true
        });
    }

    fn handles_play_requests(&self) -> bool {
        true
    }

    fn handles_output_control(&self, _control: &OutputControl) -> bool {
        true
    }

    fn supports_queue_navigation(&self) -> bool {
        true
    }

    fn allows_sender_reconnect_grace(&self) -> bool {
        // Lounge/cache can bridge a brief control-connection loss, but the
        // runtime still stops playback after its bounded grace expires.
        true
    }

    async fn on_output_control(&self, _ctx: &AppContext, control: OutputControl) {
        if self.output_control_tx.send(control).await.is_err() {
            tracing::debug!("dropping output control for stopped YouTube session");
        }
    }

    async fn on_stop(&self, _ctx: &AppContext) {
        let _ = self.cancel.send(true);
    }
}

fn send_mdx_session_status_when_ready(
    ctx: AppContext,
    mut identity: watch::Receiver<Option<LoungeIdentity>>,
    mut cancel: watch::Receiver<bool>,
    mut ownership_generation: watch::Receiver<u64>,
    expected_generation: u64,
    request_id: Option<serde_json::Value>,
) {
    tokio::spawn(async move {
        loop {
            if *cancel.borrow()
                || cancel.has_changed().is_err()
                || *ownership_generation.borrow() != expected_generation
            {
                return;
            }
            let current_identity = { identity.borrow().clone() };
            if let Some(identity) = current_identity {
                let mut response = serde_json::json!({
                    "type": "mdxSessionStatus",
                    "data": {
                        "screenId": identity.screen_id,
                        "deviceId": identity.device_id,
                    }
                });
                if let Some(request_id) = request_id.as_ref() {
                    response["requestId"] = request_id.clone();
                }
                tracing::debug!(
                    request_id_present = request_id.is_some(),
                    "sending YouTube MDX session status"
                );
                if *cancel.borrow()
                    || cancel.has_changed().is_err()
                    || *ownership_generation.borrow() != expected_generation
                {
                    return;
                }
                ctx.send_custom(MDX_NAMESPACE, response).await;
                return;
            }

            tracing::debug!("waiting for YouTube Lounge identity before MDX response");

            tokio::select! {
                result = identity.changed() => {
                    if result.is_err() {
                        return;
                    }
                }
                result = cancel.changed() => {
                    if result.is_err() || *cancel.borrow() {
                        return;
                    }
                }
                result = ownership_generation.changed() => {
                    if result.is_err() || *ownership_generation.borrow() != expected_generation {
                        return;
                    }
                }
            }
        }
    });
}

async fn run_lounge(
    http: reqwest::Client,
    receiver: vibecast_sdk::ReceiverContext,
    youtube_music: bool,
    command_tx: mpsc::Sender<LoungeCommand>,
    playback_rx: mpsc::Receiver<PlaybackState>,
    output_control_rx: mpsc::Receiver<OutputControl>,
    identity_tx: watch::Sender<Option<LoungeIdentity>>,
    volume_rx: watch::Receiver<(f64, bool)>,
    mut worker_event_rx: mpsc::Receiver<WorkerEvent>,
    mut cancel: watch::Receiver<bool>,
    mut takeover_rx: mpsc::Receiver<tokio::sync::oneshot::Sender<()>>,
) {
    let mut playback_rx = playback_rx;
    let mut output_control_rx = output_control_rx;
    let mut pending_ready = Vec::new();
    let mut ownership_epoch = 0_u64;

    loop {
        let mut lounge = loop {
            let establish = LoungeConnection::establish(http.clone(), &receiver, youtube_music);
            let result = tokio::select! {
                result = establish => Some(result),
                request = takeover_rx.recv() => {
                    let Some(request) = request else { return; };
                    let _ = identity_tx.send(None);
                    pending_ready.push(request);
                    None
                }
                result = cancel.changed() => {
                    if result.is_err() || *cancel.borrow() {
                        return;
                    }
                    None
                }
            };
            match result {
                Some(Ok(lounge)) => break lounge,
                Some(Err(error)) => {
                    tracing::warn!(%error, "YouTube Lounge pairing failed; retrying");
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                        request = takeover_rx.recv() => {
                            let Some(request) = request else { return; };
                            let _ = identity_tx.send(None);
                            pending_ready.push(request);
                        }
                        result = cancel.changed() => {
                            if result.is_err() || *cancel.borrow() {
                                return;
                            }
                        }
                    }
                }
                None => {}
            }
        };

        tracing::debug!("YouTube Lounge identity is ready");
        lounge.set_ownership_epoch(ownership_epoch);
        let _ = identity_tx.send(Some(lounge.identity()));
        for ready in pending_ready.drain(..) {
            let _ = ready.send(());
        }
        match lounge
            .with_volume(volume_rx.clone())
            .run(
                &command_tx,
                &mut playback_rx,
                &mut output_control_rx,
                &mut worker_event_rx,
                &mut cancel,
                &mut takeover_rx,
            )
            .await
        {
            LoungeRunExit::Stopped => return,
            LoungeRunExit::Takeover {
                ready,
                ownership_epoch: previous_epoch,
            } => {
                ownership_epoch = previous_epoch.wrapping_add(1);
                let _ = identity_tx.send(None);
                // End the old queue and cancel any resolver/prefetch before
                // advertising the new screen to the incoming controller.
                if command_tx.send(LoungeCommand::Stop).await.is_err() {
                    return;
                }
                while playback_rx.try_recv().is_ok() {}
                while output_control_rx.try_recv().is_ok() {}
                pending_ready.push(ready);
            }
        }
    }
}

#[derive(Default)]
struct QueueState {
    video_ids: Vec<String>,
    current_index: usize,
    list_id: Option<String>,
    params: Option<PlaylistParams>,
    credential: Option<CredentialTransferToken>,
    remote_device_ids: Vec<String>,
    generation: u64,
    ownership_epoch: u64,
    selection_serial: u64,
    next_pending: bool,
}

impl QueueState {
    fn updated_index(&self, video_ids: &[String]) -> usize {
        retained_selection_index(
            self.video_ids.get(self.current_index).map(String::as_str),
            self.current_index,
            video_ids,
        )
    }

    fn selects_prepared_next(&self, command: &LoungeCommand) -> bool {
        match command {
            LoungeCommand::Next => true,
            LoungeCommand::SetPlaylist {
                video_ids,
                current_index,
                list_id,
                credential,
                ..
            } => {
                self.list_id.is_some()
                    && self.list_id == *list_id
                    && credential.is_none()
                    && *current_index == self.current_index + 1
                    && video_ids.get(*current_index).is_some()
                    && video_ids.get(*current_index) == self.video_ids.get(self.current_index + 1)
            }
            _ => false,
        }
    }

    fn advances_to_prepared_next(&self, command: &LoungeCommand) -> bool {
        self.selects_prepared_next(command)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PrefetchKey {
    generation: u64,
    list_id: Option<String>,
    index: usize,
    video_id: String,
}

impl QueueState {
    fn next_prefetch_key(&self) -> Option<PrefetchKey> {
        let index = self.current_index.checked_add(1)?;
        Some(PrefetchKey {
            generation: self.generation,
            list_id: self.list_id.clone(),
            index,
            video_id: self.video_ids.get(index)?.clone(),
        })
    }
}

// Queue edits do not select another title. Keep the active slot if unchanged
// (including duplicate entries), otherwise follow that title to its new slot.
fn retained_selection_index(
    current_id: Option<&str>,
    current_index: usize,
    video_ids: &[String],
) -> usize {
    let Some(current_id) = current_id else {
        return current_index;
    };
    if video_ids.get(current_index).map(String::as_str) == Some(current_id) {
        current_index
    } else {
        video_ids
            .iter()
            .position(|id| id == current_id)
            .unwrap_or(current_index)
    }
}

// One session-local speculative result; never written to disk. Dropping it
// cancels the async task, so obsolete results cannot start playback.
struct NextResolution {
    key: PrefetchKey,
    task: Option<tokio::task::JoinHandle<PreparedNext>>,
}

struct PreparedNext {
    media: Result<PlaybackMedia, ResolveError>,
    completed: std::time::Instant,
    context: NavigationItem,
}

#[derive(Default)]
struct KnownNavigation {
    generation: u64,
    items: std::collections::HashMap<(usize, String), NavigationItem>,
}

impl KnownNavigation {
    fn reset(&mut self, generation: u64) {
        self.generation = generation;
        self.items.clear();
    }

    fn insert(&mut self, generation: u64, item: NavigationItem) {
        if self.generation != generation {
            return;
        }
        if let Some(index) = item.index {
            self.items.insert((index, item.video_id.clone()), item);
        }
    }

    fn retag(&mut self, generation: u64) {
        self.generation = generation;
    }

    fn get(&self, generation: u64, index: usize, video_id: &str) -> Option<NavigationItem> {
        (self.generation == generation)
            .then(|| self.items.get(&(index, video_id.to_owned())).cloned())
            .flatten()
    }
}

impl QueueState {
    fn current_navigation(&self) -> Option<NavigationItem> {
        Some(NavigationItem {
            video_id: self.video_ids.get(self.current_index)?.clone(),
            list_id: self.list_id.clone(),
            index: Some(self.current_index),
            params: self.params.as_ref().map(|value| value.value().to_owned()),
            credential: self.credential.clone(),
        })
    }

    // A queue mutation is not a new selection. Retain only the exact active
    // item's context and invalidate speculative neighbours from the old shape.
    fn apply_playlist_update(
        &mut self,
        video_ids: Vec<String>,
        list_id: Option<String>,
        params: Option<PlaylistParams>,
        known: &mut KnownNavigation,
    ) {
        let current_video_id = self.video_ids.get(self.current_index).cloned();
        let current_credential = self.credential.clone();
        let current_params = self.params.clone();
        let updated_index = self.updated_index(&video_ids);
        let retained = current_video_id
            .as_ref()
            .is_some_and(|video_id| video_ids.get(updated_index) == Some(video_id));
        let list_id = list_id.or_else(|| self.list_id.clone());
        let params = if retained {
            current_params.or(params)
        } else {
            params
        };
        let changed =
            self.video_ids != video_ids || self.list_id != list_id || self.params != params;

        self.video_ids = video_ids;
        self.current_index = updated_index.min(self.video_ids.len().saturating_sub(1));
        self.list_id = list_id;
        self.params = params;
        self.credential = retained.then_some(current_credential).flatten();

        if changed {
            self.generation = self.generation.wrapping_add(1);
            known.reset(self.generation);
            if retained {
                if let Some(context) = self.current_navigation() {
                    known.insert(self.generation, context);
                }
            }
        }
    }
}

impl Drop for NextResolution {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn prefetched_is_fresh(completed: std::time::Instant) -> bool {
    completed.elapsed() < std::time::Duration::from_secs(600)
}

fn navigation_matches_prefetch(item: &NavigationItem, key: &PrefetchKey) -> bool {
    item.video_id == key.video_id
        && item.index.is_none_or(|index| index == key.index)
        && !(item.list_id.is_some() && key.list_id.is_some() && item.list_id != key.list_id)
}

// Keep only the current stream description, not audio bytes. Replaying must not
// renew its age: signed URLs expire even when the same title keeps repeating.
struct CurrentResolution {
    video_id: String,
    credential: Option<CredentialTransferToken>,
    media: PlaybackMedia,
    completed: std::time::Instant,
    codec: PreferredVideoCodec,
}

struct FailedResolution {
    video_id: String,
    position: f64,
    credential: Option<CredentialTransferToken>,
    class: ResolutionFailureClass,
}

impl CurrentResolution {
    fn replay(
        &self,
        video_id: Option<&String>,
        codec: PreferredVideoCodec,
        credential: Option<&CredentialTransferToken>,
    ) -> Option<PlaybackMedia> {
        let cache = self
            .media
            .streams
            .first()
            .and_then(|stream| match &stream.source {
                vibecast_sdk::StreamSource::CachedUrl { cache, .. } => Some(cache),
                _ => None,
            });
        if video_id != Some(&self.video_id)
            || codec != self.codec
            || credential != self.credential.as_ref()
            || cache.is_some_and(|c| c.is_failed())
            || (!cache.is_some_and(|c| c.is_complete()) && !prefetched_is_fresh(self.completed))
        {
            return None;
        }
        let mut media = self.media.clone();
        media.start_time = 0.0;
        media.autoplay = true;
        Some(media)
    }
}

// Refresh only the signed location of the exact same encoded object. A changed
// format/revision must never be appended to bytes already held by the decoder.
struct CurrentAudioRefresh {
    resolver: Resolver,
    video_id: String,
    credential: Option<CredentialTransferToken>,
    capabilities: vibecast_sdk::PlayerCapabilities,
    codec: PreferredVideoCodec,
    identity: AudioObjectIdentity,
}

impl std::fmt::Debug for CurrentAudioRefresh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CurrentAudioRefresh")
    }
}

#[derive(Debug, PartialEq)]
struct AudioObjectIdentity {
    itag: String,
    modified: String,
    length: String,
    content_type: String,
}

impl AudioObjectIdentity {
    fn from_stream(stream: &vibecast_sdk::PlaybackStream) -> Option<Self> {
        let url = url::Url::parse(stream.source.as_url()?).ok()?;
        let field = |name: &str| {
            let mut values = url.query_pairs().filter(|(key, _)| key == name);
            let value = values.next()?.1.into_owned();
            (values.next().is_none()
                && value.bytes().all(|byte| byte.is_ascii_digit())
                && value.parse::<u64>().is_ok_and(|number| number > 0))
            .then_some(value)
        };
        if stream.content_type.trim().is_empty() {
            return None;
        }
        Some(Self {
            // googlevideo's `id` can change between resolutions of the same
            // bytes. The resolver is bound to video_id; these fields identify
            // its encoded representation, independently of signed URL routing.
            itag: field("itag")?,
            modified: field("lmt")?,
            length: field("clen")?,
            content_type: stream.content_type.clone(),
        })
    }
}

impl CurrentAudioRefresh {
    async fn refresh_source(&self) -> Option<vibecast_sdk::RefreshedMediaSource> {
        let media = self
            .resolver
            .resolve(
                &self.video_id,
                0.0,
                &self.capabilities,
                self.codec,
                self.credential.as_ref(),
            )
            .await
            .ok()?;
        if media
            .content_id
            .as_ref()
            .is_some_and(|id| id != &self.video_id)
        {
            tracing::warn!(video_mismatch = true, "refreshed audio identity rejected");
            return None;
        }
        media.streams.iter().find_map(|stream| {
            let Some(identity) = AudioObjectIdentity::from_stream(stream) else {
                tracing::warn!(
                    malformed_identity = true,
                    "refreshed audio identity rejected"
                );
                return None;
            };
            if identity != self.identity {
                tracing::warn!(
                    format_mismatch = identity.itag != self.identity.itag,
                    revision_mismatch = identity.modified != self.identity.modified,
                    length_mismatch = identity.length != self.identity.length,
                    mime_mismatch = identity.content_type != self.identity.content_type,
                    "refreshed audio identity rejected"
                );
                return None;
            }
            let url = stream.source.as_url()?.to_owned();
            let headers = match &stream.source {
                vibecast_sdk::StreamSource::CachedUrl { cache, .. } => cache.request_headers(),
                _ => Default::default(),
            };
            vibecast_sdk::RefreshedMediaSource::new(url)
                .with_request_headers(headers)
                .ok()
        })
    }
}

#[async_trait]
impl vibecast_sdk::MediaSourceRefresh for CurrentAudioRefresh {
    async fn refresh(&self) -> Option<String> {
        self.refresh_source().await.map(|source| source.url)
    }

    async fn refresh_with_context(&self) -> Option<vibecast_sdk::RefreshedMediaSource> {
        self.refresh_source().await
    }
}

fn prepare_audio_refresh(
    media: &PlaybackMedia,
    resolver: &Resolver,
    video_id: &str,
    capabilities: &vibecast_sdk::PlayerCapabilities,
    codec: PreferredVideoCodec,
    retries_used: u8,
    credential: Option<&CredentialTransferToken>,
) {
    for stream in &media.streams {
        if let vibecast_sdk::StreamSource::CachedUrl { cache, .. } = &stream.source {
            cache.set_recovery_attempts(retries_used);
            if let Some(identity) = AudioObjectIdentity::from_stream(stream) {
                cache.set_refresh(Arc::new(CurrentAudioRefresh {
                    resolver: resolver.clone(),
                    video_id: video_id.to_owned(),
                    credential: credential.cloned(),
                    capabilities: capabilities.clone(),
                    codec,
                    identity,
                }));
            }
        }
    }
}

async fn run_commands_with_events(
    mut commands: mpsc::Receiver<LoungeCommand>,
    resolver: Resolver,
    playback: Arc<dyn PlaybackController>,
    capabilities: vibecast_sdk::PlayerCapabilities,
    settings: AppSettingsReader,
    worker_events: mpsc::Sender<WorkerEvent>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut queue = QueueState::default();
    let mut next_resolution: Option<NextResolution> = None;
    let mut current_resolution: Option<CurrentResolution> = None;
    let mut playback_active = false;
    let mut stopped = false;
    let mut failed_resolution: Option<FailedResolution> = None;
    let known_navigation = Arc::new(Mutex::new(KnownNavigation::default()));
    let mut deferred = None;
    'commands: loop {
        let command = if let Some(command) = deferred.take() {
            command
        } else {
            tokio::select! {
                biased;
                result = cancel.changed() => {
                    if result.is_err() || *cancel.borrow() {
                        return;
                    }
                    continue;
                }
                command = commands.recv() => {
                    let Some(command) = command else { return; };
                    command
                }
            }
        };

        let repeating = matches!(command, LoungeCommand::RepeatCurrent);
        if repeating {
            queue.selection_serial = queue.selection_serial.wrapping_add(1);
        }
        let reload_autoplay = match &command {
            LoungeCommand::ReloadCurrent { autoplay, .. } => Some(*autoplay),
            _ => None,
        };
        let retries_used = match &command {
            LoungeCommand::ReloadCurrent { retries_used, .. } => *retries_used,
            _ => 0,
        };
        if repeating && playback_active {
            let codec = PreferredVideoCodec::from_snapshot(&settings.snapshot());
            if let Some(media) = current_resolution.as_ref().and_then(|current| {
                current.replay(
                    queue.video_ids.get(queue.current_index),
                    codec,
                    queue.credential.as_ref(),
                )
            }) {
                let cached_audio = media.streams.iter().any(|stream| matches!(
                    &stream.source, vibecast_sdk::StreamSource::CachedUrl { cache, .. } if cache.is_complete()
                ));
                tracing::info!(cached_audio, "reusing current YouTube media for repeat");
                playback.load(media).await;
                continue;
            }
        }

        // A same-queue selection of the exact next item may consume its
        // speculative context/media; unrelated selections keep the established
        // fresh-resolution path.
        let consume_prefetch_context = queue.selects_prepared_next(&command);
        let use_prefetch = queue.advances_to_prepared_next(&command);
        if let LoungeCommand::SetPlaylist {
            current_index,
            video_ids,
            current_time,
            list_id,
            params: _,
            credential,
        } = &command
        {
            tracing::info!(
                index = *current_index,
                queue_len = video_ids.len(),
                start = *current_time,
                same_queue = queue.list_id.is_some() && queue.list_id == *list_id,
                has_ctt = credential.is_some(),
                use_prefetch,
                "YouTube playlist selection"
            );
        }
        let load = match command {
            LoungeCommand::ReloadCurrent {
                video_id,
                current_time,
                credential,
                ..
            } => {
                if stopped || queue.video_ids.get(queue.current_index) != Some(&video_id) {
                    continue;
                }
                let credential = credential.or_else(|| {
                    queue
                        .credential
                        .as_ref()
                        .filter(|value| value.applies_to(&video_id))
                        .cloned()
                });
                Some((video_id, current_time, credential))
            }
            LoungeCommand::RepeatCurrent => {
                queue.next_pending = false;
                queue
                    .video_ids
                    .get(queue.current_index)
                    .cloned()
                    .map(|video_id| (video_id, 0.0, queue.credential.clone()))
            }
            LoungeCommand::SetPlaylist {
                video_ids,
                current_index,
                current_time,
                list_id,
                params,
                credential,
            } => {
                queue.selection_serial = queue.selection_serial.wrapping_add(1);
                let bounded_index = current_index.min(video_ids.len().saturating_sub(1));
                let retained_context = if !use_prefetch
                    && credential.is_none()
                    && params.is_none()
                    && queue.list_id.is_some()
                    && queue.list_id == list_id
                {
                    video_ids.get(bounded_index).and_then(|video_id| {
                        known_navigation.lock().unwrap().get(
                            queue.generation,
                            bounded_index,
                            video_id,
                        )
                    })
                } else {
                    None
                };
                if !consume_prefetch_context {
                    queue.generation = queue.generation.wrapping_add(1);
                    known_navigation.lock().unwrap().reset(queue.generation);
                }
                queue.video_ids = video_ids;
                queue.current_index = bounded_index;
                queue.list_id = retained_context
                    .as_ref()
                    .and_then(|context| context.list_id.clone())
                    .or(list_id);
                queue.params = params.or_else(|| {
                    retained_context
                        .as_ref()
                        .and_then(|context| context.params.clone())
                        .and_then(PlaylistParams::new)
                });
                queue.credential = credential.or_else(|| {
                    retained_context
                        .as_ref()
                        .and_then(|context| context.credential.clone())
                });
                queue.next_pending = false;
                if let (Some(video_id), Some(credential)) = (
                    queue.video_ids.get(queue.current_index),
                    queue.credential.clone(),
                ) {
                    known_navigation.lock().unwrap().insert(
                        queue.generation,
                        NavigationItem {
                            video_id: video_id.clone(),
                            list_id: queue.list_id.clone(),
                            index: Some(queue.current_index),
                            params: queue.params.as_ref().map(|value| value.value().to_owned()),
                            credential: Some(credential),
                        },
                    );
                }
                queue
                    .video_ids
                    .get(queue.current_index)
                    .cloned()
                    .map(|video_id| (video_id, current_time, queue.credential.clone()))
            }
            LoungeCommand::UpdatePlaylist {
                video_ids,
                list_id,
                params,
            } => {
                queue.apply_playlist_update(
                    video_ids,
                    list_id,
                    params,
                    &mut known_navigation.lock().unwrap(),
                );
                if queue.next_pending && queue.current_index + 1 < queue.video_ids.len() {
                    queue.selection_serial = queue.selection_serial.wrapping_add(1);
                    queue.current_index += 1;
                    queue.credential = None;
                    queue.params = None;
                    queue.next_pending = false;
                    queue
                        .video_ids
                        .get(queue.current_index)
                        .cloned()
                        .map(|video_id| (video_id, 0.0, None))
                } else {
                    None
                }
            }
            LoungeCommand::RemoteDevices(device_ids) => {
                if queue.remote_device_ids != device_ids {
                    queue.remote_device_ids = device_ids;
                    queue.generation = queue.generation.wrapping_add(1);
                    known_navigation.lock().unwrap().retag(queue.generation);
                    next_resolution = None;
                }
                None
            }
            LoungeCommand::Next => {
                if queue.current_index + 1 < queue.video_ids.len() {
                    queue.selection_serial = queue.selection_serial.wrapping_add(1);
                    queue.current_index += 1;
                    let context = queue
                        .video_ids
                        .get(queue.current_index)
                        .and_then(|video_id| {
                            known_navigation.lock().unwrap().get(
                                queue.generation,
                                queue.current_index,
                                video_id,
                            )
                        });
                    queue.credential = context
                        .as_ref()
                        .and_then(|context| context.credential.clone());
                    if let Some(context) = context {
                        queue.list_id = context.list_id.or_else(|| queue.list_id.take());
                        queue.params = context.params.and_then(PlaylistParams::new);
                    } else {
                        queue.params = None;
                    }
                    queue
                        .video_ids
                        .get(queue.current_index)
                        .cloned()
                        .map(|video_id| (video_id, 0.0, queue.credential.clone()))
                } else {
                    queue.next_pending = true;
                    None
                }
            }
            LoungeCommand::Previous => {
                if queue.video_ids.is_empty() {
                    continue;
                }
                queue.selection_serial = queue.selection_serial.wrapping_add(1);
                if queue.current_index > 0 {
                    queue.current_index -= 1;
                }
                let context = queue
                    .video_ids
                    .get(queue.current_index)
                    .and_then(|video_id| {
                        known_navigation.lock().unwrap().get(
                            queue.generation,
                            queue.current_index,
                            video_id,
                        )
                    });
                queue.credential = context
                    .as_ref()
                    .and_then(|context| context.credential.clone());
                if let Some(context) = context {
                    queue.list_id = context.list_id.or_else(|| queue.list_id.take());
                    queue.params = context.params.and_then(PlaylistParams::new);
                } else {
                    queue.params = None;
                }
                queue.next_pending = false;
                queue
                    .video_ids
                    .get(queue.current_index)
                    .cloned()
                    .map(|video_id| (video_id, 0.0, queue.credential.clone()))
            }
            LoungeCommand::Play => {
                if let Some(failed) = failed_resolution.take() {
                    if failed.class != ResolutionFailureClass::Transient
                        && failed.credential == queue.credential
                    {
                        tracing::info!(
                            video_id = %failed.video_id,
                            "ignoring Play without changed context after terminal resolution failure"
                        );
                        failed_resolution = Some(failed);
                        None
                    } else if !stopped
                        && queue.video_ids.get(queue.current_index) == Some(&failed.video_id)
                    {
                        Some((failed.video_id, failed.position, queue.credential.clone()))
                    } else {
                        None
                    }
                } else {
                    playback.play().await;
                    None
                }
            }
            LoungeCommand::Pause => {
                playback.pause().await;
                None
            }
            LoungeCommand::Seek(position) => {
                playback.seek(position).await;
                None
            }
            LoungeCommand::Stop => {
                stopped = true;
                failed_resolution = None;
                current_resolution = None;
                queue.credential = None;
                queue.params = None;
                queue.remote_device_ids.clear();
                queue.next_pending = false;
                queue.generation = queue.generation.wrapping_add(1);
                queue.ownership_epoch = queue.ownership_epoch.wrapping_add(1);
                queue.selection_serial = 0;
                known_navigation.lock().unwrap().reset(queue.generation);
                playback_active = false;
                next_resolution = None;
                playback.stop().await;
                None
            }
        };

        if let Some((video_id, start_time, credential)) = load {
            stopped = false;
            failed_resolution = None;
            current_resolution = None;
            tracing::info!(%video_id, start_time, "YouTube queue requests load");
            let snapshot = settings.snapshot();
            let preferred_video_codec = PreferredVideoCodec::from_snapshot(&snapshot);
            // Repeat and current-item recovery are independent from the next
            // title and must not cancel its preparation.
            let pending = if repeating || reload_autoplay.is_some() {
                None
            } else {
                next_resolution.take()
            };
            let requested_context = NavigationItem {
                video_id: video_id.clone(),
                list_id: queue.list_id.clone(),
                index: Some(queue.current_index),
                params: queue.params.as_ref().map(|value| value.value().to_owned()),
                credential: credential.clone(),
            };
            let expected_generation = queue.generation;
            let expected_index = queue.current_index;
            let expected_ownership_epoch = queue.ownership_epoch;
            let mut event_selection_serial = queue.selection_serial;
            let resolution = async {
                let mut prepared = None;
                let mut effective_context = requested_context;
                if let Some(mut pending) = pending {
                    if consume_prefetch_context
                        && pending.key.generation == expected_generation
                        && pending.key.index == expected_index
                        && pending.key.video_id == video_id
                    {
                        if let Some(task) = pending.task.as_mut() {
                            if let Ok(next) = task.await {
                                // The command loop supersedes this future for an
                                // actual selection change. Queue enrichment or a
                                // remote-device update does not invalidate media
                                // and CTT that are bound to this exact video.
                                // Sender-supplied item context is newer than the
                                // speculative navigation result. Preserve it and
                                // fill only missing fields (most importantly the
                                // CTT) from the prepared next item.
                                effective_context = NavigationItem {
                                    video_id: video_id.clone(),
                                    list_id: effective_context
                                        .list_id
                                        .take()
                                        .or(next.context.list_id),
                                    index: effective_context.index.or(next.context.index),
                                    params: effective_context.params.take().or(next.context.params),
                                    credential: effective_context
                                        .credential
                                        .take()
                                        .or(next.context.credential),
                                };
                                if use_prefetch && prefetched_is_fresh(next.completed) {
                                    if let Ok(mut media) = next.media {
                                        media.start_time = start_time.max(0.0);
                                        prepared = Some((
                                            media,
                                            next.completed,
                                            effective_context.clone(),
                                        ));
                                        tracing::info!(%video_id, "using prefetched YouTube media");
                                    }
                                }
                            }
                        }
                    }
                }
                match prepared {
                    Some(media) => Ok(media),
                    None => {
                        let mut result = resolver
                            .resolve(
                                &video_id,
                                start_time,
                                &capabilities,
                                preferred_video_codec,
                                effective_context.credential.as_ref(),
                            )
                            .await;
                        // Metadata/extractor requests can fail before a LOAD ever
                        // reaches the decoder (including repeat cache renewal).
                        if result
                            .as_ref()
                            .err()
                            .is_some_and(retryable_resolution_error)
                        {
                            tracing::warn!("retrying YouTube resolution after request failure");
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                            result = resolver
                                .resolve(
                                    &video_id,
                                    start_time,
                                    &capabilities,
                                    preferred_video_codec,
                                    effective_context.credential.as_ref(),
                                )
                                .await;
                        }
                        match result {
                            Ok(media) => Ok((media, std::time::Instant::now(), effective_context)),
                            Err(error) => Err((error, effective_context)),
                        }
                    }
                }
            };
            tokio::pin!(resolution);
            let mut requested_position = start_time;
            let mut autoplay = reload_autoplay.unwrap_or(true);
            let result = loop {
                tokio::select! {
                    biased;
                    changed = cancel.changed() => {
                        if changed.is_err() || *cancel.borrow() { return; }
                    }
                    command = commands.recv() => {
                        match command {
                            None => return,
                            Some(LoungeCommand::Pause) => {
                                autoplay = false;
                                playback.pause().await;
                            }
                            Some(LoungeCommand::Play) => { autoplay = true; }
                            Some(LoungeCommand::Seek(position)) => {
                                requested_position = position.max(0.0);
                            }
                            Some(LoungeCommand::Next)
                                if queue.current_index + 1 >= queue.video_ids.len() => {
                                queue.next_pending = true;
                            }
                            Some(LoungeCommand::UpdatePlaylist { video_ids, list_id, params })
                                if queue.next_pending && queue.updated_index(&video_ids) + 1 < video_ids.len() => {
                                deferred = Some(LoungeCommand::UpdatePlaylist { video_ids, list_id, params });
                                continue 'commands;
                            }
                            Some(LoungeCommand::UpdatePlaylist { video_ids, list_id, params }) => {
                                queue.apply_playlist_update(
                                    video_ids,
                                    list_id,
                                    params,
                                    &mut known_navigation.lock().unwrap(),
                                );
                            }
                            Some(LoungeCommand::RemoteDevices(device_ids)) => {
                                if queue.remote_device_ids != device_ids {
                                    queue.remote_device_ids = device_ids;
                                    queue.generation = queue.generation.wrapping_add(1);
                                    known_navigation.lock().unwrap().retag(queue.generation);
                                }
                            }
                            Some(LoungeCommand::SetPlaylist { video_ids, current_index, current_time, list_id, params, credential: incoming_credential })
                                if video_ids.get(current_index) == Some(&video_id)
                                    && current_index == queue.current_index && current_time == start_time
                                    && incoming_credential == credential
                                    && params == queue.params => {
                                queue.selection_serial = queue.selection_serial.wrapping_add(1);
                                event_selection_serial = queue.selection_serial;
                                queue.apply_playlist_update(
                                    video_ids,
                                    list_id,
                                    params,
                                    &mut known_navigation.lock().unwrap(),
                                );
                                queue.credential = incoming_credential;
                            }
                            Some(LoungeCommand::ReloadCurrent { video_id: retry_id, .. })
                                if retry_id != video_id => {
                                // A delayed recovery for the previous item must not
                                // cancel the user's newer in-flight selection.
                            }
                            Some(command) => {
                                tracing::info!(%video_id, "superseding pending YouTube load");
                                deferred = Some(command);
                                // Dropping resolution also cancels its owned extractor.
                                continue 'commands;
                            }
                        }
                    }
                    result = &mut resolution => break result,
                }
            };
            match result {
                Ok((mut media, completed, context)) => {
                    if queue.video_ids.get(queue.current_index) != Some(&video_id) {
                        tracing::info!(%video_id, "discarding resolved media after selection changed");
                        continue 'commands;
                    }
                    let effective_credential = context.credential.clone();
                    queue.credential = effective_credential.clone();
                    let context_matches_queue = !(context.list_id.is_some()
                        && queue.list_id.is_some()
                        && context.list_id != queue.list_id);
                    if context_matches_queue {
                        queue.list_id = context.list_id.clone().or_else(|| queue.list_id.take());
                        queue.params = context.params.clone().and_then(PlaylistParams::new);
                    }
                    if let Some(context) = queue.current_navigation() {
                        known_navigation
                            .lock()
                            .unwrap()
                            .insert(queue.generation, context);
                    }
                    media.start_time = requested_position;
                    media.autoplay = autoplay;
                    prepare_audio_refresh(
                        &media,
                        &resolver,
                        &video_id,
                        &capabilities,
                        preferred_video_codec,
                        retries_used,
                        effective_credential.as_ref(),
                    );
                    playback_active = true;
                    current_resolution = Some(CurrentResolution {
                        video_id: video_id.clone(),
                        credential: effective_credential.clone(),
                        media: media.clone(),
                        completed,
                        codec: preferred_video_codec,
                    });
                    tracing::info!(%video_id, start_time = requested_position,
                        autoplay, has_ctt = effective_credential.is_some(),
                        "committing resolved YouTube selection");
                    let _ = worker_events
                        .send(WorkerEvent::ContextCommitted(CommittedContext {
                            ownership_epoch: expected_ownership_epoch,
                            selection_serial: event_selection_serial,
                            video_id: video_id.clone(),
                            index: queue.current_index,
                            list_id: queue.list_id.clone(),
                            params: queue.params.clone(),
                            credential: effective_credential,
                        }))
                        .await;
                    playback.load(media).await;
                }
                Err((error, context)) => {
                    playback_active = false;
                    let class = resolution_failure_class(&error);
                    let discovered = known_navigation.lock().unwrap().get(
                        queue.generation,
                        queue.current_index,
                        &video_id,
                    );
                    let failure_credential = context
                        .credential
                        .or_else(|| {
                            discovered
                                .as_ref()
                                .and_then(|context| context.credential.clone())
                        })
                        .or_else(|| credential.clone());
                    queue.credential = failure_credential.clone();
                    failed_resolution = Some(FailedResolution {
                        video_id: video_id.clone(),
                        position: requested_position,
                        credential: failure_credential,
                        class,
                    });
                    tracing::warn!(%video_id, %error, "failed to resolve YouTube video");
                    if class != ResolutionFailureClass::Transient {
                        let (acknowledged, acknowledgement) = tokio::sync::oneshot::channel();
                        if worker_events
                            .send(WorkerEvent::ResolutionFailed {
                                ownership_epoch: expected_ownership_epoch,
                                selection_serial: event_selection_serial,
                                video_id: video_id.clone(),
                                class,
                                acknowledged,
                            })
                            .await
                            .is_ok()
                        {
                            let _ = tokio::time::timeout(
                                std::time::Duration::from_secs(1),
                                acknowledgement,
                            )
                            .await;
                        }
                    }
                    playback.stop().await;
                }
            }
        }

        let candidate = (!stopped).then(|| queue.next_prefetch_key()).flatten();
        if next_resolution.as_ref().map(|p| &p.key) != candidate.as_ref() {
            next_resolution = None;
            if let Some(key) = candidate {
                let resolver = resolver.clone();
                let capabilities = capabilities.clone();
                let codec = PreferredVideoCodec::from_snapshot(&settings.snapshot());
                let known_navigation = known_navigation.clone();
                let current_video_id = queue.video_ids.get(queue.current_index).cloned();
                let current_list_id = queue.list_id.clone();
                let current_params = queue.params.as_ref().map(|value| value.value().to_owned());
                let current_credential = queue.credential.clone();
                let current_index = queue.current_index;
                let remote_device_ids = queue.remote_device_ids.clone();
                let queue_ids = queue.video_ids.clone();
                tracing::info!(video_id = %key.video_id, "prefetching next YouTube media");
                let task_key = key.clone();
                next_resolution = Some(NextResolution {
                    key,
                    task: Some(tokio::spawn(async move {
                        let mut context = NavigationItem {
                            video_id: task_key.video_id.clone(),
                            list_id: task_key.list_id.clone(),
                            index: Some(task_key.index),
                            // Player params belong to the exact navigation
                            // endpoint that supplied them. Reusing the current
                            // song's opaque params for the next item can select
                            // a different playback context.
                            params: None,
                            credential: None,
                        };
                        if let Some(current_video_id) = current_video_id {
                            match resolver
                                .resolve_neighbors(NavigationRequest {
                                    video_id: &current_video_id,
                                    list_id: current_list_id.as_deref(),
                                    index: current_index,
                                    params: current_params.as_deref(),
                                    credential: current_credential.as_ref(),
                                    remote_device_ids: &remote_device_ids,
                                })
                                .await
                            {
                                Ok(neighbors) => {
                                    let mut known = known_navigation.lock().unwrap();
                                    if let Some(mut previous) = neighbors.previous {
                                        if previous.index.is_none() {
                                            previous.index = current_index.checked_sub(1);
                                        }
                                        if previous.index.and_then(|index| queue_ids.get(index))
                                            == Some(&previous.video_id)
                                        {
                                            known.insert(task_key.generation, previous);
                                        }
                                    }
                                    if let Some(mut next) = neighbors.next {
                                        if next.index.is_none() {
                                            next.index = Some(task_key.index);
                                        }
                                        if navigation_matches_prefetch(&next, &task_key) {
                                            if next.list_id.is_none() {
                                                next.list_id = task_key.list_id.clone();
                                            }
                                            context = next.clone();
                                            known.insert(task_key.generation, next);
                                        } else {
                                            tracing::warn!(
                                                expected_video_id = %task_key.video_id,
                                                "YouTube navigation next did not match local queue"
                                            );
                                        }
                                    }
                                }
                                Err(error) => tracing::warn!(
                                    %error,
                                    "YouTube navigation lookup failed; using anonymous prefetch fallback"
                                ),
                            }
                        }
                        let media = resolver
                            .resolve(
                                &task_key.video_id,
                                0.0,
                                &capabilities,
                                codec,
                                context.credential.as_ref(),
                            )
                            .await;
                        PreparedNext {
                            media,
                            completed: std::time::Instant::now(),
                            context,
                        }
                    })),
                });
            }
        }
    }
}

#[cfg(test)]
async fn run_commands(
    commands: mpsc::Receiver<LoungeCommand>,
    resolver: Resolver,
    playback: Arc<dyn PlaybackController>,
    capabilities: vibecast_sdk::PlayerCapabilities,
    settings: AppSettingsReader,
    cancel: watch::Receiver<bool>,
) {
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let drain = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            if let WorkerEvent::ResolutionFailed { acknowledged, .. } = event {
                let _ = acknowledged.send(());
            }
        }
    });
    run_commands_with_events(
        commands,
        resolver,
        playback,
        capabilities,
        settings,
        event_tx,
        cancel,
    )
    .await;
    drain.abort();
}

fn retryable_resolution_error(error: &ResolveError) -> bool {
    match error {
        ResolveError::Http(error) => error.status().map(|s| s.as_u16()) != Some(429),
        ResolveError::Protocol("external audio resolver failed") => true,
        _ => false,
    }
}

fn resolution_failure_class(error: &ResolveError) -> ResolutionFailureClass {
    match error {
        ResolveError::CredentialsRequired(_) => ResolutionFailureClass::CredentialsRequired,
        ResolveError::Unplayable { .. } | ResolveError::NoCompatibleStream => {
            ResolutionFailureClass::Permanent
        }
        ResolveError::Http(_) | ResolveError::Protocol(_) => ResolutionFailureClass::Transient,
    }
}

fn map_resolve_error(error: ResolveError) -> MediaResolveError {
    match error {
        ResolveError::Http(error) => error.into(),
        ResolveError::CredentialsRequired(message) => {
            MediaResolveError::content_unavailable("YOUTUBE_CREDENTIALS_REQUIRED")
                .with_message(message)
        }
        ResolveError::Unplayable { reason, .. } => {
            MediaResolveError::content_unavailable("YOUTUBE_UNPLAYABLE").with_message(reason)
        }
        ResolveError::NoCompatibleStream => {
            MediaResolveError::content_unavailable("NO_COMPATIBLE_STREAM")
        }
        ResolveError::Protocol(message) => {
            MediaResolveError::internal("YOUTUBE_PROTOCOL").with_message(message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn navigation_generation_transition_rejects_obsolete_inserts() {
        let context = |video_id: &str, index: usize, token: &str| NavigationItem {
            video_id: video_id.into(),
            list_id: Some("queue".into()),
            index: Some(index),
            params: Some(format!("params-{video_id}")),
            credential: Some(CredentialTransferToken::new(video_id.into(), token.into())),
        };
        let mut known = KnownNavigation::default();
        known.reset(1);
        known.insert(1, context("A", 0, "token-a"));
        known.retag(2);
        known.insert(1, context("B", 1, "obsolete-token"));

        assert_eq!(
            known
                .get(2, 0, "A")
                .and_then(|item| item.credential)
                .as_ref()
                .map(CredentialTransferToken::value),
            Some("token-a")
        );
        assert!(known.get(2, 1, "B").is_none());
    }

    #[test]
    fn queue_reorder_retains_only_the_active_items_exact_context() {
        let mut queue = QueueState {
            video_ids: vec!["A".into(), "B".into()],
            current_index: 1,
            list_id: Some("queue".into()),
            params: PlaylistParams::new("params-b".into()),
            credential: Some(CredentialTransferToken::new("B".into(), "token-b".into())),
            generation: 4,
            ..Default::default()
        };
        let mut known = KnownNavigation::default();
        known.reset(4);
        known.insert(
            4,
            NavigationItem {
                video_id: "A".into(),
                list_id: Some("queue".into()),
                index: Some(0),
                params: Some("params-a".into()),
                credential: Some(CredentialTransferToken::new("A".into(), "token-a".into())),
            },
        );

        queue.apply_playlist_update(
            vec!["B".into(), "A".into(), "C".into()],
            Some("queue".into()),
            PlaylistParams::new("unrelated-update-params".into()),
            &mut known,
        );

        assert_eq!(queue.current_index, 0);
        assert_eq!(
            queue.params.as_ref().map(PlaylistParams::value),
            Some("params-b")
        );
        assert_eq!(
            queue
                .credential
                .as_ref()
                .map(CredentialTransferToken::value),
            Some("token-b")
        );
        let retained = known.get(queue.generation, 0, "B").unwrap();
        assert_eq!(retained.params.as_deref(), Some("params-b"));
        assert_eq!(
            retained
                .credential
                .as_ref()
                .map(CredentialTransferToken::value),
            Some("token-b")
        );
        assert!(known.get(queue.generation, 1, "A").is_none());
    }

    #[tokio::test]
    async fn cache_refresh_accepts_only_the_same_encoded_object() {
        use vibecast_sdk::MediaSourceRefresh;
        let original = vibecast_sdk::PlaybackStream::cached_url(
            "https://media.test/audio?id=object-A&itag=251&lmt=123&clen=12345&sig=old",
            "audio/webm",
        );
        for (query, mime, accepted) in [
            (
                "id=object-B&itag=251&lmt=123&clen=12345",
                "audio/webm",
                true,
            ),
            ("itag=251&lmt=123&clen=12345", "audio/webm", true),
            (
                "id=object-A&id=object-B&itag=251&lmt=123&clen=12345",
                "audio/webm",
                true,
            ),
            (
                "id=object-A&itag=251&lmt=123&clen=12345&sig=new",
                "audio/webm",
                true,
            ),
            (
                "id=object-A&itag=140&lmt=123&clen=12345",
                "audio/webm",
                false,
            ),
            (
                "id=object-A&itag=251&lmt=456&clen=12345",
                "audio/webm",
                false,
            ),
            (
                "id=object-A&itag=251&lmt=123&clen=99999",
                "audio/webm",
                false,
            ),
            (
                "id=object-A&itag=251&lmt=123&clen=12345",
                "audio/mp4",
                false,
            ),
            ("id=object-A&itag=251&clen=12345", "audio/webm", false),
            ("itag=251&lmt=123&clen=12345&clen=999", "audio/webm", false),
            ("itag=251&lmt=123&lmt=456&clen=12345", "audio/webm", false),
            ("itag=251&lmt=invalid&clen=12345", "audio/webm", false),
            ("itag=251&lmt=123&clen=0", "audio/webm", false),
            ("itag=251&lmt=123&clen=-1", "audio/webm", false),
            (
                "itag=251&lmt=123&clen=18446744073709551616",
                "audio/webm",
                false,
            ),
            (
                "id=object-A&itag=251&itag=140&lmt=123&clen=12345",
                "audio/webm",
                false,
            ),
        ] {
            let (resolver, mut requests) = Resolver::controlled();
            let provider = CurrentAudioRefresh {
                resolver,
                video_id: "A".into(),
                credential: None,
                capabilities: Default::default(),
                codec: PreferredVideoCodec::Auto,
                identity: AudioObjectIdentity::from_stream(&original).unwrap(),
            };
            let task = tokio::spawn(async move { provider.refresh().await });
            let (id, response) = requests.recv().await.unwrap();
            assert_eq!(id, "A");
            let mut media = test_media("A");
            let url = format!("https://other-cdn.test/audio?{query}");
            media
                .streams
                .push(vibecast_sdk::PlaybackStream::cached_url(&url, mime));
            response.send(Ok(media)).unwrap();
            assert_eq!(task.await.unwrap(), accepted.then_some(url));
        }
    }

    #[tokio::test]
    async fn selected_audio_installs_refresh_and_carries_reload_budget() {
        let (resolver, mut requests) = Resolver::controlled();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            Default::default(),
            test_settings(),
            cancelled,
        ));
        tx.send(selection(&["A"], 0)).await.unwrap();
        let (_, response) = requests.recv().await.unwrap();
        let mut media = test_media("A");
        media.streams.push(vibecast_sdk::PlaybackStream::cached_url(
            "https://media.test/audio?id=object-A&itag=251&lmt=123&clen=12345",
            "audio/webm",
        ));
        response.send(Ok(media.clone())).unwrap();
        wait_loads(&player, 1).await;
        let hint = match &player.loaded.lock().unwrap()[0].streams[0].source {
            vibecast_sdk::StreamSource::CachedUrl { cache, .. } => cache.clone(),
            _ => panic!("expected cached audio"),
        };
        assert_eq!(hint.recovery_attempts(), 0);
        let refresh = tokio::spawn(async move { hint.source_refresh().unwrap().refresh().await });
        let (id, response) = requests.recv().await.unwrap();
        assert_eq!(id, "A");
        response.send(Ok(media.clone())).unwrap();
        assert!(refresh.await.unwrap().is_some());
        tx.send(LoungeCommand::ReloadCurrent {
            video_id: "A".into(),
            current_time: 42.0,
            autoplay: false,
            retries_used: 2,
            credential: None,
        })
        .await
        .unwrap();
        let (_, response) = requests.recv().await.unwrap();
        response.send(Ok(media)).unwrap();
        wait_loads(&player, 2).await;
        {
            let loaded = player.loaded.lock().unwrap();
            let vibecast_sdk::StreamSource::CachedUrl { cache, .. } = &loaded[1].streams[0].source
            else {
                panic!()
            };
            assert_eq!(cache.recovery_attempts(), 2);
            assert!(!cache.try_recovery());
            assert_eq!(loaded[1].start_time, 42.0);
            assert!(!loaded[1].autoplay);
        }
        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn cache_refresh_rejects_a_different_resolved_video() {
        use vibecast_sdk::MediaSourceRefresh;
        let stream = vibecast_sdk::PlaybackStream::cached_url(
            "https://media.test/audio?itag=251&lmt=123&clen=12345",
            "audio/webm",
        );
        let (resolver, mut requests) = Resolver::controlled();
        let provider = CurrentAudioRefresh {
            resolver,
            video_id: "A".into(),
            credential: None,
            capabilities: Default::default(),
            codec: PreferredVideoCodec::Auto,
            identity: AudioObjectIdentity::from_stream(&stream).unwrap(),
        };
        let task = tokio::spawn(async move { provider.refresh().await });
        let (requested_id, response) = requests.recv().await.unwrap();
        assert_eq!(requested_id, "A");
        let mut media = test_media("B");
        media.streams.push(stream);
        response.send(Ok(media)).unwrap();
        assert!(task.await.unwrap().is_none());
    }

    #[tokio::test]
    async fn cache_refresh_reuses_the_same_video_credential() {
        use vibecast_sdk::MediaSourceRefresh;
        let stream = vibecast_sdk::PlaybackStream::cached_url(
            "https://media.test/audio?itag=251&lmt=123&clen=12345",
            "audio/webm",
        );
        let (resolver, mut requests, mut contexts) = Resolver::controlled_with_contexts();
        let provider = CurrentAudioRefresh {
            resolver,
            video_id: "A".into(),
            credential: Some(CredentialTransferToken::new(
                "A".into(),
                "refresh-synthetic-token".into(),
            )),
            capabilities: Default::default(),
            codec: PreferredVideoCodec::Auto,
            identity: AudioObjectIdentity::from_stream(&stream).unwrap(),
        };
        let task = tokio::spawn(async move { provider.refresh().await });
        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "A");
        assert_eq!(
            credential.as_ref().map(CredentialTransferToken::value),
            Some("refresh-synthetic-token")
        );
        let (_, response) = requests.recv().await.unwrap();
        let mut media = test_media("A");
        media.streams.push(stream);
        response.send(Ok(media)).unwrap();
        assert!(task.await.unwrap().is_some());
    }
    #[test]
    fn exact_next_selection_can_use_prefetch_but_unrelated_selection_cannot() {
        let queue = QueueState {
            video_ids: vec!["a".into(), "b".into()],
            current_index: 0,
            list_id: Some("queue".into()),
            credential: None,
            next_pending: false,
            ..Default::default()
        };
        let selection = |list: &str, index, time| LoungeCommand::SetPlaylist {
            video_ids: vec!["a".into(), "b".into()],
            current_index: index,
            current_time: time,
            list_id: Some(list.into()),
            params: None,
            credential: None,
        };
        assert!(queue.advances_to_prepared_next(&selection("queue", 1, 0.0)));
        assert!(queue.advances_to_prepared_next(&LoungeCommand::Next));
        assert!(!queue.advances_to_prepared_next(&selection("other", 1, 0.0)));
        assert!(!queue.advances_to_prepared_next(&selection("queue", 0, 0.0)));
        assert!(queue.advances_to_prepared_next(&selection("queue", 1, 42.0)));
        let mut credential_advance = selection("queue", 1, 0.0);
        if let LoungeCommand::SetPlaylist { credential, .. } = &mut credential_advance {
            *credential = Some(CredentialTransferToken::new(
                "b".into(),
                "synthetic-token".into(),
            ));
        }
        assert!(!queue.advances_to_prepared_next(&credential_advance));
    }
    #[test]
    fn prefetch_expires_after_ten_minutes() {
        let now = std::time::Instant::now();
        assert!(prefetched_is_fresh(now));
        assert!(!prefetched_is_fresh(
            now - std::time::Duration::from_secs(601)
        ));
    }

    #[tokio::test]
    async fn dropping_prefetch_aborts_its_task() {
        let task = tokio::spawn(async {
            std::future::pending::<()>().await;
            PreparedNext {
                media: Err(ResolveError::Protocol("unused")),
                completed: std::time::Instant::now(),
                context: NavigationItem {
                    video_id: "unused".into(),
                    list_id: None,
                    index: Some(0),
                    params: None,
                    credential: None,
                },
            }
        });
        let handle = task.abort_handle();
        drop(NextResolution {
            key: PrefetchKey {
                generation: 0,
                list_id: None,
                index: 0,
                video_id: "unused".into(),
            },
            task: Some(task),
        });
        tokio::task::yield_now().await;
        assert!(handle.is_finished());
    }
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSender {
        sent: Mutex<Vec<(String, serde_json::Value)>>,
    }

    #[async_trait]
    impl vibecast_sdk::SenderChannel for RecordingSender {
        async fn send_custom(&self, namespace: &str, data: serde_json::Value) {
            self.sent.lock().unwrap().push((namespace.to_owned(), data));
        }

        async fn broadcast_custom(&self, _namespace: &str, _data: serde_json::Value) {}
    }

    #[derive(Default)]
    struct RecordingPlayback {
        operations: Mutex<Vec<String>>,
        loaded: Mutex<Vec<PlaybackMedia>>,
    }

    #[async_trait]
    impl PlaybackController for RecordingPlayback {
        async fn load(&self, media: PlaybackMedia) {
            self.operations.lock().unwrap().push(format!(
                "load:{}",
                media.content_id.clone().unwrap_or_default()
            ));
            self.loaded.lock().unwrap().push(media);
        }
        async fn play(&self) {
            self.operations.lock().unwrap().push("play".into());
        }
        async fn pause(&self) {
            self.operations.lock().unwrap().push("pause".into());
        }
        async fn seek(&self, position: f64) {
            self.operations
                .lock()
                .unwrap()
                .push(format!("seek:{position}"));
        }
        async fn stop(&self) {
            self.operations.lock().unwrap().push("stop".into());
        }
    }

    fn mdx_test_session() -> (YouTubeSession, AppContext, Arc<RecordingSender>) {
        let sender = Arc::new(RecordingSender::default());
        let ctx = AppContext::new(
            "session",
            "transport",
            APP_IDS[1],
            reqwest::Client::new(),
            vibecast_sdk::ReceiverContext::new(
                "YouTube test",
                "Test",
                "test-device",
                std::path::PathBuf::new(),
            ),
            sender.clone(),
        );
        let (_identity_tx, identity) = watch::channel(Some(LoungeIdentity {
            screen_id: "screen-123".into(),
            device_id: "device-456".into(),
        }));
        let (playback_tx, _playback_rx) = mpsc::channel(1);
        let (output_control_tx, _output_control_rx) = mpsc::channel(1);
        let (volume_tx, _volume_rx) = watch::channel((1.0, false));
        let (ownership_generation_tx, _) = watch::channel(0_u64);
        let (takeover_tx, _takeover_rx) = mpsc::channel(1);
        let (cancel, _cancelled) = watch::channel(false);
        (
            YouTubeSession {
                resolver: Resolver::new(reqwest::Client::new()),
                capabilities: vibecast_sdk::PlayerCapabilities::default(),
                identity,
                mdx_requested: Arc::new(AtomicBool::new(false)),
                legacy_status_scheduled: Arc::new(AtomicBool::new(false)),
                ownership_generation_tx,
                takeover_tx,
                playback_tx,
                output_control_tx,
                volume_tx,
                cancel,
            },
            ctx,
            sender,
        )
    }

    #[tokio::test]
    async fn explicit_mdx_status_request_receives_one_lounge_identity() {
        let (session, ctx, sender) = mdx_test_session();
        session
            .on_sender_connected(&ctx, "sender-one", SenderAttachment::Initial)
            .await;
        session
            .on_sender_connected(&ctx, "sender-two", SenderAttachment::Initial)
            .await;

        let disposition = session
            .on_message(
                &ctx,
                MDX_NAMESPACE,
                &serde_json::json!({"type": "getMdxSessionStatus", "requestId": 73}),
            )
            .await;
        assert_eq!(disposition, MessageDisposition::Handled);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while sender.sent.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let sent = sender.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, MDX_NAMESPACE);
        assert_eq!(sent[0].1["type"], "mdxSessionStatus");
        assert_eq!(sent[0].1["data"]["screenId"], "screen-123");
        assert_eq!(sent[0].1["data"]["deviceId"], "device-456");
        assert_eq!(sent[0].1["requestId"], 73);
    }

    #[tokio::test]
    async fn legacy_mobile_sender_receives_one_fallback_status() {
        let (session, ctx, sender) = mdx_test_session();
        session
            .on_sender_connected(&ctx, "sender-one", SenderAttachment::Initial)
            .await;
        session
            .on_sender_connected(&ctx, "sender-two", SenderAttachment::Initial)
            .await;

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while sender.sent.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let sent = sender.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].1["type"], "mdxSessionStatus");
        assert!(sent[0].1.get("requestId").is_none());
    }

    #[tokio::test]
    async fn stale_owner_never_receives_the_rotated_lounge_identity() {
        let (_session, ctx, sender) = mdx_test_session();
        let (identity_tx, identity) = watch::channel(None);
        let (generation_tx, generation) = watch::channel(0_u64);
        let (_cancel_tx, cancel) = watch::channel(false);

        send_mdx_session_status_when_ready(ctx, identity, cancel, generation, 0, None);
        generation_tx.send(1).unwrap();
        identity_tx
            .send(Some(LoungeIdentity {
                screen_id: "new-owner-screen".into(),
                device_id: "device-456".into(),
            }))
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        assert!(sender.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancelled_session_never_sends_a_ready_lounge_identity() {
        let (session, ctx, sender) = mdx_test_session();
        session.cancel.send_replace(true);
        send_mdx_session_status_when_ready(
            ctx,
            session.identity.clone(),
            session.cancel.subscribe(),
            session.ownership_generation_tx.subscribe(),
            0,
            None,
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(sender.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropped_session_never_sends_a_ready_lounge_identity() {
        let (session, ctx, sender) = mdx_test_session();
        let cancel = session.cancel.subscribe();
        let identity = session.identity.clone();
        let generation = session.ownership_generation_tx.subscribe();
        drop(session);
        send_mdx_session_status_when_ready(ctx, identity, cancel, generation, 0, None);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(sender.sent.lock().unwrap().is_empty());
    }

    #[test]
    fn provider_declares_captured_identity() {
        let manifest = YouTube::new().manifest();
        assert_eq!(manifest.app_key, "youtube");
        assert_eq!(manifest.app_ids, APP_IDS);
        assert_eq!(manifest.display_name, "YouTube");
        assert_eq!(manifest.icon_url, Some(ICON_URL));
        assert!(manifest.namespaces.contains(&MDX_NAMESPACE));
        assert!(manifest.namespaces.contains(&CUSTOM_DATA_NAMESPACE));
        assert_eq!(manifest.settings.settings().len(), 1);
        assert_eq!(
            manifest.settings.settings()[0],
            SettingDescriptor::Choice {
                key: "preferred_video_codec".to_owned(),
                label: "Preferred video codec".to_owned(),
                description: Some(
                    "Choose which video codec YouTube should prefer when available.".to_owned()
                ),
                scope: SettingScope::AppPlayer,
                default: "auto".to_owned(),
                choices: vec![
                    ChoiceOption::new("auto", "Automatic"),
                    ChoiceOption::new("av1", "AV1"),
                    ChoiceOption::new("vp9", "VP9"),
                    ChoiceOption::new("h264", "H.264"),
                ],
            }
        );
    }

    #[tokio::test]
    async fn lounge_controls_are_forwarded_without_media_resolution() {
        let playback = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(8);
        let (_cancel, cancel_rx) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            Resolver::new(reqwest::Client::new()),
            playback.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            vibecast_sdk::AppContext::new(
                "session",
                "transport",
                APP_IDS[0],
                reqwest::Client::new(),
                vibecast_sdk::ReceiverContext::new(
                    "YouTube test",
                    "Test",
                    "test-device",
                    std::path::PathBuf::new(),
                ),
                Arc::new(vibecast_sdk::NoopSenderChannel),
            )
            .settings,
            cancel_rx,
        ));

        tx.send(LoungeCommand::Pause).await.unwrap();
        tx.send(LoungeCommand::Seek(42.0)).await.unwrap();
        tx.send(LoungeCommand::Play).await.unwrap();
        drop(tx);
        worker.await.unwrap();

        assert_eq!(
            *playback.operations.lock().unwrap(),
            ["pause", "seek:42", "play"]
        );
    }

    fn test_settings() -> AppSettingsReader {
        vibecast_sdk::AppContext::new(
            "session",
            "transport",
            APP_IDS[0],
            reqwest::Client::new(),
            vibecast_sdk::ReceiverContext::new("test", "test", "test", std::path::PathBuf::new()),
            Arc::new(vibecast_sdk::NoopSenderChannel),
        )
        .settings
    }

    fn selection(ids: &[&str], index: usize) -> LoungeCommand {
        LoungeCommand::SetPlaylist {
            video_ids: ids.iter().map(|s| s.to_string()).collect(),
            current_index: index,
            current_time: 0.0,
            list_id: Some("test-queue".into()),
            params: None,
            credential: None,
        }
    }

    fn credential_selection(ids: &[&str], index: usize, token: &str) -> LoungeCommand {
        let video_id = ids[index].to_string();
        LoungeCommand::SetPlaylist {
            video_ids: ids.iter().map(|s| s.to_string()).collect(),
            current_index: index,
            current_time: 0.0,
            list_id: Some("test-queue".into()),
            params: None,
            credential: Some(CredentialTransferToken::new(video_id, token.into())),
        }
    }

    #[test]
    fn credential_debug_is_redacted() {
        let credential =
            CredentialTransferToken::new("A".into(), "synthetic-secret-must-not-appear".into());
        let debug = format!("{credential:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("synthetic-secret-must-not-appear"));
    }

    #[tokio::test]
    async fn selection_context_survives_retry_but_not_prefetch_or_stop() {
        let (resolver, mut requests, mut contexts) = Resolver::controlled_with_contexts();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            Default::default(),
            test_settings(),
            cancelled,
        ));

        tx.send(credential_selection(
            &["A", "B"],
            0,
            "first-synthetic-token",
        ))
        .await
        .unwrap();
        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "A");
        assert_eq!(
            credential.as_ref().map(CredentialTransferToken::value),
            Some("first-synthetic-token")
        );
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("A")))
            .unwrap();
        wait_loads(&player, 1).await;

        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "B");
        assert!(
            credential.is_none(),
            "the current video's CTT must not reach prefetch"
        );
        let (_, old_prefetch) = requests.recv().await.unwrap();

        tx.send(credential_selection(
            &["A", "B"],
            0,
            "second-synthetic-token",
        ))
        .await
        .unwrap();
        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "A");
        assert_eq!(
            credential.as_ref().map(CredentialTransferToken::value),
            Some("second-synthetic-token")
        );
        let (_, replacement) = requests.recv().await.unwrap();
        assert!(old_prefetch.is_closed());
        replacement.send(Ok(test_media("A"))).unwrap();
        wait_loads(&player, 2).await;

        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "B");
        assert!(credential.is_none());
        let (_, new_prefetch) = requests.recv().await.unwrap();

        tx.send(LoungeCommand::Stop).await.unwrap();
        tx.send(selection(&["A"], 0)).await.unwrap();
        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "A");
        assert!(credential.is_none(), "Stop must discard the prior CTT");
        let (_, anonymous) = requests.recv().await.unwrap();
        assert!(new_prefetch.is_closed());
        anonymous.send(Ok(test_media("A"))).unwrap();
        wait_loads(&player, 3).await;

        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    fn test_media(id: &str) -> PlaybackMedia {
        let mut media = PlaybackMedia::new("session", vec![], vibecast_sdk::StreamType::Buffered);
        media.content_id = Some(id.into());
        media
    }

    async fn wait_loads(player: &RecordingPlayback, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while player.loaded.lock().unwrap().len() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn tail_next_preserves_pending_load_and_extension_advances() {
        for finish_current_first in [true, false] {
            let (resolver, mut requests) = Resolver::controlled();
            let player = Arc::new(RecordingPlayback::default());
            let (tx, rx) = mpsc::channel(16);
            let (cancel, cancelled) = watch::channel(false);
            let worker = tokio::spawn(run_commands(
                rx,
                resolver,
                player.clone(),
                vibecast_sdk::PlayerCapabilities::default(),
                test_settings(),
                cancelled,
            ));
            tx.send(selection(&["A"], 0)).await.unwrap();
            let (_, pending) = requests.recv().await.unwrap();
            let mut pending = Some(pending);
            tx.send(LoungeCommand::Next).await.unwrap();
            if finish_current_first {
                pending.take().unwrap().send(Ok(test_media("A"))).unwrap();
                wait_loads(&player, 1).await;
            }
            tx.send(LoungeCommand::UpdatePlaylist {
                video_ids: vec!["A".into(), "B".into()],
                list_id: Some("test-queue".into()),
                params: None,
            })
            .await
            .unwrap();
            let (id, next) = requests.recv().await.unwrap();
            assert_eq!(id, "B");
            if let Some(pending) = pending {
                assert!(pending.is_closed());
            }
            next.send(Ok(test_media("B"))).unwrap();
            wait_loads(&player, if finish_current_first { 2 } else { 1 }).await;
            cancel.send(true).unwrap();
            worker.await.unwrap();
        }
    }

    #[tokio::test]
    async fn next_prefetch_uses_derived_ctt_and_keeps_it_for_reload() {
        let (resolver, mut requests, mut contexts, mut navigation) =
            Resolver::controlled_with_navigation();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));

        tx.send(selection(&["A", "B"], 0)).await.unwrap();
        assert_eq!(contexts.recv().await.unwrap(), ("A".into(), None));
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("A")))
            .unwrap();
        wait_loads(&player, 1).await;

        let (navigation_id, navigation_response) = navigation.recv().await.unwrap();
        assert_eq!(navigation_id, "A");
        navigation_response
            .send(Ok(resolver::NavigationNeighbors {
                previous: None,
                next: Some(NavigationItem {
                    video_id: "B".into(),
                    list_id: Some("test-queue".into()),
                    index: Some(1),
                    params: Some("next-private-params".into()),
                    credential: Some(CredentialTransferToken::new(
                        "B".into(),
                        "next-synthetic-ctt".into(),
                    )),
                }),
            }))
            .unwrap();
        let (prefetch_id, prefetch_credential) = contexts.recv().await.unwrap();
        assert_eq!(prefetch_id, "B");
        assert_eq!(
            prefetch_credential
                .as_ref()
                .map(CredentialTransferToken::value),
            Some("next-synthetic-ctt")
        );
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("B-prefetched")))
            .unwrap();
        tokio::task::yield_now().await;

        tx.send(LoungeCommand::Next).await.unwrap();
        wait_loads(&player, 2).await;
        assert_eq!(
            player.loaded.lock().unwrap()[1].content_id.as_deref(),
            Some("B-prefetched")
        );

        tx.send(LoungeCommand::ReloadCurrent {
            video_id: "B".into(),
            current_time: 12.0,
            autoplay: true,
            retries_used: 0,
            credential: None,
        })
        .await
        .unwrap();
        let (reload_id, reload_credential) = contexts.recv().await.unwrap();
        assert_eq!(reload_id, "B");
        assert_eq!(
            reload_credential
                .as_ref()
                .map(CredentialTransferToken::value),
            Some("next-synthetic-ctt")
        );
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("B-reloaded")))
            .unwrap();
        wait_loads(&player, 3).await;
        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn queue_enrichment_during_next_keeps_the_exact_derived_ctt() {
        let (resolver, mut requests, mut contexts, mut navigation) =
            Resolver::controlled_with_navigation();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));

        tx.send(selection(&["A", "B"], 0)).await.unwrap();
        assert_eq!(contexts.recv().await.unwrap(), ("A".into(), None));
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("A")))
            .unwrap();
        wait_loads(&player, 1).await;
        let (navigation_id, navigation_response) = navigation.recv().await.unwrap();
        assert_eq!(navigation_id, "A");

        tx.send(LoungeCommand::Next).await.unwrap();
        tx.send(LoungeCommand::UpdatePlaylist {
            video_ids: vec!["A".into(), "B".into(), "C".into()],
            list_id: Some("test-queue".into()),
            params: None,
        })
        .await
        .unwrap();
        navigation_response
            .send(Ok(resolver::NavigationNeighbors {
                previous: None,
                next: Some(NavigationItem {
                    video_id: "B".into(),
                    list_id: Some("test-queue".into()),
                    index: Some(1),
                    params: Some("params-b".into()),
                    credential: Some(CredentialTransferToken::new("B".into(), "token-b".into())),
                }),
            }))
            .unwrap();
        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "B");
        assert_eq!(
            credential.as_ref().map(CredentialTransferToken::value),
            Some("token-b")
        );
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("B-prefetched")))
            .unwrap();
        wait_loads(&player, 2).await;
        assert_eq!(
            player.loaded.lock().unwrap()[1].content_id.as_deref(),
            Some("B-prefetched")
        );

        tx.send(LoungeCommand::ReloadCurrent {
            video_id: "B".into(),
            current_time: 8.0,
            autoplay: true,
            retries_used: 0,
            credential: None,
        })
        .await
        .unwrap();
        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "B");
        assert_eq!(
            credential.as_ref().map(CredentialTransferToken::value),
            Some("token-b")
        );
        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn positioned_next_selection_awaits_context_and_reuses_exact_media() {
        let (resolver, mut requests, mut contexts, mut navigation) =
            Resolver::controlled_with_navigation();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands_with_events(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            event_tx,
            cancelled,
        ));

        tx.send(selection(&["A", "B"], 0)).await.unwrap();
        assert_eq!(contexts.recv().await.unwrap(), ("A".into(), None));
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("A")))
            .unwrap();
        wait_loads(&player, 1).await;
        assert!(matches!(
            event_rx.recv().await,
            Some(WorkerEvent::ContextCommitted(CommittedContext { video_id, .. }))
                if video_id == "A"
        ));
        let (_, navigation_response) = navigation.recv().await.unwrap();

        tx.send(LoungeCommand::SetPlaylist {
            video_ids: vec!["A".into(), "B".into()],
            current_index: 1,
            current_time: 17.5,
            list_id: Some("test-queue".into()),
            params: PlaylistParams::new("sender-params-b".into()),
            credential: None,
        })
        .await
        .unwrap();
        navigation_response
            .send(Ok(resolver::NavigationNeighbors {
                previous: None,
                next: Some(NavigationItem {
                    video_id: "B".into(),
                    list_id: Some("test-queue".into()),
                    index: Some(1),
                    params: Some("endpoint-params-b".into()),
                    credential: Some(CredentialTransferToken::new("B".into(), "token-b".into())),
                }),
            }))
            .unwrap();
        let (id, credential) = contexts.recv().await.unwrap();
        assert_eq!(id, "B");
        assert_eq!(
            credential.as_ref().map(CredentialTransferToken::value),
            Some("token-b")
        );
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("B-prefetched")))
            .unwrap();
        wait_loads(&player, 2).await;
        {
            let loaded = player.loaded.lock().unwrap();
            assert_eq!(loaded[1].content_id.as_deref(), Some("B-prefetched"));
            assert_eq!(loaded[1].start_time, 17.5);
            assert!(requests.try_recv().is_err());
        }
        match event_rx.recv().await {
            Some(WorkerEvent::ContextCommitted(CommittedContext {
                video_id,
                params,
                credential,
                ..
            })) => {
                assert_eq!(video_id, "B");
                assert_eq!(
                    params.as_ref().map(PlaylistParams::value),
                    Some("sender-params-b")
                );
                assert_eq!(
                    credential.as_ref().map(CredentialTransferToken::value),
                    Some("token-b")
                );
            }
            _ => panic!("missing B context event"),
        }

        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn credentials_required_does_not_retry_until_context_changes() {
        let (resolver, mut requests, mut contexts) = Resolver::controlled_with_contexts();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));

        tx.send(selection(&["adult"], 0)).await.unwrap();
        assert_eq!(contexts.recv().await.unwrap(), ("adult".into(), None));
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Err(ResolveError::CredentialsRequired("login".into())))
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !player
                .operations
                .lock()
                .unwrap()
                .iter()
                .any(|op| op == "stop")
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tx.send(LoungeCommand::Play).await.unwrap();
        tokio::task::yield_now().await;
        assert!(requests.try_recv().is_err());

        tx.send(credential_selection(&["adult"], 0, "fresh-synthetic-ctt"))
            .await
            .unwrap();
        let (_, credential) = contexts.recv().await.unwrap();
        assert_eq!(
            credential.as_ref().map(CredentialTransferToken::value),
            Some("fresh-synthetic-ctt")
        );
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("adult")))
            .unwrap();
        wait_loads(&player, 1).await;
        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn failed_repeat_resolution_retries_then_play_resolves_instead_of_unpausing() {
        let (resolver, mut requests) = Resolver::controlled();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));
        tx.send(selection(&["A"], 0)).await.unwrap();
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Err(ResolveError::Protocol(
                "external audio resolver failed",
            )))
            .unwrap();
        let (id, retry) = requests.recv().await.unwrap();
        assert_eq!(id, "A");
        tx.send(LoungeCommand::Seek(66.0)).await.unwrap();
        retry
            .send(Err(ResolveError::Protocol(
                "external audio resolver failed",
            )))
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !player.operations.lock().unwrap().contains(&"stop".into()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            requests.try_recv().is_err(),
            "automatic resolution retry is bounded"
        );
        tx.send(LoungeCommand::Play).await.unwrap();
        let (id, manual) = requests.recv().await.unwrap();
        assert_eq!(id, "A");
        manual.send(Ok(test_media("A-fresh"))).unwrap();
        wait_loads(&player, 1).await;
        assert_eq!(player.loaded.lock().unwrap()[0].start_time, 66.0);
        assert_eq!(*player.operations.lock().unwrap(), ["stop", "load:A-fresh"]);
        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn stop_cancels_resolution_retry() {
        let (resolver, mut requests) = Resolver::controlled();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (_cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));
        tx.send(selection(&["A"], 0)).await.unwrap();
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Err(ResolveError::Protocol(
                "external audio resolver failed",
            )))
            .unwrap();
        let (_, retry) = requests.recv().await.unwrap();
        tx.send(LoungeCommand::Stop).await.unwrap();
        tx.send(LoungeCommand::Play).await.unwrap();
        drop(tx);
        worker.await.unwrap();
        assert!(retry.is_closed());
        assert!(requests.try_recv().is_err());
        assert!(player.loaded.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reload_resolves_fresh_media_and_preserves_position_and_controls() {
        let (resolver, mut requests) = Resolver::controlled();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));
        tx.send(selection(&["A", "B"], 0)).await.unwrap();
        let (_, first) = requests.recv().await.unwrap();
        first.send(Ok(test_media("A-old"))).unwrap();
        wait_loads(&player, 1).await;
        let (id, prepared) = requests.recv().await.unwrap();
        assert_eq!(id, "B");
        tx.send(LoungeCommand::ReloadCurrent {
            video_id: "A".into(),
            current_time: 66.0,
            autoplay: false,
            retries_used: 0,
            credential: None,
        })
        .await
        .unwrap();
        let (id, retry) = requests.recv().await.unwrap();
        assert_eq!(id, "A");
        assert!(!prepared.is_closed());
        prepared.send(Ok(test_media("B"))).unwrap();
        retry.send(Ok(test_media("A-fresh"))).unwrap();
        wait_loads(&player, 2).await;
        {
            let loaded = player.loaded.lock().unwrap();
            assert_eq!(loaded[1].content_id.as_deref(), Some("A-fresh"));
            assert_eq!(loaded[1].start_time, 66.0);
            assert!(!loaded[1].autoplay);
        }
        tx.send(LoungeCommand::ReloadCurrent {
            video_id: "A".into(),
            current_time: 66.0,
            autoplay: true,
            retries_used: 0,
            credential: None,
        })
        .await
        .unwrap();
        let (_, manual) = requests.recv().await.unwrap();
        tx.send(LoungeCommand::Pause).await.unwrap();
        tx.send(LoungeCommand::Seek(80.0)).await.unwrap();
        manual.send(Ok(test_media("A-manual"))).unwrap();
        wait_loads(&player, 3).await;
        {
            let loaded = player.loaded.lock().unwrap();
            assert_eq!(loaded[2].start_time, 80.0);
            assert!(!loaded[2].autoplay);
        }
        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn reload_cannot_override_stop_or_new_selection() {
        let (resolver, mut requests) = Resolver::controlled();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));
        tx.send(selection(&["A"], 0)).await.unwrap();
        requests
            .recv()
            .await
            .unwrap()
            .1
            .send(Ok(test_media("A")))
            .unwrap();
        wait_loads(&player, 1).await;
        let reload = LoungeCommand::ReloadCurrent {
            video_id: "A".into(),
            current_time: 66.0,
            autoplay: true,
            retries_used: 0,
            credential: None,
        };
        tx.send(reload.clone()).await.unwrap();
        let (_, retry) = requests.recv().await.unwrap();
        tx.send(LoungeCommand::Stop).await.unwrap();
        tx.send(reload.clone()).await.unwrap(); // queued obsolete retry
        tx.send(selection(&["B"], 0)).await.unwrap();
        let (id, replacement) = requests.recv().await.unwrap();
        assert_eq!(id, "B");
        assert!(retry.is_closed());
        tx.send(reload.clone()).await.unwrap(); // also stale while B is resolving
        replacement.send(Ok(test_media("B"))).unwrap();
        wait_loads(&player, 2).await;
        tx.send(reload).await.unwrap(); // wrong selection must be ignored
        tx.send(LoungeCommand::Pause).await.unwrap();
        drop(tx);
        worker.await.unwrap();
        assert!(requests.try_recv().is_err());
        assert_eq!(player.loaded.lock().unwrap().len(), 2);
        drop(cancel);
    }

    #[tokio::test]
    async fn latest_selection_preserves_controls_and_prepared_next() {
        let (resolver, mut requests) = Resolver::controlled();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));
        tx.send(selection(&["A"], 0)).await.unwrap();
        let (id, old) = requests.recv().await.unwrap();
        assert_eq!(id, "A");
        tx.send(selection(&["B"], 0)).await.unwrap();
        let (id, middle) = requests.recv().await.unwrap();
        assert_eq!(id, "B");
        assert!(old.is_closed());
        tx.send(selection(&["C", "D"], 0)).await.unwrap();
        let (id, latest) = requests.recv().await.unwrap();
        assert_eq!(id, "C");
        assert!(middle.is_closed());
        tx.send(selection(&["C", "D"], 0)).await.unwrap(); // duplicate, no restart
        tx.send(LoungeCommand::Pause).await.unwrap();
        tx.send(LoungeCommand::Seek(42.0)).await.unwrap();
        latest.send(Ok(test_media("C"))).unwrap();
        wait_loads(&player, 1).await;
        {
            let loaded = player.loaded.lock().unwrap();
            assert_eq!(loaded[0].content_id.as_deref(), Some("C"));
            assert_eq!(loaded[0].start_time, 42.0);
            assert!(!loaded[0].autoplay);
        }
        let (id, prepared) = requests.recv().await.unwrap();
        assert_eq!(id, "D");
        prepared.send(Ok(test_media("D"))).unwrap();
        tx.send(selection(&["C", "D"], 1)).await.unwrap();
        wait_loads(&player, 2).await;
        assert!(requests.try_recv().is_err(), "Next must reuse preparation");
        assert_eq!(
            player.loaded.lock().unwrap()[1].content_id.as_deref(),
            Some("D")
        );
        cancel.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn queue_reorder_preserves_active_title_and_next_during_or_after_resolution() {
        for during_resolution in [false, true] {
            let (resolver, mut requests) = Resolver::controlled();
            let player = Arc::new(RecordingPlayback::default());
            let (tx, rx) = mpsc::channel(16);
            let (cancel, cancelled) = watch::channel(false);
            let worker = tokio::spawn(run_commands(
                rx,
                resolver,
                player.clone(),
                Default::default(),
                test_settings(),
                cancelled,
            ));
            tx.send(selection(&["A", "B"], 1)).await.unwrap();
            let (id, response) = requests.recv().await.unwrap();
            assert_eq!(id, "B");
            let mut response = Some(response);
            if !during_resolution {
                response.take().unwrap().send(Ok(test_media("B"))).unwrap();
                wait_loads(&player, 1).await;
            }
            tx.send(LoungeCommand::UpdatePlaylist {
                video_ids: vec!["B".into(), "A".into()],
                list_id: Some("test-queue".into()),
                params: None,
            })
            .await
            .unwrap();
            if let Some(response) = response {
                response.send(Ok(test_media("B"))).unwrap();
            }
            wait_loads(&player, 1).await;
            let (id, prepared) =
                tokio::time::timeout(std::time::Duration::from_secs(2), requests.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(id, "A");
            prepared.send(Ok(test_media("A"))).unwrap();
            tx.send(LoungeCommand::Next).await.unwrap();
            wait_loads(&player, 2).await;
            assert_eq!(
                player.loaded.lock().unwrap()[1].content_id.as_deref(),
                Some("A")
            );
            cancel.send(true).unwrap();
            worker.await.unwrap();
        }
    }

    #[tokio::test]
    async fn repeat_reuses_current_media_and_preserves_prepared_next() {
        let (resolver, mut requests) = Resolver::controlled();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(16);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));
        tx.send(selection(&["A", "B"], 0)).await.unwrap();
        let (id, resolved) = requests.recv().await.unwrap();
        assert_eq!(id, "A");
        resolved.send(Ok(test_media("A"))).unwrap();
        wait_loads(&player, 1).await;
        let (id, next) = requests.recv().await.unwrap();
        assert_eq!(id, "B");
        for count in 2..=3 {
            tx.send(LoungeCommand::RepeatCurrent).await.unwrap();
            wait_loads(&player, count).await;
            let loaded = player.loaded.lock().unwrap();
            let replay = loaded.last().unwrap();
            assert_eq!(replay.content_id.as_deref(), Some("A"));
            assert_eq!(replay.start_time, 0.0);
            assert!(replay.autoplay);
            assert!(
                !next.is_closed(),
                "repeat must preserve next-item preparation"
            );
            assert!(
                requests.try_recv().is_err(),
                "repeat must not resolve again"
            );
        }
        next.send(Ok(test_media("B"))).unwrap();
        tx.send(LoungeCommand::Next).await.unwrap();
        wait_loads(&player, 4).await;
        assert_eq!(
            player.loaded.lock().unwrap()[3].content_id.as_deref(),
            Some("B")
        );
        assert!(
            requests.try_recv().is_err(),
            "manual Next must still reuse preparation"
        );
        tx.send(LoungeCommand::RepeatCurrent).await.unwrap();
        wait_loads(&player, 5).await;
        assert_eq!(
            player.loaded.lock().unwrap()[4].content_id.as_deref(),
            Some("B")
        );
        assert!(requests.try_recv().is_err());
        // Stop invalidates reuse even if a late repeat command arrives.
        tx.send(LoungeCommand::Stop).await.unwrap();
        tx.send(LoungeCommand::RepeatCurrent).await.unwrap();
        let (id, pending) = requests.recv().await.unwrap();
        assert_eq!(id, "B");
        cancel.send(true).unwrap();
        worker.await.unwrap();
        assert!(pending.is_closed());
    }

    #[test]
    fn repeat_cache_expires_without_renewal_and_requires_matching_selection_and_codec() {
        let id = "A".to_string();
        let mut media = test_media("A");
        media.start_time = 42.0;
        media.autoplay = false;
        let mut current = CurrentResolution {
            video_id: id.clone(),
            credential: None,
            media,
            completed: std::time::Instant::now(),
            codec: PreferredVideoCodec::Auto,
        };
        let original_age = current.completed;
        for _ in 0..2 {
            let replay = current
                .replay(Some(&id), PreferredVideoCodec::Auto, None)
                .unwrap();
            assert_eq!(replay.start_time, 0.0);
            assert!(replay.autoplay);
            assert_eq!(current.completed, original_age);
        }
        let credential = CredentialTransferToken::new("A".into(), "synthetic-token".into());
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::Auto, Some(&credential))
            .is_none());
        assert!(current
            .replay(Some(&"B".into()), PreferredVideoCodec::Auto, None)
            .is_none());
        assert!(current
            .replay(None, PreferredVideoCodec::Auto, None)
            .is_none());
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::H264, None)
            .is_none());
        current.completed = std::time::Instant::now() - std::time::Duration::from_secs(601);
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::Auto, None)
            .is_none());
    }

    #[test]
    fn only_complete_current_audio_replays_after_url_age_limit() {
        let id = "A".to_string();
        let mut media = test_media("A");
        let stream =
            vibecast_sdk::PlaybackStream::cached_url("https://example.test/audio", "audio/mp4");
        let vibecast_sdk::StreamSource::CachedUrl { cache, .. } = &stream.source else {
            unreachable!()
        };
        let hint = cache.clone();
        media.streams = vec![stream];
        let current = CurrentResolution {
            video_id: id.clone(),
            credential: None,
            media,
            completed: std::time::Instant::now() - std::time::Duration::from_secs(3600),
            codec: PreferredVideoCodec::Auto,
        };
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::Auto, None)
            .is_none());
        hint.mark_complete();
        let replay = current
            .replay(Some(&id), PreferredVideoCodec::Auto, None)
            .unwrap();
        assert_eq!(replay.start_time, 0.0);
        assert!(replay.autoplay);
        assert!(current
            .replay(Some(&"B".into()), PreferredVideoCodec::Auto, None)
            .is_none());
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::H264, None)
            .is_none());
        hint.invalidate();
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::Auto, None)
            .is_none());
    }

    #[tokio::test]
    async fn stop_and_disconnect_cancel_pending_resolution() {
        let (resolver, mut requests) = Resolver::controlled();
        let player = Arc::new(RecordingPlayback::default());
        let (tx, rx) = mpsc::channel(8);
        let (cancel, cancelled) = watch::channel(false);
        let worker = tokio::spawn(run_commands(
            rx,
            resolver,
            player.clone(),
            vibecast_sdk::PlayerCapabilities::default(),
            test_settings(),
            cancelled,
        ));
        tx.send(selection(&["A"], 0)).await.unwrap();
        let (_, old) = requests.recv().await.unwrap();
        tx.send(LoungeCommand::Stop).await.unwrap();
        tx.send(selection(&["B"], 0)).await.unwrap();
        let (_, latest) = requests.recv().await.unwrap();
        assert!(old
            .send(Err(ResolveError::Protocol("late failure")))
            .is_err());
        cancel.send(true).unwrap();
        worker.await.unwrap();
        assert!(latest.is_closed());
        assert!(player.loaded.lock().unwrap().is_empty());
        assert_eq!(*player.operations.lock().unwrap(), ["stop"]);
    }
}
