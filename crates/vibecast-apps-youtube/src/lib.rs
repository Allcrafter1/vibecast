//! Bundled YouTube app using the captured MDX/Lounge control flow.

#![forbid(unsafe_code)]

mod lounge;
mod resolver;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use vibecast_sdk::{
    AppContext, AppManifest, AppProvider, AppSession, AppSettingsReader, AppSettingsSchema,
    ChoiceOption, LaunchCredentials, LaunchError, LoadRequest, MediaResolveError, OutputControl,
    PlaybackController, PlaybackMedia, PlaybackState, SettingDescriptor, SettingScope,
};

use lounge::{LoungeCommand, LoungeConnection, LoungeIdentity};
use resolver::{PreferredVideoCodec, ResolveError, Resolver, PREFERRED_VIDEO_CODEC_KEY};

const APP_IDS: &[&str] = &["233637DE", "2DB7CC49"];
const MDX_NAMESPACE: &str = "urn:x-cast:com.google.youtube.mdx";
const CUSTOM_DATA_NAMESPACE: &str = "urn:x-cast:com.google.cast.customdata";
const ICON_URL: &str = "https://www.gstatic.com/youtube/img/branding/favicon/favicon_144x144.png";

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
        let (cancel, _) = watch::channel(false);
        tokio::spawn(run_commands(
            command_rx,
            resolver,
            playback,
            capabilities,
            ctx.settings.clone(),
            cancel.subscribe(),
        ));
        tokio::spawn(run_lounge(
            ctx.http.clone(),
            ctx.receiver.clone(),
            command_tx,
            playback_rx,
            output_control_rx,
            identity_tx,
            volume_rx,
            cancel.subscribe(),
        ));

        Ok(Arc::new(YouTubeSession {
            resolver: Resolver::new(ctx.http.clone()),
            capabilities: ctx.receiver.capabilities.clone(),
            identity,
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
        self.resolver
            .resolve(
                &video_id,
                request.current_time,
                &self.capabilities,
                preferred_video_codec,
            )
            .await
            .map_err(map_resolve_error)
    }

    async fn on_sender_connected(&self, ctx: &AppContext, _sender_id: &str) {
        let ctx = ctx.clone();
        let mut identity = self.identity.clone();
        let mut cancel = self.cancel.subscribe();
        tokio::spawn(async move {
            loop {
                let current_identity = { identity.borrow().clone() };
                if let Some(identity) = current_identity {
                    ctx.send_custom(
                        MDX_NAMESPACE,
                        serde_json::json!({
                            "type": "mdxSessionStatus",
                            "data": {
                                "screenId": identity.screen_id,
                                "deviceId": identity.device_id,
                            }
                        }),
                    )
                    .await;
                    return;
                }

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
                }
            }
        });
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

    fn allows_sender_reconnect_grace(&self) -> bool {
        // Lounge/cache can bridge a brief control-connection loss, but the
        // runtime must stop playback if control is not restored within 10s.
        true
    }

    async fn on_output_control(&self, _ctx: &AppContext, control: OutputControl) {
        let _ = self.output_control_tx.try_send(control);
    }

    async fn on_stop(&self, _ctx: &AppContext) {
        let _ = self.cancel.send(true);
    }
}

async fn run_lounge(
    http: reqwest::Client,
    receiver: vibecast_sdk::ReceiverContext,
    command_tx: mpsc::Sender<LoungeCommand>,
    playback_rx: mpsc::Receiver<PlaybackState>,
    output_control_rx: mpsc::Receiver<OutputControl>,
    identity_tx: watch::Sender<Option<LoungeIdentity>>,
    volume_rx: watch::Receiver<(f64, bool)>,
    mut cancel: watch::Receiver<bool>,
) {
    let lounge = loop {
        let establish = LoungeConnection::establish(http.clone(), &receiver);
        let result = tokio::select! {
            result = establish => result,
            result = cancel.changed() => {
                if result.is_err() || *cancel.borrow() {
                    return;
                }
                continue;
            }
        };
        match result {
            Ok(lounge) => break lounge,
            Err(error) => tracing::warn!(%error, "YouTube Lounge pairing failed; retrying"),
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
            result = cancel.changed() => {
                if result.is_err() || *cancel.borrow() {
                    return;
                }
            }
        }
    };

    let _ = identity_tx.send(Some(lounge.identity()));
    lounge
        .with_volume(volume_rx)
        .run(command_tx, playback_rx, output_control_rx, cancel)
        .await;
}

#[derive(Default)]
struct QueueState {
    video_ids: Vec<String>,
    current_index: usize,
    list_id: Option<String>,
    next_pending: bool,
}

impl QueueState {
    fn advances_to_prepared_next(&self, command: &LoungeCommand) -> bool {
        match command {
            LoungeCommand::Next => true,
            LoungeCommand::SetPlaylist {
                video_ids,
                current_index,
                current_time,
                list_id,
            } => {
                self.list_id.is_some()
                    && self.list_id == *list_id
                    && *current_time == 0.0
                    && *current_index == self.current_index + 1
                    && video_ids.get(*current_index).is_some()
                    && video_ids.get(*current_index) == self.video_ids.get(self.current_index + 1)
            }
            _ => false,
        }
    }
}

// One session-local speculative result; never written to disk. Dropping it
// cancels the async task, so obsolete results cannot start playback.
struct NextResolution {
    video_id: String,
    task:
        Option<tokio::task::JoinHandle<(Result<PlaybackMedia, ResolveError>, std::time::Instant)>>,
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

// Keep only the current stream description, not audio bytes. Replaying must not
// renew its age: signed URLs expire even when the same title keeps repeating.
struct CurrentResolution {
    video_id: String,
    media: PlaybackMedia,
    completed: std::time::Instant,
    codec: PreferredVideoCodec,
}

impl CurrentResolution {
    fn replay(
        &self,
        video_id: Option<&String>,
        codec: PreferredVideoCodec,
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

async fn run_commands(
    mut commands: mpsc::Receiver<LoungeCommand>,
    resolver: Resolver,
    playback: Arc<dyn PlaybackController>,
    capabilities: vibecast_sdk::PlayerCapabilities,
    settings: AppSettingsReader,
    mut cancel: watch::Receiver<bool>,
) {
    let mut queue = QueueState::default();
    let mut next_resolution: Option<NextResolution> = None;
    let mut current_resolution: Option<CurrentResolution> = None;
    let mut playback_active = false;
    let mut stopped = false;
    let mut failed_resolution: Option<(String, f64)> = None;
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
        let reload_autoplay = match &command {
            LoungeCommand::ReloadCurrent { autoplay, .. } => Some(*autoplay),
            _ => None,
        };
        if repeating && playback_active {
            let codec = PreferredVideoCodec::from_snapshot(&settings.snapshot());
            if let Some(media) = current_resolution
                .as_ref()
                .and_then(|current| current.replay(queue.video_ids.get(queue.current_index), codec))
            {
                let cached_audio = media.streams.iter().any(|stream| matches!(
                    &stream.source, vibecast_sdk::StreamSource::CachedUrl { cache, .. } if cache.is_complete()
                ));
                tracing::info!(cached_audio, "reusing current YouTube media for repeat");
                playback.load(media).await;
                continue;
            }
        }

        // New-queue selections keep their established load/coalescing path.
        // Only explicit/automatic Next consumes speculative media.
        let use_prefetch = queue.advances_to_prepared_next(&command);
        if let LoungeCommand::SetPlaylist {
            current_index,
            video_ids,
            current_time,
            list_id,
        } = &command
        {
            tracing::info!(
                index = *current_index,
                queue_len = video_ids.len(),
                start = *current_time,
                same_queue = queue.list_id.is_some() && queue.list_id == *list_id,
                use_prefetch,
                "YouTube playlist selection"
            );
        }
        let load = match command {
            LoungeCommand::ReloadCurrent {
                video_id,
                current_time,
                ..
            } => {
                if stopped || queue.video_ids.get(queue.current_index) != Some(&video_id) {
                    continue;
                }
                Some((video_id, current_time))
            }
            LoungeCommand::RepeatCurrent => {
                queue.next_pending = false;
                queue
                    .video_ids
                    .get(queue.current_index)
                    .cloned()
                    .map(|video_id| (video_id, 0.0))
            }
            LoungeCommand::SetPlaylist {
                video_ids,
                current_index,
                current_time,
                list_id,
            } => {
                queue.video_ids = video_ids;
                queue.current_index = current_index.min(queue.video_ids.len().saturating_sub(1));
                queue.list_id = list_id;
                queue.next_pending = false;
                queue
                    .video_ids
                    .get(queue.current_index)
                    .cloned()
                    .map(|video_id| (video_id, current_time))
            }
            LoungeCommand::UpdatePlaylist { video_ids, list_id } => {
                queue.video_ids = video_ids;
                queue.list_id = list_id;
                if queue.next_pending && queue.current_index + 1 < queue.video_ids.len() {
                    queue.current_index += 1;
                    queue.next_pending = false;
                    queue
                        .video_ids
                        .get(queue.current_index)
                        .cloned()
                        .map(|video_id| (video_id, 0.0))
                } else {
                    None
                }
            }
            LoungeCommand::Next => {
                if queue.current_index + 1 < queue.video_ids.len() {
                    queue.current_index += 1;
                    queue
                        .video_ids
                        .get(queue.current_index)
                        .cloned()
                        .map(|video_id| (video_id, 0.0))
                } else {
                    queue.next_pending = true;
                    None
                }
            }
            LoungeCommand::Previous => {
                if queue.current_index > 0 {
                    queue.current_index -= 1;
                }
                queue.next_pending = false;
                queue
                    .video_ids
                    .get(queue.current_index)
                    .cloned()
                    .map(|video_id| (video_id, 0.0))
            }
            LoungeCommand::Play => {
                if let Some((video_id, position)) = failed_resolution.take() {
                    if !stopped && queue.video_ids.get(queue.current_index) == Some(&video_id) {
                        Some((video_id, position))
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
                queue.next_pending = false;
                playback_active = false;
                next_resolution = None;
                playback.stop().await;
                None
            }
        };

        if let Some((video_id, start_time)) = load {
            stopped = false;
            failed_resolution = None;
            current_resolution = None;
            tracing::info!(%video_id, start_time, "YouTube queue requests load");
            let snapshot = settings.snapshot();
            let preferred_video_codec = PreferredVideoCodec::from_snapshot(&snapshot);
            // Repeat must not discard the already prepared manual Next.
            let pending = if repeating {
                None
            } else {
                next_resolution.take()
            };
            let resolution = async {
                let mut prepared = None;
                if let Some(mut pending) = pending {
                    if use_prefetch && pending.video_id == video_id {
                        if let Some(task) = pending.task.as_mut() {
                            if let Ok((Ok(mut media), completed)) = task.await {
                                if prefetched_is_fresh(completed) {
                                    media.start_time = start_time.max(0.0);
                                    prepared = Some((media, completed));
                                    tracing::info!(%video_id, "using prefetched YouTube media");
                                }
                            }
                        }
                    }
                }
                match prepared {
                    Some(media) => Ok(media),
                    None => {
                        let mut result = resolver
                            .resolve(&video_id, start_time, &capabilities, preferred_video_codec)
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
                                )
                                .await;
                        }
                        result.map(|media| (media, std::time::Instant::now()))
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
                            Some(LoungeCommand::UpdatePlaylist { video_ids, list_id }) => {
                                queue.video_ids = video_ids;
                                queue.list_id = list_id;
                            }
                            Some(LoungeCommand::SetPlaylist { video_ids, current_index, current_time, list_id })
                                if video_ids.get(current_index) == Some(&video_id)
                                    && current_index == queue.current_index && current_time == start_time => {
                                queue.video_ids = video_ids;
                                queue.list_id = list_id;
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
                Ok((mut media, completed)) => {
                    media.start_time = requested_position;
                    media.autoplay = autoplay;
                    playback_active = true;
                    current_resolution = Some(CurrentResolution {
                        video_id: video_id.clone(),
                        media: media.clone(),
                        completed,
                        codec: preferred_video_codec,
                    });
                    tracing::info!(%video_id, start_time = requested_position,
                        autoplay, "committing resolved YouTube selection");
                    playback.load(media).await;
                }
                Err(error) => {
                    playback_active = false;
                    failed_resolution = Some((video_id.clone(), requested_position));
                    tracing::warn!(%video_id, %error, "failed to resolve YouTube video");
                    playback.stop().await;
                }
            }
        }

        let candidate = if playback_active {
            queue.video_ids.get(queue.current_index + 1).cloned()
        } else {
            None
        };
        if next_resolution.as_ref().map(|p| &p.video_id) != candidate.as_ref() {
            next_resolution = None;
            if let Some(video_id) = candidate {
                let resolver = resolver.clone();
                let capabilities = capabilities.clone();
                let codec = PreferredVideoCodec::from_snapshot(&settings.snapshot());
                let id = video_id.clone();
                tracing::info!(%video_id, "prefetching next YouTube media");
                next_resolution = Some(NextResolution {
                    video_id,
                    task: Some(tokio::spawn(async move {
                        let result = resolver.resolve(&id, 0.0, &capabilities, codec).await;
                        (result, std::time::Instant::now())
                    })),
                });
            }
        }
    }
}

fn retryable_resolution_error(error: &ResolveError) -> bool {
    match error {
        ResolveError::Http(error) => error.status().map(|s| s.as_u16()) != Some(429),
        ResolveError::Protocol("external audio resolver failed") => true,
        _ => false,
    }
}

fn map_resolve_error(error: ResolveError) -> MediaResolveError {
    match error {
        ResolveError::Http(error) => error.into(),
        ResolveError::Unplayable(message) => {
            MediaResolveError::content_unavailable("YOUTUBE_UNPLAYABLE").with_message(message)
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
    fn playlist_advance_can_use_prefetch_but_new_selection_cannot() {
        let queue = QueueState {
            video_ids: vec!["a".into(), "b".into()],
            current_index: 0,
            list_id: Some("queue".into()),
            next_pending: false,
        };
        let selection = |list: &str, index, time| LoungeCommand::SetPlaylist {
            video_ids: vec!["a".into(), "b".into()],
            current_index: index,
            current_time: time,
            list_id: Some(list.into()),
        };
        assert!(queue.advances_to_prepared_next(&selection("queue", 1, 0.0)));
        assert!(queue.advances_to_prepared_next(&LoungeCommand::Next));
        assert!(!queue.advances_to_prepared_next(&selection("other", 1, 0.0)));
        assert!(!queue.advances_to_prepared_next(&selection("queue", 0, 0.0)));
        assert!(!queue.advances_to_prepared_next(&selection("queue", 1, 42.0)));
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
            (
                Err(ResolveError::Protocol("unused")),
                std::time::Instant::now(),
            )
        });
        let handle = task.abort_handle();
        drop(NextResolution {
            video_id: "unused".into(),
            task: Some(task),
        });
        tokio::task::yield_now().await;
        assert!(handle.is_finished());
    }
    use std::sync::Mutex;

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
        }
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
        })
        .await
        .unwrap();
        let (id, retry) = requests.recv().await.unwrap();
        assert_eq!(id, "A");
        assert!(prepared.is_closed());
        retry.send(Ok(test_media("A-fresh"))).unwrap();
        wait_loads(&player, 2).await;
        {
            let loaded = player.loaded.lock().unwrap();
            assert_eq!(loaded[1].content_id.as_deref(), Some("A-fresh"));
            assert_eq!(loaded[1].start_time, 66.0);
            assert!(!loaded[1].autoplay);
        }
        let (_, _next) = requests.recv().await.unwrap();
        tx.send(LoungeCommand::ReloadCurrent {
            video_id: "A".into(),
            current_time: 66.0,
            autoplay: true,
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
            media,
            completed: std::time::Instant::now(),
            codec: PreferredVideoCodec::Auto,
        };
        let original_age = current.completed;
        for _ in 0..2 {
            let replay = current
                .replay(Some(&id), PreferredVideoCodec::Auto)
                .unwrap();
            assert_eq!(replay.start_time, 0.0);
            assert!(replay.autoplay);
            assert_eq!(current.completed, original_age);
        }
        assert!(current
            .replay(Some(&"B".into()), PreferredVideoCodec::Auto)
            .is_none());
        assert!(current.replay(None, PreferredVideoCodec::Auto).is_none());
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::H264)
            .is_none());
        current.completed = std::time::Instant::now() - std::time::Duration::from_secs(601);
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::Auto)
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
            media,
            completed: std::time::Instant::now() - std::time::Duration::from_secs(3600),
            codec: PreferredVideoCodec::Auto,
        };
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::Auto)
            .is_none());
        hint.mark_complete();
        let replay = current
            .replay(Some(&id), PreferredVideoCodec::Auto)
            .unwrap();
        assert_eq!(replay.start_time, 0.0);
        assert!(replay.autoplay);
        assert!(current
            .replay(Some(&"B".into()), PreferredVideoCodec::Auto)
            .is_none());
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::H264)
            .is_none());
        hint.invalidate();
        assert!(current
            .replay(Some(&id), PreferredVideoCodec::Auto)
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
