//! YouTube Lounge pairing and BrowserChannel command transport.

use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use url::Url;
use vibecast_sdk::{IdleReason, OutputControl, PlaybackState, PlayerState, ReceiverContext};

const LOUNGE_BASE: &str = "https://www.youtube.com/api/lounge";
const USER_AGENT: &str =
    "Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 Chrome/120 Safari/537.36 CrKey/1.56";
const MAX_FRAME_LENGTH: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum LoopMode {
    #[default]
    Off,
    One,
    All,
}

impl LoopMode {
    fn parse(value: &Value) -> Option<Self> {
        match value.as_str()? {
            "LOOP_MODE_OFF" => Some(Self::Off),
            "LOOP_MODE_ONE" => Some(Self::One),
            "LOOP_MODE_ALL" => Some(Self::All),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Off => "LOOP_MODE_OFF",
            Self::One => "LOOP_MODE_ONE",
            Self::All => "LOOP_MODE_ALL",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LoungeCommand {
    SetPlaylist {
        video_ids: Vec<String>,
        current_index: usize,
        current_time: f64,
        list_id: Option<String>,
    },
    UpdatePlaylist {
        video_ids: Vec<String>,
        list_id: Option<String>,
    },
    Play,
    Pause,
    Stop,
    Seek(f64),
    Next,
    Previous,
}

pub(crate) struct LoungeConnection {
    volume_rx: Option<watch::Receiver<(f64, bool)>>,
    http: reqwest::Client,
    bind_url: Url,
    bound: BoundSession,
    screen_id: String,
    device_id: String,
    discovery_device_id: String,
    current: CurrentMedia,
}

#[derive(Clone)]
pub(crate) struct LoungeIdentity {
    pub(crate) screen_id: String,
    pub(crate) device_id: String,
}

#[derive(Clone)]
struct BoundSession {
    sid: String,
    gsession_id: String,
    aid: u64,
    rid: u64,
    ofs: u64,
}

#[derive(Default)]
struct CurrentMedia {
    loop_mode: LoopMode,
    volume: Option<(f64, bool)>,
    awaiting_load: bool,
    video_ids: Vec<String>,
    video_id: Option<String>,
    list_id: Option<String>,
    current_index: usize,
    next_pending: bool,
    state: Option<PlaybackState>,
}

impl CurrentMedia {
    // Translate repeats into an ordinary selection so the playback queue and
    // Lounge feedback share the same index and existing cancellation path.
    fn next_command(&self, finished: bool) -> LoungeCommand {
        let index = match self.loop_mode {
            LoopMode::One if finished => Some(self.current_index),
            LoopMode::All if self.current_index + 1 >= self.video_ids.len() => Some(0),
            _ => None,
        };
        if let Some(index) = index.filter(|index| self.video_ids.get(*index).is_some()) {
            LoungeCommand::SetPlaylist {
                video_ids: self.video_ids.clone(),
                current_index: index,
                current_time: 0.0,
                list_id: self.list_id.clone(),
            }
        } else {
            LoungeCommand::Next
        }
    }

    fn coalesce_pending_selection(&self, incoming: Incoming) -> Incoming {
        if matches!(incoming, Incoming::Command(LoungeCommand::Next)) {
            return Incoming::Command(self.next_command(false));
        }
        let incoming = match incoming {
            Incoming::QueueAddition(command) => {
                if let LoungeCommand::SetPlaylist {
                    ref video_ids,
                    ref list_id,
                    ..
                } = command
                {
                    if self.video_id.is_some()
                        && self.video_id.as_ref() == video_ids.get(self.current_index)
                        && self.list_id.is_some()
                        && self.list_id == *list_id
                    {
                        tracing::debug!(video_id = ?self.video_id, index = self.current_index,
                            "preserving selection across VIDEO_ADDED queue enrichment");
                        return Incoming::Command(LoungeCommand::UpdatePlaylist {
                            video_ids: video_ids.clone(),
                            list_id: list_id.clone(),
                        });
                    }
                }
                // No proven unchanged active slot: retain prior semantics.
                Incoming::Command(command)
            }
            other => other,
        };
        if let Incoming::Command(LoungeCommand::SetPlaylist {
            ref video_ids,
            current_index,
            current_time,
            ref list_id,
        }) = incoming
        {
            if self.awaiting_load
                && self.video_id.is_some()
                && self.video_id.as_ref() == video_ids.get(current_index)
                && self.current_index == current_index
                && self
                    .state
                    .as_ref()
                    .is_some_and(|s| s.current_time == current_time)
            {
                tracing::debug!("coalesced pending YouTube selection into queue update");
                return Incoming::Command(LoungeCommand::UpdatePlaylist {
                    video_ids: video_ids.clone(),
                    list_id: list_id.clone(),
                });
            }
        }
        incoming
    }
}

async fn volume_change(rx: &mut Option<watch::Receiver<(f64, bool)>>) -> (f64, bool) {
    if let Some(rx) = rx {
        if rx.changed().await.is_ok() {
            return *rx.borrow_and_update();
        }
    }
    std::future::pending().await
}

#[derive(Debug, Error)]
pub(crate) enum LoungeError {
    #[error("YouTube Lounge HTTP request failed")]
    Http(#[from] reqwest::Error),
    #[error("YouTube Lounge JSON response was invalid")]
    Json(#[from] serde_json::Error),
    #[error("YouTube Lounge protocol error: {0}")]
    Protocol(&'static str),
}

impl LoungeConnection {
    pub(crate) fn with_volume(mut self, rx: watch::Receiver<(f64, bool)>) -> Self {
        self.current.volume = Some(*rx.borrow());
        self.volume_rx = Some(rx);
        self
    }

    pub(crate) async fn establish(
        http: reqwest::Client,
        receiver: &ReceiverContext,
    ) -> Result<Self, LoungeError> {
        Self::establish_at(http, receiver, LOUNGE_BASE).await
    }

    async fn establish_at(
        http: reqwest::Client,
        receiver: &ReceiverContext,
        base: &str,
    ) -> Result<Self, LoungeError> {
        let base = Url::parse(base).map_err(|_| LoungeError::Protocol("invalid base URL"))?;
        let screen: ScreenIdResponse = http
            .get(join(&base, "pairing/generate_screen_id")?)
            .query(&[("enable_screen_id_secret_generation", "true")])
            .header("User-Agent", USER_AGENT)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let token_body = {
            let mut serializer = url::form_urlencoded::Serializer::new(String::new());
            serializer.append_pair("screen_ids", &screen.screen_id);
            serializer.finish()
        };
        let token_response: LoungeTokenResponse = http
            .post(join(&base, "pairing/get_lounge_token_batch")?)
            .header("User-Agent", USER_AGENT)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(token_body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let lounge_token = token_response
            .screens
            .into_iter()
            .find(|item| item.screen_id == screen.screen_id)
            .ok_or(LoungeError::Protocol("token response omitted screen"))?
            .lounge_token;

        let device_id = uuid::Uuid::new_v4().to_string();
        let bind_url = build_bind_url(
            &base,
            &screen.screen_id_secret,
            &lounge_token,
            &device_id,
            receiver,
        )?;
        let bound = initial_bind(&http, &bind_url).await?;

        Ok(Self {
            volume_rx: None,
            http,
            bind_url,
            bound,
            screen_id: screen.screen_id,
            device_id,
            discovery_device_id: cast_cloud_device_id(&receiver.device_id),
            current: CurrentMedia::default(),
        })
    }

    pub(crate) fn identity(&self) -> LoungeIdentity {
        LoungeIdentity {
            screen_id: self.screen_id.clone(),
            device_id: self.device_id.clone(),
        }
    }

    pub(crate) async fn run(
        mut self,
        command_tx: mpsc::Sender<LoungeCommand>,
        mut playback_rx: mpsc::Receiver<PlaybackState>,
        mut output_control_rx: mpsc::Receiver<OutputControl>,
        mut cancel: watch::Receiver<bool>,
    ) {
        loop {
            if *cancel.borrow() {
                return;
            }

            match self
                .run_bound(
                    &command_tx,
                    &mut playback_rx,
                    &mut output_control_rx,
                    &mut cancel,
                )
                .await
            {
                Ok(()) => return,
                Err(error) => tracing::warn!(%error, "YouTube Lounge session interrupted"),
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                result = cancel.changed() => {
                    if result.is_err() || *cancel.borrow() {
                        return;
                    }
                }
            }

            match initial_bind(&self.http, &self.bind_url).await {
                Ok(bound) => self.bound = bound,
                Err(error) => {
                    tracing::warn!(%error, "YouTube Lounge rebind failed");
                }
            }
        }
    }

    async fn run_bound(
        &mut self,
        command_tx: &mpsc::Sender<LoungeCommand>,
        playback_rx: &mut mpsc::Receiver<PlaybackState>,
        output_control_rx: &mut mpsc::Receiver<OutputControl>,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<(), LoungeError> {
        self.post(Outbound::NowPlaying).await?;

        loop {
            // Keep the long poll alive while playback reports arrive. Dropping
            // it on every position update starves incoming controls.
            let http = self.http.clone();
            let bind_url = self.bind_url.clone();
            let bound = self.bound.clone();
            let poll = poll_commands(&http, &bind_url, &bound);
            tokio::pin!(poll);
            loop {
                tokio::select! {
                    volume = volume_change(&mut self.volume_rx) => {
                        self.current.volume = Some(volume);
                        self.post(Outbound::Volume).await?;
                    }
                    control = output_control_rx.recv() => {
                        let Some(control) = control else { return Ok(()); };
                        let command = match control {
                            OutputControl::Next => self.current.next_command(false),
                            OutputControl::Previous => LoungeCommand::Previous,
                        };
                        let incoming = Incoming::Command(command.clone());
                        let outbound = self.handle_internal(&incoming);
                        if command_tx.send(command).await.is_err() {
                            return Ok(());
                        }
                        if let Some(outbound) = outbound {
                            self.post(outbound).await?;
                        }
                    }
                    result = cancel.changed() => {
                        if result.is_err() || *cancel.borrow() {
                            return Ok(());
                        }
                    }
                    state = playback_rx.recv() => {
                        let Some(state) = state else { return Ok(()); };
                        if self.current.awaiting_load {
                            if state.player_state != PlayerState::Buffering
                                && !matches!(state.idle_reason, Some(IdleReason::Error | IdleReason::Cancelled)) {
                                continue;
                            }
                            self.current.awaiting_load = false;
                        }
                        let ended = state.idle_reason == Some(IdleReason::Finished)
                            && self.current.state.as_ref().and_then(|s| s.idle_reason)
                                != Some(IdleReason::Finished);
                        self.current.state = Some(state.clone());
                        self.post(Outbound::State(state)).await?;
                        if ended {
                            let command = self.current.next_command(true);
                            let outbound = self.handle_internal(&Incoming::Command(command.clone()));
                            let _ = command_tx.send(command).await;
                            if let Some(outbound) = outbound { self.post(outbound).await?; }
                        }
                    }
                    result = &mut poll => {
                        let batch = match result {
                            Ok(batch) => batch,
                            Err(LoungeError::Http(error)) if error.is_timeout() => break,
                            Err(error) => return Err(error),
                        };
                        self.bound.aid = self.bound.aid.max(batch.aid);
                        for incoming in batch.messages {
                            let queue_addition = matches!(&incoming, Incoming::QueueAddition(_));
                            let incoming = self.current.coalesce_pending_selection(incoming);
                            let outbound = self.handle_internal(&incoming)
                                .or_else(|| queue_addition.then_some(Outbound::NowPlaying));
                            if let Incoming::Command(command) = incoming {
                                if command_tx.send(command).await.is_err() {
                                    return Ok(());
                                }
                            }
                            // Local playback need not wait for Google's HTTP ACK.
                            // Internal buffering/queue state was already updated.
                            if let Some(outbound) = outbound {
                                self.post(outbound).await?;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    fn handle_internal(&mut self, incoming: &Incoming) -> Option<Outbound> {
        match incoming {
            Incoming::SetLoopMode(mode) => {
                self.current.loop_mode = *mode;
                tracing::info!(loop_mode = mode.as_str(), "YouTube repeat mode changed");
                Some(Outbound::LoopMode)
            }
            Incoming::GetLoopMode => Some(Outbound::LoopMode),
            Incoming::Command(LoungeCommand::SetPlaylist {
                video_ids,
                current_index,
                current_time,
                list_id,
            }) => {
                self.current.video_ids.clone_from(video_ids);
                self.current.awaiting_load = true;
                self.current.video_id = video_ids.get(*current_index).cloned();
                self.current.current_index = *current_index;
                self.current.list_id.clone_from(list_id);
                self.current.next_pending = false;
                self.current.state = Some(PlaybackState {
                    player_state: PlayerState::Buffering,
                    current_time: *current_time,
                    duration: None,
                    idle_reason: None,
                });
                Some(Outbound::NowPlaying)
            }
            Incoming::Command(LoungeCommand::UpdatePlaylist { video_ids, list_id }) => {
                self.current.video_ids.clone_from(video_ids);
                self.current.list_id.clone_from(list_id);
                if self.current.next_pending
                    && self.current.current_index + 1 < self.current.video_ids.len()
                {
                    self.current.current_index += 1;
                    self.current.video_id = self
                        .current
                        .video_ids
                        .get(self.current.current_index)
                        .cloned();
                    self.current.next_pending = false;
                    return Some(Outbound::NowPlaying);
                }
                None
            }
            Incoming::Command(LoungeCommand::Next) => {
                self.current.awaiting_load = true;
                self.current.state = Some(PlaybackState {
                    player_state: PlayerState::Buffering,
                    current_time: 0.0,
                    duration: None,
                    idle_reason: None,
                });
                if self.current.current_index + 1 < self.current.video_ids.len() {
                    self.current.current_index += 1;
                    self.current.video_id = self
                        .current
                        .video_ids
                        .get(self.current.current_index)
                        .cloned();
                    Some(Outbound::NowPlaying)
                } else {
                    self.current.next_pending = true;
                    None
                }
            }
            Incoming::Command(LoungeCommand::Previous) => {
                if self.current.video_ids.is_empty() {
                    return None;
                }
                self.current.awaiting_load = true;
                self.current.next_pending = false;
                self.current.current_index = self.current.current_index.saturating_sub(1);
                self.current.video_id = self
                    .current
                    .video_ids
                    .get(self.current.current_index)
                    .cloned();
                self.current.state = Some(PlaybackState {
                    player_state: PlayerState::Buffering,
                    current_time: 0.0,
                    duration: None,
                    idle_reason: None,
                });
                Some(Outbound::NowPlaying)
            }
            Incoming::GetNowPlaying => Some(Outbound::NowPlaying),
            Incoming::GetPlaybackSpeed => Some(Outbound::PlaybackSpeed),
            Incoming::GetVolume => Some(Outbound::Volume),
            Incoming::SetDiscoveryDeviceId => Some(Outbound::DiscoveryDeviceId),
            Incoming::Command(_) | Incoming::QueueAddition(_) | Incoming::Ignored => None,
        }
    }

    async fn post(&mut self, outbound: Outbound) -> Result<(), LoungeError> {
        self.bound.rid += 1;
        let mut url = self.bind_url.clone();
        append_bound_query(&mut url, &self.bound, self.bound.rid.to_string().as_str());
        url.query_pairs_mut()
            .append_pair("zx", &uuid::Uuid::new_v4().simple().to_string());

        let body = outbound.form_body(
            self.bound.ofs,
            &self.current,
            &self.discovery_device_id,
            &self.device_id,
        );
        self.bound.ofs += if matches!(outbound, Outbound::NowPlaying) {
            2
        } else {
            1
        };
        self.http
            .post(url)
            .header("User-Agent", USER_AGENT)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(())
    }
}

enum Outbound {
    LoopMode,
    NowPlaying,
    State(PlaybackState),
    PlaybackSpeed,
    Volume,
    DiscoveryDeviceId,
}

impl Outbound {
    fn form_body(
        &self,
        ofs: u64,
        current: &CurrentMedia,
        discovery_id: &str,
        lounge_device_id: &str,
    ) -> String {
        let mut form = url::form_urlencoded::Serializer::new(String::new());
        form.append_pair(
            "count",
            if matches!(self, Self::NowPlaying) {
                "2"
            } else {
                "1"
            },
        )
        .append_pair("ofs", &ofs.to_string());
        match self {
            Self::LoopMode => {
                form.append_pair("req0__sc", "onLoopModeChanged")
                    .append_pair("req0_loopMode", current.loop_mode.as_str());
            }
            Self::NowPlaying => {
                form.append_pair("req0__sc", "nowPlaying");
                if let Some(video_id) = &current.video_id {
                    form.append_pair("req0_videoId", video_id);
                }
                if let Some(state) = &current.state {
                    append_state_fields(&mut form, "req0_", state);
                }
                if let Some(list_id) = &current.list_id {
                    form.append_pair("req0_listId", list_id);
                }
                form.append_pair("req0_currentIndex", &current.current_index.to_string());
                form.append_pair("req1__sc", "onLoopModeChanged")
                    .append_pair("req1_loopMode", current.loop_mode.as_str());
            }
            Self::State(state) => {
                form.append_pair("req0__sc", "onStateChange");
                append_state_fields(&mut form, "req0_", state);
                form.append_pair("req0_playabilityStatus", "OK");
            }
            Self::PlaybackSpeed => {
                form.append_pair("req0__sc", "onPlaybackSpeedChanged")
                    .append_pair("req0_playbackSpeed", "1");
            }
            Self::Volume => {
                let (level, muted) = current.volume.unwrap_or((1.0, false));
                form.append_pair("req0__sc", "onVolumeChanged")
                    .append_pair(
                        "req0_volume",
                        &(level.clamp(0.0, 1.0) * 100.0).round().to_string(),
                    )
                    .append_pair("req0_muted", if muted { "true" } else { "false" });
            }
            Self::DiscoveryDeviceId => {
                form.append_pair("req0__sc", "setDiscoveryDeviceId")
                    .append_pair("req0_discoveryDeviceId", discovery_id)
                    .append_pair("req0_loungeDeviceId", lounge_device_id)
                    .append_pair("req0_castCloudDeviceId", discovery_id);
            }
        }
        form.finish()
    }
}

fn append_state_fields(
    form: &mut url::form_urlencoded::Serializer<'_, String>,
    prefix: &str,
    state: &PlaybackState,
) {
    let lounge_state = match state.player_state {
        PlayerState::Playing => "1",
        PlayerState::Paused => "2",
        PlayerState::Buffering => "3",
        PlayerState::Idle => "0",
    };
    form.append_pair(&format!("{prefix}state"), lounge_state)
        .append_pair(
            &format!("{prefix}currentTime"),
            &state.current_time.to_string(),
        )
        .append_pair(
            &format!("{prefix}duration"),
            &state.duration.unwrap_or_default().to_string(),
        )
        .append_pair(
            &format!("{prefix}loadedTime"),
            &state.current_time.to_string(),
        )
        .append_pair(&format!("{prefix}seekableStartTime"), "0")
        .append_pair(
            &format!("{prefix}seekableEndTime"),
            &state.duration.unwrap_or_default().to_string(),
        );
}

async fn initial_bind(http: &reqwest::Client, bind_url: &Url) -> Result<BoundSession, LoungeError> {
    let mut url = bind_url.clone();
    url.query_pairs_mut()
        .append_pair("RID", "1")
        .append_pair("CVER", "1")
        .append_pair("TYPE", "xmlhttp")
        .append_pair("zx", &uuid::Uuid::new_v4().simple().to_string());
    let bytes = http
        .post(url)
        .header("User-Agent", USER_AGENT)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("count=0")
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    parse_initial_bind(&bytes)
}

fn parse_initial_bind(bytes: &[u8]) -> Result<BoundSession, LoungeError> {
    let frames = decode_frames(bytes)?;
    let mut sid = None;
    let mut gsession_id = None;
    let mut aid = 0;
    for frame in frames {
        let Some(entries) = frame.as_array() else {
            continue;
        };
        for entry in entries {
            let Some(parts) = entry.as_array() else {
                continue;
            };
            aid = aid.max(parts.first().and_then(Value::as_u64).unwrap_or_default());
            let Some(message) = parts.get(1).and_then(Value::as_array) else {
                continue;
            };
            match message.first().and_then(Value::as_str) {
                Some("c") => sid = message.get(1).and_then(Value::as_str).map(str::to_string),
                Some("S") => {
                    gsession_id = message.get(1).and_then(Value::as_str).map(str::to_string)
                }
                _ => {}
            }
        }
    }
    Ok(BoundSession {
        sid: sid.ok_or(LoungeError::Protocol("initial bind omitted SID"))?,
        gsession_id: gsession_id.ok_or(LoungeError::Protocol("initial bind omitted gsessionid"))?,
        aid,
        rid: 1,
        ofs: 0,
    })
}

async fn poll_commands(
    http: &reqwest::Client,
    bind_url: &Url,
    bound: &BoundSession,
) -> Result<IncomingBatch, LoungeError> {
    let mut url = bind_url.clone();
    append_bound_query(&mut url, bound, "rpc");
    url.query_pairs_mut()
        .append_pair("CI", "1")
        .append_pair("TYPE", "xmlhttp")
        .append_pair("zx", &uuid::Uuid::new_v4().simple().to_string());
    let bytes = http
        .get(url)
        .header("User-Agent", USER_AGENT)
        .timeout(Duration::from_secs(60))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    parse_incoming(&bytes)
}

fn append_bound_query(url: &mut Url, bound: &BoundSession, rid: &str) {
    url.query_pairs_mut()
        .append_pair("RID", rid)
        .append_pair("SID", &bound.sid)
        .append_pair("AID", &bound.aid.to_string())
        .append_pair("gsessionid", &bound.gsession_id);
}

struct IncomingBatch {
    aid: u64,
    messages: Vec<Incoming>,
}

enum Incoming {
    SetLoopMode(LoopMode),
    GetLoopMode,
    QueueAddition(LoungeCommand),
    Command(LoungeCommand),
    GetNowPlaying,
    GetPlaybackSpeed,
    GetVolume,
    SetDiscoveryDeviceId,
    Ignored,
}

fn parse_incoming(bytes: &[u8]) -> Result<IncomingBatch, LoungeError> {
    let mut aid = 0;
    let mut messages = Vec::new();
    for frame in decode_frames(bytes)? {
        let Some(entries) = frame.as_array() else {
            continue;
        };
        for entry in entries {
            let Some(parts) = entry.as_array() else {
                continue;
            };
            aid = aid.max(parts.first().and_then(Value::as_u64).unwrap_or_default());
            let Some(message) = parts.get(1).and_then(Value::as_array) else {
                continue;
            };
            tracing::debug!(
                sequence = parts.first().and_then(serde_json::Value::as_u64),
                command = message.first().and_then(serde_json::Value::as_str),
                "Lounge incoming command"
            );
            let incoming = parse_message(message);
            // The sender can carry its existing repeat mode into a new cast.
            if message.first().and_then(Value::as_str) == Some("setPlaylist")
                && matches!(&incoming, Incoming::Command(_) | Incoming::QueueAddition(_))
            {
                if let Some(mode) = message
                    .get(1)
                    .and_then(|p| p.get("loopMode"))
                    .and_then(LoopMode::parse)
                {
                    messages.push(Incoming::SetLoopMode(mode));
                }
            }
            if tracing::enabled!(tracing::Level::DEBUG)
                && message.first().and_then(Value::as_str) == Some("setPlaylist")
            {
                let details = selection_diagnostics(message, &incoming);
                tracing::debug!(sequence = parts.first().and_then(serde_json::Value::as_u64),
                    selection = %details, "Lounge selection interpretation");
            }
            messages.push(incoming);
        }
    }
    Ok(IncomingBatch { aid, messages })
}

// Allowlisted selection fields only: never log full params/eventDetails, which
// may carry account/session material. This does not change selection semantics.
fn selection_diagnostics(message: &[Value], incoming: &Incoming) -> Value {
    let params = message.get(1).unwrap_or(&Value::Null);
    let event = params
        .get("eventDetails")
        .and_then(Value::as_str)
        .and_then(|s| serde_json::from_str::<Value>(s).ok());
    let event_id = event
        .as_ref()
        .and_then(|v| v.get("videoId").and_then(Value::as_str));
    // Do not dump unknown event values: eventDetails can contain account data.
    let event_type = event
        .as_ref()
        .and_then(|v| v.get("eventType"))
        .and_then(Value::as_str)
        .map(|kind| match kind {
            "PLAYLIST_SET" | "PLAYLIST_UPDATED" | "VIDEO_ADDED" | "VIDEOS_ADDED"
            | "VIDEO_REMOVED" | "VIDEO_SELECTED" | "QUEUE_UPDATED" => kind,
            _ => "OTHER",
        });
    let (selected, index, len) = match incoming {
        Incoming::Command(LoungeCommand::SetPlaylist {
            video_ids,
            current_index,
            ..
        })
        | Incoming::QueueAddition(LoungeCommand::SetPlaylist {
            video_ids,
            current_index,
            ..
        }) => (
            video_ids.get(*current_index).cloned(),
            Some(*current_index),
            video_ids.len(),
        ),
        _ => (None, None, 0),
    };
    serde_json::json!({
        "videoId": params.get("videoId").and_then(Value::as_str),
        "eventVideoId": event_id,
        "eventType": event_type,
        "eventVideoCount": event.as_ref().and_then(|v| v.get("videoIds"))
            .and_then(Value::as_array).map(Vec::len),
        "currentIndex": params.get("currentIndex").and_then(value_as_usize),
        "parsedIndex": index, "selectedVideoId": selected, "queueLength": len,
        "currentTime": params.get("currentTime").and_then(value_as_f64),
    })
}

#[cfg(test)]
mod selection_diagnostic_tests {
    use super::*;
    #[test]
    fn reproduced_additions_preserve_state_and_feedback_until_explicit_selection() {
        let mut connection = LoungeConnection {
            volume_rx: None,
            http: reqwest::Client::new(),
            bind_url: Url::parse("http://127.0.0.1:9/").unwrap(),
            bound: BoundSession {
                sid: "test".into(),
                gsession_id: "test".into(),
                aid: 0,
                rid: 0,
                ofs: 0,
            },
            screen_id: "test".into(),
            device_id: "test".into(),
            discovery_device_id: "test".into(),
            current: CurrentMedia::default(),
        };
        // Sanitized reconstruction of Controls-16: each addition inserts at
        // index 1. It is not a complete raw-wire capture of the full queues.
        connection.handle_internal(&event("PLAYLIST_SET", "z,next", 0, "queue"));
        for (n, videos) in ["z,b,next", "z,c,b,next", "z,d,c,b,next"]
            .iter()
            .enumerate()
        {
            if n == 2 {
                // The last addition arrives after actual playback began.
                connection.current.awaiting_load = false;
                connection.current.state = Some(PlaybackState {
                    player_state: PlayerState::Playing,
                    current_time: 0.4,
                    duration: Some(180.0),
                    idle_reason: None,
                });
            }
            let incoming = connection.current.coalesce_pending_selection(event(
                "VIDEO_ADDED",
                videos,
                1,
                "queue",
            ));
            assert!(matches!(
                &incoming,
                Incoming::Command(LoungeCommand::UpdatePlaylist { .. })
            ));
            connection.handle_internal(&incoming);
            assert_eq!(connection.current.video_id.as_deref(), Some("z"));
            assert_eq!(connection.current.current_index, 0);
            let fields: std::collections::HashMap<String, String> = url::form_urlencoded::parse(
                Outbound::NowPlaying
                    .form_body(0, &connection.current, "test", "test")
                    .as_bytes(),
            )
            .into_owned()
            .collect();
            assert_eq!(fields["req0_videoId"], "z");
            assert_eq!(fields["req0_currentIndex"], "0");
            if n == 2 {
                assert_eq!(fields["req0_state"], "1");
                assert_eq!(fields["req0_currentTime"], "0.4");
            }
        }
        // Known boundary: this also describes a legitimate explicit user tap.
        // A recent VIDEO_ADDED must not blacklist this selection.
        let explicit = connection.current.coalesce_pending_selection(event(
            "VIDEO_SELECTED",
            "z,d,c,b,next",
            1,
            "queue",
        ));
        assert!(matches!(
            &explicit,
            Incoming::Command(LoungeCommand::SetPlaylist { .. })
        ));
        connection.handle_internal(&explicit);
        assert_eq!(connection.current.video_id.as_deref(), Some("d"));
        assert_eq!(connection.current.current_index, 1);
    }

    #[test]
    fn output_queue_navigation_updates_lounge_now_playing_state() {
        let mut connection = LoungeConnection {
            volume_rx: None,
            http: reqwest::Client::new(),
            bind_url: Url::parse("http://127.0.0.1:9/").unwrap(),
            bound: BoundSession {
                sid: "test".into(),
                gsession_id: "test".into(),
                aid: 0,
                rid: 0,
                ofs: 0,
            },
            screen_id: "test".into(),
            device_id: "test".into(),
            discovery_device_id: "test".into(),
            current: CurrentMedia {
                video_ids: vec!["a".into(), "b".into(), "c".into()],
                video_id: Some("b".into()),
                current_index: 1,
                ..Default::default()
            },
        };

        assert!(matches!(
            connection.handle_internal(&Incoming::Command(LoungeCommand::Next)),
            Some(Outbound::NowPlaying)
        ));
        assert_eq!(connection.current.current_index, 2);
        assert_eq!(connection.current.video_id.as_deref(), Some("c"));
        assert_eq!(
            connection.current.state.as_ref().unwrap().player_state,
            PlayerState::Buffering
        );

        assert!(matches!(
            connection.handle_internal(&Incoming::Command(LoungeCommand::Previous)),
            Some(Outbound::NowPlaying)
        ));
        assert_eq!(connection.current.current_index, 1);
        assert_eq!(connection.current.video_id.as_deref(), Some("b"));
    }

    fn event(kind: &str, videos: &str, index: usize, list: &str) -> Incoming {
        parse_message(&[
            serde_json::json!("setPlaylist"),
            serde_json::json!({
                "videoIds":videos, "currentIndex":index, "listId":list,
                "eventDetails":serde_json::json!({"eventType":kind}).to_string()
            }),
        ])
    }

    #[test]
    fn delayed_additions_preserve_last_selection_but_real_reselection_wins() {
        let mut current = CurrentMedia {
            video_id: Some("z".into()),
            video_ids: vec!["z".into()],
            list_id: Some("queue".into()),
            ..Default::default()
        };
        // Captured pattern: final PLAYLIST_SET, then four VIDEO_ADDED messages
        // whose currentIndex points at the newly inserted (older requested) item.
        for (videos, index) in [("z,a", 1), ("z,a,b", 2), ("z,a,b,c", 3), ("z,a,b,c,d", 4)] {
            let result =
                current.coalesce_pending_selection(event("VIDEO_ADDED", videos, index, "queue"));
            let Incoming::Command(LoungeCommand::UpdatePlaylist { video_ids, .. }) = result else {
                panic!("addition must not start another title");
            };
            current.video_ids = video_ids;
            assert_eq!(current.video_ids[current.current_index], "z");
        }
        for kind in ["VIDEO_SELECTED", "PLAYLIST_SET"] {
            assert!(matches!(
                current.coalesce_pending_selection(event(kind, "z,a,b,c,d", 1, "queue")),
                Incoming::Command(LoungeCommand::SetPlaylist {
                    current_index: 1,
                    ..
                })
            ));
        }
        current.awaiting_load = true;
        assert!(matches!(
            current.coalesce_pending_selection(event("VIDEO_ADDED", "z,a,b,c,d,e", 5, "queue")),
            Incoming::Command(LoungeCommand::UpdatePlaylist { .. })
        ));
    }

    #[test]
    fn additions_without_matching_active_queue_keep_previous_semantics() {
        let current = CurrentMedia {
            video_id: Some("z".into()),
            list_id: Some("queue".into()),
            ..Default::default()
        };
        for incoming in [
            event("VIDEO_ADDED", "z,a", 1, "other"),
            event("VIDEO_ADDED", "a,z", 0, "queue"),
        ] {
            assert!(matches!(
                current.coalesce_pending_selection(incoming),
                Incoming::Command(LoungeCommand::SetPlaylist { .. })
            ));
        }
        assert!(matches!(
            CurrentMedia::default().coalesce_pending_selection(event(
                "VIDEO_ADDED",
                "a",
                0,
                "queue"
            )),
            Incoming::Command(LoungeCommand::SetPlaylist { .. })
        ));
    }

    #[test]
    fn exposes_mismatch_without_logging_private_params() {
        let message = vec![
            serde_json::json!("setPlaylist"),
            serde_json::json!({
                "videoId":"explicit", "videoIds":"first,indexed", "currentIndex":"1",
                "currentTime":"0", "token":"secret-do-not-log",
                "eventDetails":"{\"videoId\":\"event\",\"credential\":\"hidden\"}"
            }),
        ];
        let incoming = parse_message(&message);
        let details = selection_diagnostics(&message, &incoming);
        assert_eq!(details["videoId"], "explicit");
        assert_eq!(details["eventVideoId"], "event");
        assert_eq!(details["selectedVideoId"], "indexed");
        assert_eq!(details["parsedIndex"], 1);
        assert!(!details.to_string().contains("secret-do-not-log"));
        assert!(!details.to_string().contains("hidden"));
    }

    #[test]
    fn exposes_event_kind_but_redacts_unknown_values() {
        for (kind, expected) in [
            ("PLAYLIST_SET", "PLAYLIST_SET"),
            ("VIDEO_ADDED", "VIDEO_ADDED"),
            ("private-value", "OTHER"),
        ] {
            let message = vec![
                serde_json::json!("setPlaylist"),
                serde_json::json!({
                    "videoIds":"a,b", "currentIndex":"1",
                    "eventDetails": serde_json::json!({"eventType":kind,
                        "videoIds":["b"], "user":"private-user"}).to_string()
                }),
            ];
            let details = selection_diagnostics(&message, &parse_message(&message));
            assert_eq!(details["eventType"], expected);
            assert_eq!(details["eventVideoCount"], 1);
            assert!(!details.to_string().contains("private-"));
        }
    }
}

fn parse_message(message: &[Value]) -> Incoming {
    let Some(name) = message.first().and_then(Value::as_str) else {
        return Incoming::Ignored;
    };
    let params = message.get(1);
    let command = match name {
        "setPlaylist" => parse_set_playlist(params),
        "updatePlaylist" => parse_update_playlist(params),
        "play" => Some(LoungeCommand::Play),
        "pause" => Some(LoungeCommand::Pause),
        "stopVideo" | "stop" => Some(LoungeCommand::Stop),
        "next" => Some(LoungeCommand::Next),
        "seekTo" => params
            .and_then(|value| value.get("newTime"))
            .and_then(value_as_f64)
            .map(LoungeCommand::Seek),
        _ => None,
    };
    if let Some(command) = command {
        let is_addition = name == "setPlaylist"
            && params
                .and_then(|p| p.get("eventDetails"))
                .and_then(Value::as_str)
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .is_some_and(|e| e.get("eventType").and_then(Value::as_str) == Some("VIDEO_ADDED"));
        if is_addition {
            return Incoming::QueueAddition(command);
        }
        return Incoming::Command(command);
    }
    match name {
        "setLoopMode" => params
            .and_then(|p| p.get("loopMode"))
            .and_then(LoopMode::parse)
            .map(Incoming::SetLoopMode)
            .unwrap_or(Incoming::Ignored),
        "getLoopMode" => Incoming::GetLoopMode,
        "getNowPlaying" => Incoming::GetNowPlaying,
        "getPlaybackSpeed" => Incoming::GetPlaybackSpeed,
        "getVolume" => Incoming::GetVolume,
        "onSetDiscoveryDeviceId" => Incoming::SetDiscoveryDeviceId,
        _ => Incoming::Ignored,
    }
}

fn parse_set_playlist(params: Option<&Value>) -> Option<LoungeCommand> {
    let params = params?;
    let event_video_id = params
        .get("eventDetails")
        .and_then(Value::as_str)
        .and_then(|json| serde_json::from_str::<Value>(json).ok())
        .and_then(|event| event.get("videoId")?.as_str().map(str::to_string));
    let primary = params
        .get("videoId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or(event_video_id);
    let mut video_ids = parse_video_ids(params);
    if video_ids.is_empty() {
        video_ids.extend(primary);
    }
    if video_ids.is_empty() {
        return None;
    }
    let current_index = params
        .get("currentIndex")
        .and_then(value_as_usize)
        .unwrap_or_default()
        .min(video_ids.len() - 1);
    Some(LoungeCommand::SetPlaylist {
        video_ids,
        current_index,
        current_time: params
            .get("currentTime")
            .and_then(value_as_f64)
            .unwrap_or_default(),
        list_id: params
            .get("listId")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn parse_update_playlist(params: Option<&Value>) -> Option<LoungeCommand> {
    let params = params?;
    let video_ids = parse_video_ids(params);
    (!video_ids.is_empty()).then(|| LoungeCommand::UpdatePlaylist {
        video_ids,
        list_id: params
            .get("listId")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn parse_video_ids(params: &Value) -> Vec<String> {
    params
        .get("videoIds")
        .and_then(Value::as_str)
        .into_iter()
        .flat_map(|ids| ids.split(','))
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect()
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn value_as_usize(value: &Value) -> Option<usize> {
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn decode_frames(bytes: &[u8]) -> Result<Vec<Value>, LoungeError> {
    let mut decoder = FrameDecoder::default();
    decoder.push(bytes);
    let mut frames = Vec::new();
    while let Some(frame) = decoder.next()? {
        frames.push(frame);
    }
    if !decoder.is_empty() {
        return Err(LoungeError::Protocol("truncated BrowserChannel frame"));
    }
    Ok(frames)
}

#[derive(Default)]
struct FrameDecoder {
    buffer: Vec<u8>,
}

impl FrameDecoder {
    fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    fn next(&mut self) -> Result<Option<Value>, LoungeError> {
        while matches!(self.buffer.first(), Some(b'\n' | b'\r' | b' ' | b'\t')) {
            self.buffer.remove(0);
        }
        let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') else {
            return Ok(None);
        };
        let length = std::str::from_utf8(&self.buffer[..newline])
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or(LoungeError::Protocol("invalid BrowserChannel length"))?;
        if length > MAX_FRAME_LENGTH {
            return Err(LoungeError::Protocol("BrowserChannel frame is too large"));
        }
        let payload_start = newline + 1;
        let payload_end = payload_start + length;
        if self.buffer.len() < payload_end {
            return Ok(None);
        }
        let value = serde_json::from_slice(&self.buffer[payload_start..payload_end])?;
        self.buffer.drain(..payload_end);
        Ok(Some(value))
    }

    fn is_empty(&self) -> bool {
        self.buffer
            .iter()
            .all(|byte| matches!(byte, b'\n' | b'\r' | b' ' | b'\t'))
    }
}

fn build_bind_url(
    base: &Url,
    screen_secret: &str,
    lounge_token: &str,
    device_id: &str,
    receiver: &ReceiverContext,
) -> Result<Url, LoungeError> {
    let mut url = join(base, "bc/bind")?;
    let device_info = serde_json::json!({
        "brand": "vibecast",
        "model": receiver.device_model,
        "year": 0,
        "os": "Linux",
        "osVersion": "1",
        "chipset": "",
        "clientName": "TVHTML5",
        "dialAdditionalDataSupportLevel": "unsupported",
        "mdxDialServerType": "MDX_DIAL_SERVER_TYPE_UNKNOWN"
    });
    url.query_pairs_mut()
        .append_pair("device", "LOUNGE_SCREEN")
        .append_pair("id", device_id)
        .append_pair("name", "YouTube on TV")
        .append_pair("app", "lb-v4")
        .append_pair("theme", "cl")
        .append_pair(
            "capabilities",
            "dsp,dpa,mic,ntb,vsp,ads,pas,dcn,dcp,drq,sads,mlm",
        )
        .append_pair("cst", "m")
        .append_pair("mdxVersion", "2")
        .append_pair("screenIdSecret", screen_secret)
        .append_pair("loungeIdToken", lounge_token)
        .append_pair("VER", "8")
        .append_pair("v", "2")
        .append_pair("t", "1")
        .append_pair("deviceInfo", &device_info.to_string());
    Ok(url)
}

fn cast_cloud_device_id(device_id: &str) -> String {
    uuid::Uuid::parse_str(device_id)
        .map(|id| id.simple().to_string().to_ascii_uppercase())
        .unwrap_or_else(|_| device_id.to_string())
}

fn join(base: &Url, path: &str) -> Result<Url, LoungeError> {
    base.join(&format!("{}/{}", base.path().trim_end_matches('/'), path))
        .map_err(|_| LoungeError::Protocol("invalid Lounge endpoint"))
}

#[derive(Deserialize)]
struct ScreenIdResponse {
    #[serde(rename = "screenId")]
    screen_id: String,
    #[serde(rename = "screenIdSecret")]
    screen_id_secret: String,
}

#[derive(Deserialize)]
struct LoungeTokenResponse {
    screens: Vec<LoungeTokenScreen>,
}

#[derive(Deserialize)]
struct LoungeTokenScreen {
    #[serde(rename = "screenId")]
    screen_id: String,
    #[serde(rename = "loungeToken")]
    lounge_token: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeat_capability_and_sender_modes_round_trip() {
        let receiver = ReceiverContext::new("Test", "Model", "device", PathBuf::new());
        let url = build_bind_url(
            &Url::parse(LOUNGE_BASE).unwrap(),
            "secret",
            "token",
            "device",
            &receiver,
        )
        .unwrap();
        let capabilities = url
            .query_pairs()
            .find(|(key, _)| key == "capabilities")
            .unwrap()
            .1
            .into_owned();
        assert!(capabilities.split(',').any(|value| value == "mlm"));
        for mode in [LoopMode::Off, LoopMode::One, LoopMode::All] {
            let incoming = parse_message(&[
                Value::from("setLoopMode"),
                serde_json::json!({"loopMode": mode.as_str()}),
            ]);
            assert!(matches!(incoming, Incoming::SetLoopMode(value) if value == mode));
            let current = CurrentMedia {
                loop_mode: mode,
                ..Default::default()
            };
            for (outbound, prefix) in [
                (Outbound::LoopMode, "req0_"),
                (Outbound::NowPlaying, "req1_"),
            ] {
                let body = outbound.form_body(7, &current, "device", "lounge");
                let values: std::collections::HashMap<_, _> =
                    url::form_urlencoded::parse(body.as_bytes())
                        .into_owned()
                        .collect();
                assert_eq!(values[&format!("{prefix}_sc")], "onLoopModeChanged");
                assert_eq!(values[&format!("{prefix}loopMode")], mode.as_str());
            }
        }
        for value in [Value::Null, Value::from(1), Value::from("unknown")] {
            assert!(matches!(
                parse_message(&[
                    Value::from("setLoopMode"),
                    serde_json::json!({"loopMode": value})
                ]),
                Incoming::Ignored
            ));
        }
    }

    #[test]
    fn initial_playlist_preserves_sender_repeat_mode() {
        let batch = parse_incoming(
            frame(r#"[[5,["setPlaylist",{"videoIds":"a,b","loopMode":"LOOP_MODE_ALL"}]]]"#)
                .as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            batch.messages[0],
            Incoming::SetLoopMode(LoopMode::All)
        ));
        assert!(matches!(
            batch.messages[1],
            Incoming::Command(LoungeCommand::SetPlaylist { .. })
        ));
    }

    #[test]
    fn repeat_one_restarts_on_finish_but_manual_next_still_advances() {
        let current = CurrentMedia {
            loop_mode: LoopMode::One,
            video_ids: vec!["a".into(), "b".into()],
            current_index: 1,
            list_id: Some("queue".into()),
            ..Default::default()
        };
        assert_eq!(
            current.next_command(true),
            LoungeCommand::SetPlaylist {
                video_ids: vec!["a".into(), "b".into()],
                current_index: 1,
                current_time: 0.0,
                list_id: Some("queue".into()),
            }
        );
        assert_eq!(current.next_command(false), LoungeCommand::Next);
    }

    #[test]
    fn repeat_all_wraps_only_at_queue_end_and_off_keeps_queue_extension() {
        let mut current = CurrentMedia {
            loop_mode: LoopMode::All,
            video_ids: vec!["a".into(), "b".into()],
            ..Default::default()
        };
        assert_eq!(current.next_command(true), LoungeCommand::Next);
        current.current_index = 1;
        for finished in [true, false] {
            assert!(matches!(
                current.next_command(finished),
                LoungeCommand::SetPlaylist {
                    current_index: 0,
                    current_time: 0.0,
                    ..
                }
            ));
        }
        current.loop_mode = LoopMode::Off;
        assert_eq!(current.next_command(true), LoungeCommand::Next);
        for mode in [LoopMode::One, LoopMode::All] {
            current.loop_mode = mode;
            current.video_ids.clear();
            assert_eq!(current.next_command(true), LoungeCommand::Next);
        }
    }
    #[test]
    fn volume_reply_uses_actual_level_and_mute() {
        let current = CurrentMedia {
            volume: Some((0.5, true)),
            ..Default::default()
        };
        let body = Outbound::Volume.form_body(0, &current, "device", "lounge");
        assert!(body.contains("req0_volume=50"));
        assert!(body.contains("req0_muted=true"));
    }

    #[test]
    fn pending_selection_updates_queue_without_second_load() {
        let mut current = CurrentMedia {
            awaiting_load: true,
            video_id: Some("a".into()),
            state: Some(PlaybackState {
                player_state: PlayerState::Buffering,
                current_time: 0.0,
                duration: None,
                idle_reason: None,
            }),
            ..Default::default()
        };
        let selection = |video: &str, position| {
            Incoming::Command(LoungeCommand::SetPlaylist {
                video_ids: vec![video.into(), "next".into()],
                current_index: 0,
                current_time: position,
                list_id: Some("new-queue".into()),
            })
        };
        assert!(
            matches!(current.coalesce_pending_selection(selection("a", 0.0)),
            Incoming::Command(LoungeCommand::UpdatePlaylist { video_ids, list_id })
                if video_ids == ["a", "next"] && list_id.as_deref() == Some("new-queue"))
        );
        for command in [selection("b", 0.0), selection("a", 42.0)] {
            assert!(matches!(
                current.coalesce_pending_selection(command),
                Incoming::Command(LoungeCommand::SetPlaylist { .. })
            ));
        }
        current.awaiting_load = false;
        assert!(matches!(
            current.coalesce_pending_selection(selection("a", 0.0)),
            Incoming::Command(LoungeCommand::SetPlaylist { .. })
        ));
    }
    use std::path::PathBuf;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn frame(value: &str) -> String {
        format!("{}\n{}\n", value.len(), value)
    }

    #[test]
    fn decoder_retains_partial_and_uses_declared_byte_length() {
        let first = r#"[[5,["noop"]]]"#;
        let second = r#"[[6,["seekTo",{"note":"[inside]","newTime":"12"}]]]"#;
        let encoded = format!("{}{}", frame(first), frame(second));
        let split = frame(first).len() + 8;

        let mut decoder = FrameDecoder::default();
        decoder.push(&encoded.as_bytes()[..split]);
        assert_eq!(decoder.next().unwrap().unwrap()[0][0], 5);
        assert!(decoder.next().unwrap().is_none());
        decoder.push(&encoded.as_bytes()[split..]);
        assert_eq!(decoder.next().unwrap().unwrap()[0][0], 6);
        assert!(decoder.is_empty());
    }

    #[test]
    fn parses_captured_playlist_and_controls() {
        let body = [
            frame(r#"[[15,["setPlaylist",{"listId":"queue","eventDetails":"{\"videoId\":\"dQw4w9WgXcQ\"}","videoIds":"dQw4w9WgXcQ","currentIndex":"0","currentTime":"7.5"}]]]"#),
            frame(r#"[[16,["pause"]],[17,["play"]],[18,["seekTo",{"newTime":"111"}]]]"#),
        ]
        .concat();
        let batch = parse_incoming(body.as_bytes()).unwrap();

        assert_eq!(batch.aid, 18);
        assert!(matches!(
            &batch.messages[0],
            Incoming::Command(LoungeCommand::SetPlaylist {
                video_ids,
                current_time,
                ..
            }) if video_ids == &["dQw4w9WgXcQ"] && *current_time == 7.5
        ));
        assert!(matches!(
            batch.messages[1],
            Incoming::Command(LoungeCommand::Pause)
        ));
        assert!(matches!(
            batch.messages[2],
            Incoming::Command(LoungeCommand::Play)
        ));
        assert!(matches!(
            batch.messages[3],
            Incoming::Command(LoungeCommand::Seek(111.0))
        ));
    }

    #[test]
    fn initial_bind_requires_both_session_ids() {
        let valid = frame(r#"[[0,["c","SID","",8]],[1,["S","GSID"]]]"#);
        let bound = parse_initial_bind(valid.as_bytes()).unwrap();
        assert_eq!(bound.sid, "SID");
        assert_eq!(bound.gsession_id, "GSID");
        assert_eq!(bound.aid, 1);

        let missing = frame(r#"[[0,["c","SID","",8]]]"#);
        assert!(matches!(
            parse_initial_bind(missing.as_bytes()),
            Err(LoungeError::Protocol("initial bind omitted gsessionid"))
        ));
    }

    #[test]
    fn playback_state_is_encoded_for_lounge() {
        let current = CurrentMedia::default();
        let body = Outbound::State(PlaybackState {
            player_state: PlayerState::Paused,
            current_time: 42.5,
            duration: Some(120.0),
            idle_reason: None,
        })
        .form_body(3, &current, "device", "lounge-device");
        let values: std::collections::HashMap<_, _> = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        assert_eq!(
            values.get("req0__sc").map(String::as_str),
            Some("onStateChange")
        );
        assert_eq!(values.get("req0_state").map(String::as_str), Some("2"));
        assert_eq!(
            values.get("req0_currentTime").map(String::as_str),
            Some("42.5")
        );
    }

    #[test]
    fn discovery_status_includes_cast_and_lounge_identities() {
        let body = Outbound::DiscoveryDeviceId.form_body(
            0,
            &CurrentMedia::default(),
            "CAST-ID",
            "lounge-id",
        );
        let values: std::collections::HashMap<_, _> = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        assert_eq!(values.get("req0_discoveryDeviceId").unwrap(), "CAST-ID");
        assert_eq!(values.get("req0_castCloudDeviceId").unwrap(), "CAST-ID");
        assert_eq!(values.get("req0_loungeDeviceId").unwrap(), "lounge-id");
    }

    #[test]
    fn cast_cloud_identity_normalizes_uuid_device_ids() {
        assert_eq!(
            cast_cloud_device_id("123e4567-e89b-12d3-a456-426614174000"),
            "123E4567E89B12D3A456426614174000"
        );
    }

    #[tokio::test]
    async fn establish_pairs_and_requires_a_valid_initial_bind() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/lounge/pairing/generate_screen_id"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "screenId": "screen-id",
                "screenIdSecret": "screen-secret"
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/lounge/pairing/get_lounge_token_batch"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "screens": [{"screenId": "screen-id", "loungeToken": "lounge-token"}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/lounge/bc/bind"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(frame(r#"[[0,["c","SID","",8]],[1,["S","GSID"]]]"#)),
            )
            .mount(&server)
            .await;

        let receiver = ReceiverContext::new("Living Room", "Model", "device-1", PathBuf::new());
        let mut connection = LoungeConnection::establish_at(
            reqwest::Client::new(),
            &receiver,
            &format!("{}/api/lounge", server.uri()),
        )
        .await
        .unwrap();

        let identity = connection.identity();
        assert_eq!(identity.screen_id, "screen-id");
        assert!(!identity.device_id.is_empty());
        assert_eq!(connection.bound.sid, "SID");
        assert_eq!(connection.bound.gsession_id, "GSID");

        connection.bound.aid = 4;
        connection.post(Outbound::NowPlaying).await.unwrap();
        assert_eq!(connection.bound.aid, 4, "forward ACK must not advance AID");
    }

    #[tokio::test]
    async fn repeat_commands_reach_playback_and_feedback_without_reloading_on_toggle() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        // Hold the poll open: playback EOF and receiver controls must still work.
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
            .mount(&server)
            .await;
        let mut connection = LoungeConnection {
            volume_rx: None,
            http: reqwest::Client::new(),
            bind_url: Url::parse(&format!("{}/api/lounge/bc/bind", server.uri())).unwrap(),
            bound: BoundSession {
                sid: "SID".into(),
                gsession_id: "GSID".into(),
                aid: 0,
                rid: 1,
                ofs: 0,
            },
            screen_id: "screen".into(),
            device_id: "device".into(),
            discovery_device_id: "cast".into(),
            current: CurrentMedia {
                video_ids: vec!["a".into(), "b".into()],
                video_id: Some("b".into()),
                current_index: 1,
                list_id: Some("queue".into()),
                state: Some(PlaybackState {
                    player_state: PlayerState::Playing,
                    current_time: 12.0,
                    duration: Some(20.0),
                    idle_reason: None,
                }),
                ..Default::default()
            },
        };
        for mode in [LoopMode::Off, LoopMode::All, LoopMode::One] {
            let incoming = parse_message(&[
                Value::from("setLoopMode"),
                serde_json::json!({"loopMode": mode.as_str()}),
            ]);
            let outbound = connection.handle_internal(&incoming).unwrap();
            assert!(matches!(outbound, Outbound::LoopMode));
            assert_eq!(
                connection.current.state.as_ref().unwrap().current_time,
                12.0
            );
            assert!(!connection.current.awaiting_load);
            connection.post(outbound).await.unwrap();
        }
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let (playback_tx, mut playback_rx) = mpsc::channel(4);
        let (control_tx, mut control_rx) = mpsc::channel(4);
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            connection
                .run_bound(
                    &command_tx,
                    &mut playback_rx,
                    &mut control_rx,
                    &mut cancel_rx,
                )
                .await
        });
        playback_tx
            .send(PlaybackState {
                player_state: PlayerState::Idle,
                current_time: 20.0,
                duration: Some(20.0),
                idle_reason: Some(IdleReason::Finished),
            })
            .await
            .unwrap();
        let command = tokio::time::timeout(Duration::from_secs(2), command_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            command,
            LoungeCommand::SetPlaylist {
                current_index: 1,
                current_time: 0.0,
                ..
            }
        ));
        // A duplicate EOF while the repeat is loading must not start it twice.
        playback_tx
            .send(PlaybackState {
                player_state: PlayerState::Idle,
                current_time: 20.0,
                duration: Some(20.0),
                idle_reason: Some(IdleReason::Finished),
            })
            .await
            .unwrap();
        control_tx.send(OutputControl::Next).await.unwrap();
        let command = tokio::time::timeout(Duration::from_secs(2), command_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(command, LoungeCommand::Next);
        cancel_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(command_rx.try_recv().is_err());
        let requests = server.received_requests().await.unwrap();
        assert!(requests.iter().any(|r| {
            let body = String::from_utf8_lossy(&r.body);
            body.contains("req0_currentIndex=1") && body.contains("req1_loopMode=LOOP_MODE_ONE")
        }));
    }

    #[tokio::test]
    async fn discovery_identity_follows_the_server_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/lounge/bc/bind"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/lounge/bc/bind"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(frame(r#"[[5,["onSetDiscoveryDeviceId"]]]"#)),
            )
            .mount(&server)
            .await;

        let mut connection = LoungeConnection {
            volume_rx: None,
            http: reqwest::Client::new(),
            bind_url: Url::parse(&format!("{}/api/lounge/bc/bind", server.uri())).unwrap(),
            bound: BoundSession {
                sid: "SID".to_string(),
                gsession_id: "GSID".to_string(),
                aid: 0,
                rid: 1,
                ofs: 0,
            },
            screen_id: "screen-id".to_string(),
            device_id: "lounge-device".to_string(),
            discovery_device_id: "CAST-ID".to_string(),
            current: CurrentMedia::default(),
        };
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_playback_tx, mut playback_rx) = mpsc::channel(1);
        let (_output_control_tx, mut output_control_rx) = mpsc::channel(1);
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            connection
                .run_bound(
                    &command_tx,
                    &mut playback_rx,
                    &mut output_control_rx,
                    &mut cancel_rx,
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if server.received_requests().await.unwrap().len() >= 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("discovery response was not sent");

        cancel_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("Lounge loop did not stop after cancellation")
            .unwrap()
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        let sequence = requests[..3]
            .iter()
            .map(|request| {
                if request.method.as_str() == "GET" {
                    "poll"
                } else if String::from_utf8_lossy(&request.body).contains("req0__sc=nowPlaying") {
                    "nowPlaying"
                } else {
                    "setDiscoveryDeviceId"
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(sequence, &["nowPlaying", "poll", "setDiscoveryDeviceId"]);
    }
}
