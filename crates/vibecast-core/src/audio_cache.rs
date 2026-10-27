//! One bounded progressive audio object per active output. No disk/archive.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use http::{header, HeaderMap, HeaderValue, Method};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use vibecast_player_api::{CachedMediaBody, CachedMediaResponse};
use vibecast_sdk::MediaCacheHint;

pub(crate) const MAX_AUDIO_BYTES: usize = 32 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;
const BLOCK: usize = 128 * 1024;
const UPSTREAM_RANGE: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Pending,
    Streaming,
    Complete,
    Bypass,
    Failed,
    Cancelled,
}

struct Data {
    phase: Phase,
    blocks: Vec<Vec<u8>>,
    wanted: VecDeque<usize>,
    length: usize,
    content_type: HeaderValue,
    resume_index: usize,
    bypass_url: Option<String>,
    integrity_failed: bool,
}

struct Shared {
    data: Mutex<Data>,
    changed: watch::Sender<()>,
    hint: MediaCacheHint,
}

pub(crate) struct AudioCache {
    pub token: String,
    pub hint: MediaCacheHint,
    url: String,
    http: reqwest::Client,
    shared: Arc<Shared>,
    task: Mutex<Option<JoinHandle<()>>>,
    readers: Arc<Semaphore>,
    limit: usize,
    idle_timeout: Duration,
}

impl AudioCache {
    pub fn new(http: reqwest::Client, url: String, hint: MediaCacheHint) -> Self {
        Self {
            token: format!("cache-{}", uuid::Uuid::new_v4()),
            hint: hint.clone(),
            url,
            http,
            shared: Arc::new(Shared {
                data: Mutex::new(Data {
                    phase: Phase::Pending,
                    blocks: Vec::new(),
                    wanted: VecDeque::new(),
                    length: 0,
                    content_type: HeaderValue::from_static("application/octet-stream"),
                    resume_index: 0,
                    bypass_url: None,
                    integrity_failed: false,
                }),
                changed: watch::channel(()).0,
                hint,
            }),
            task: Mutex::new(None),
            readers: Arc::new(Semaphore::new(8)),
            limit: MAX_AUDIO_BYTES,
            idle_timeout: Duration::from_secs(30),
        }
    }

    pub fn cancel(&self) {
        if let Some(task) = self.task.lock().unwrap().take() {
            task.abort();
        }
        let mut data = self.shared.data.lock().unwrap();
        data.phase = Phase::Cancelled;
        data.blocks = Vec::new();
        data.wanted.clear();
        self.hint.invalidate();
        self.shared.changed.send_replace(());
    }

    pub(crate) fn has_failed(&self) -> bool {
        self.shared.data.lock().unwrap().phase == Phase::Failed
    }

    fn start(&self) {
        let mut task = self.task.lock().unwrap();
        if task.is_some() || self.shared.data.lock().unwrap().phase != Phase::Pending {
            return;
        }
        let shared = self.shared.clone();
        let http = self.http.clone();
        let mut url = self.url.clone();
        let limit = self.limit;
        let idle_timeout = self.idle_timeout;
        *task = Some(tokio::spawn(async move {
            loop {
                if download(&http, &url, &shared, limit, idle_timeout)
                    .await
                    .is_ok()
                {
                    return;
                }
                // Repair the same object while the decoder can still play its
                // buffered audio. Keep partial blocks and pending HTTP readers;
                // closing them here would defer recovery until decoder EOF.
                if shared.data.lock().unwrap().phase == Phase::Cancelled {
                    return;
                }
                let mut refreshed = None;
                // Conflicting cached bytes or an invalid response tail must
                // never be promoted to a complete object on another attempt.
                // Leave clean decoder reloads to the app's remaining budget.
                if let Some(refresh) = shared
                    .hint
                    .source_refresh()
                    .filter(|_| !shared.data.lock().unwrap().integrity_failed)
                {
                    // Keep the final round for a clean decoder reload. A
                    // different representation cannot be spliced into existing
                    // bytes, but may still play successfully as a new load.
                    if shared.hint.recovery_attempts() == 0 && shared.hint.try_recovery() {
                        tracing::warn!(
                            attempt = shared.hint.recovery_attempts(),
                            "refreshing failed audio source before decoder buffer drains"
                        );
                        if let Ok(Some(next)) = tokio::time::timeout(
                            Duration::from_secs(30),
                            refresh.refresh_with_context(),
                        )
                        .await
                        {
                            refreshed = Some(next);
                        }
                    }
                }
                if let Some(next) = refreshed {
                    let data = shared.data.lock().unwrap();
                    tracing::info!(
                        attempt = shared.hint.recovery_attempts(),
                        resume_byte = data.resume_index * BLOCK
                            + data.blocks.get(data.resume_index).map_or(0, Vec::len),
                        "resuming cached audio with refreshed source"
                    );
                    drop(data);
                    if shared
                        .hint
                        .set_request_headers(next.request_headers())
                        .is_err()
                    {
                        tracing::warn!("refreshed audio request context was rejected");
                        let mut data = shared.data.lock().unwrap();
                        data.phase = Phase::Failed;
                        data.blocks = Vec::new();
                        data.wanted.clear();
                        shared.hint.invalidate();
                        shared.changed.send_replace(());
                        return;
                    }
                    url = next.url;
                    continue;
                }
                let mut data = shared.data.lock().unwrap();
                if data.phase != Phase::Cancelled {
                    data.phase = Phase::Failed;
                    data.blocks = Vec::new();
                    data.wanted.clear();
                    shared.hint.invalidate();
                    tracing::warn!("current audio cache fetch failed");
                    shared.changed.send_replace(());
                }
                return;
            }
        }));
    }

    pub async fn response(&self, method: Method, headers: HeaderMap) -> CachedMediaResponse {
        let Ok(permit) = self.readers.clone().try_acquire_owned() else {
            return empty(503);
        };
        self.start();
        let mut changed = self.shared.changed.subscribe();
        let (phase, length, content_type) = loop {
            changed.borrow_and_update();
            let snapshot = {
                let data = self.shared.data.lock().unwrap();
                (data.phase, data.length, data.content_type.clone())
            };
            if snapshot.0 != Phase::Pending {
                break snapshot;
            }
            if changed.changed().await.is_err() {
                return empty(502);
            }
        };
        if phase == Phase::Bypass {
            return self.proxy_bypass(method, headers, permit).await;
        }
        if matches!(phase, Phase::Failed | Phase::Cancelled) {
            return empty(502);
        }
        // Without validators, If-Range requires the full representation.
        let range = if headers.contains_key(header::IF_RANGE) {
            None
        } else {
            headers.get(header::RANGE).and_then(|v| v.to_str().ok())
        };
        let (start, end) = match byte_range(range, length) {
            Ok(range) => range,
            Err(()) => {
                let mut response = empty(416);
                response.headers.insert(
                    header::CONTENT_RANGE,
                    HeaderValue::from_str(&format!("bytes */{length}")).unwrap(),
                );
                return response;
            }
        };
        let mut response = empty(if range.is_some() { 206 } else { 200 });
        response.headers.insert(header::CONTENT_TYPE, content_type);
        response
            .headers
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        response.headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&(end - start).to_string()).unwrap(),
        );
        if range.is_some() {
            response.headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {start}-{}/{length}", end - 1)).unwrap(),
            );
        }
        if method != Method::HEAD {
            response.body = Some(Box::new(Reader {
                shared: self.shared.clone(),
                changed,
                position: start,
                end,
                _permit: permit,
            }));
        }
        response
    }

    async fn proxy_bypass(
        &self,
        method: Method,
        client_headers: HeaderMap,
        permit: OwnedSemaphorePermit,
    ) -> CachedMediaResponse {
        if !matches!(method, Method::GET | Method::HEAD) {
            return empty(405);
        }
        let Some(url) = self.shared.data.lock().unwrap().bypass_url.clone() else {
            return empty(502);
        };
        let mut headers = self.hint.request_headers();
        for name in [
            header::HOST,
            header::RANGE,
            header::ACCEPT_ENCODING,
            header::CONTENT_LENGTH,
            header::CONNECTION,
            header::TRANSFER_ENCODING,
            header::UPGRADE,
            header::TRAILER,
            header::TE,
        ] {
            headers.remove(name);
        }
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );
        if let Some(range) = client_headers.get(header::RANGE) {
            headers.insert(header::RANGE, range.clone());
        }
        if let Some(if_range) = client_headers.get(header::IF_RANGE) {
            headers.insert(header::IF_RANGE, if_range.clone());
        }
        let request = self.http.request(method.clone(), url).headers(headers);
        let response = match tokio::time::timeout(Duration::from_secs(15), request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let _ = transport_failure("bypass_response_headers", &error);
                return empty(502);
            }
            Err(_) => return empty(504),
        };
        let status = response.status().as_u16();
        let mut response_headers = HeaderMap::new();
        for name in [
            header::CONTENT_TYPE,
            header::CONTENT_LENGTH,
            header::CONTENT_RANGE,
            header::ACCEPT_RANGES,
            header::ETAG,
            header::LAST_MODIFIED,
            header::CACHE_CONTROL,
        ] {
            if let Some(value) = response.headers().get(&name) {
                response_headers.insert(name, value.clone());
            }
        }
        CachedMediaResponse {
            status,
            headers: response_headers,
            body: (method != Method::HEAD).then(|| {
                Box::new(ProxiedBody {
                    response,
                    idle_timeout: self.idle_timeout,
                    _permit: permit,
                }) as Box<dyn CachedMediaBody>
            }),
        }
    }
}

impl Drop for AudioCache {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn empty(status: u16) -> CachedMediaResponse {
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    CachedMediaResponse {
        status,
        headers,
        body: None,
    }
}

async fn download(
    http: &reqwest::Client,
    url: &str,
    shared: &Shared,
    limit: usize,
    idle_timeout: Duration,
) -> Result<(), ()> {
    // Drain each downloader-sized response promptly. Holding an origin body
    // open across playback pauses/seeks can trigger peer resets. Local readers
    // still receive bytes immediately; demand gates only the next HTTP range.
    let mut changed = shared.changed.subscribe();
    let started = std::time::Instant::now();
    let mut index = shared.data.lock().unwrap().resume_index;
    loop {
        let (known_length, present) = {
            let data = shared.data.lock().unwrap();
            (data.length, data.blocks.get(index).map_or(0, Vec::len))
        };
        if known_length != 0 && present == BLOCK.min(known_length - index * BLOCK) {
            if shared
                .data
                .lock()
                .unwrap()
                .blocks
                .iter()
                .map(Vec::len)
                .sum::<usize>()
                == known_length
            {
                let mut data = shared.data.lock().unwrap();
                if data.phase == Phase::Cancelled {
                    return Ok(());
                }
                // Reaching this branch on restart means the prior response
                // failed after filling a hole but before validating its tail.
                // Only the normal, fully drained response path may complete.
                data.integrity_failed = true;
                return Err(fetch_failure("unvalidated_complete_cache"));
            }
            index = next_wanted(shared, &mut changed).await?;
            continue;
        }
        let start = index * BLOCK + present;
        let end = if known_length == 0 {
            index * BLOCK + UPSTREAM_RANGE
        } else {
            (index * BLOCK + UPSTREAM_RANGE).min(known_length)
        };
        shared.data.lock().unwrap().resume_index = index;
        let response_started = std::time::Instant::now();
        let mut headers = shared.hint.request_headers();
        // Cache framing always wins over extractor-provided values. The SDK
        // rejects these names already; removing them here keeps the transport
        // boundary safe if that validation is ever relaxed.
        for name in [
            header::HOST,
            header::RANGE,
            header::ACCEPT_ENCODING,
            header::CONTENT_LENGTH,
            header::CONNECTION,
            header::TRANSFER_ENCODING,
            header::UPGRADE,
            header::TRAILER,
            header::TE,
        ] {
            headers.remove(name);
        }
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );
        headers.insert(
            header::RANGE,
            HeaderValue::from_str(&format!("bytes={start}-{}", end - 1)).unwrap(),
        );
        let request = http.get(url).headers(headers);
        let mut response = tokio::time::timeout(Duration::from_secs(15), request.send())
            .await
            .map_err(|_| fetch_failure("response_headers_timeout"))?
            .map_err(|error| transport_failure("response_headers", &error))?;
        if response.status().as_u16() == 200 && known_length == 0 {
            // Do not silently turn a range-ignoring origin into an eager download.
            let mut data = shared.data.lock().unwrap();
            if data.phase != Phase::Cancelled {
                data.phase = Phase::Bypass;
                data.bypass_url = Some(url.to_string());
                shared.changed.send_replace(());
            }
            return Ok(());
        }
        if response.status().as_u16() != 206 {
            tracing::warn!(
                status = response.status().as_u16(),
                range_start = start,
                range_end = end - 1,
                known_length,
                elapsed_seconds = started.elapsed().as_secs(),
                http_version = ?response.version(),
                "current audio cache HTTP failure"
            );
            return Err(());
        }
        let range_header = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| fetch_failure("missing_content_range"))?;
        if known_length == 0 && range_header.ends_with("/*") {
            let mut data = shared.data.lock().unwrap();
            if data.phase != Phase::Cancelled {
                data.phase = Phase::Bypass;
                data.bypass_url = Some(url.to_string());
                shared.changed.send_replace(());
            }
            return Ok(());
        }
        let (first, last, length) =
            content_range(range_header).map_err(|_| fetch_failure("invalid_content_range"))?;
        if length == 0 || length > limit {
            if known_length != 0 {
                return Err(fetch_failure("changed_or_oversized_length"));
            }
            let mut data = shared.data.lock().unwrap();
            if data.phase != Phase::Cancelled {
                data.phase = Phase::Bypass;
                data.bypass_url = Some(url.to_string());
                shared.changed.send_replace(());
            }
            return Ok(());
        }
        if first != start
            || last + 1 != end.min(length)
            || (known_length != 0 && length != known_length)
            || response
                .content_length()
                .is_some_and(|n| n != (last + 1 - first) as u64)
        {
            return Err(fetch_failure("mismatched_range_or_length"));
        }
        {
            let mut data = shared.data.lock().unwrap();
            if data.phase == Phase::Cancelled {
                return Ok(());
            }
            if known_length == 0 {
                data.length = length;
                data.blocks = (0..length.div_ceil(BLOCK)).map(|_| Vec::new()).collect();
                data.content_type = response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .cloned()
                    .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
                data.phase = Phase::Streaming;
                shared.changed.send_replace(());
            }
        }
        let mut pending = bytes::Bytes::new();
        let mut received = 0;
        loop {
            shared.data.lock().unwrap().resume_index = index;
            let block_end = ((index + 1) * BLOCK).min(length);
            let expected = block_end - index * BLOCK;
            let mut response_offset = start.saturating_sub(index * BLOCK);
            loop {
                if response_offset == expected {
                    break;
                }
                if pending.is_empty() {
                    // No timeout runs between completed ranges while paused.
                    pending = tokio::time::timeout(idle_timeout, response.chunk())
                        .await
                        .map_err(|_| fetch_failure("body_read_timeout"))?
                        .map_err(|error| {
                            tracing::warn!(
                                range_start = start,
                                range_end = end - 1,
                                received_bytes = received,
                                response_age_seconds = response_started.elapsed().as_secs(),
                                "audio cache interrupted response"
                            );
                            transport_failure("body_read", &error)
                        })?
                        .ok_or_else(|| fetch_failure("truncated_body"))?;
                    received += pending.len();
                    if received > last + 1 - first {
                        shared.data.lock().unwrap().integrity_failed = true;
                        return Err(fetch_failure("oversized_body"));
                    }
                }
                let mut data = shared.data.lock().unwrap();
                if data.phase == Phase::Cancelled {
                    return Ok(());
                }
                let count = pending.len().min(expected - response_offset);
                let block = &mut data.blocks[index];
                if response_offset > block.len() {
                    data.integrity_failed = true;
                    return Err(fetch_failure("cache_range_gap"));
                }
                let overlap = count.min(block.len() - response_offset);
                if block[response_offset..response_offset + overlap] != pending[..overlap] {
                    data.integrity_failed = true;
                    return Err(fetch_failure("changed_cached_bytes"));
                }
                let incoming = pending.split_to(count);
                block.extend_from_slice(&incoming[overlap..]);
                response_offset += count;
                shared.changed.send_replace(());
            }
            if block_end == last + 1 {
                // Cached bytes alone do not prove response integrity. A failed
                // tail validation must not become Complete on an in-place retry.
                shared.data.lock().unwrap().integrity_failed = true;
                if !pending.is_empty()
                    || tokio::time::timeout(idle_timeout, response.chunk())
                        .await
                        .map_err(|_| fetch_failure("body_end_timeout"))?
                        .map_err(|error| transport_failure("body_end", &error))?
                        .is_some()
                {
                    return Err(fetch_failure("unexpected_body_tail"));
                }
                shared.data.lock().unwrap().integrity_failed = false;
            }
            {
                let mut data = shared.data.lock().unwrap();
                if data.phase == Phase::Cancelled {
                    return Ok(());
                }
                if block_end == last + 1
                    && data.blocks.iter().map(Vec::len).sum::<usize>() == length
                {
                    data.phase = Phase::Complete;
                    shared.hint.mark_complete();
                    shared.changed.send_replace(());
                    tracing::info!(bytes = length, "current audio cache complete");
                    return Ok(());
                }
            }
            // Finish the open response even without decoder demand. Do not
            // issue the next bounded range until a local reader needs it.
            if block_end < last + 1 {
                index += 1;
                continue;
            }
            let next = next_wanted(shared, &mut changed).await?;
            index = next;
            if index * BLOCK != block_end || block_end == last + 1 {
                break;
            }
        }
    }
}

fn fetch_failure(kind: &'static str) {
    tracing::warn!(kind, "audio cache fetch failure detail");
}

fn transport_failure(stage: &'static str, error: &reqwest::Error) {
    // reqwest's Display/Debug may contain a signed media URL: only safe flags.
    let (io_kind, os_error) = transport_io_cause(error);
    tracing::warn!(
        stage,
        timeout = error.is_timeout(),
        connect = error.is_connect(),
        body = error.is_body(),
        decode = error.is_decode(),
        io_kind = ?io_kind,
        os_error,
        "audio cache transport failure"
    );
}

fn transport_io_cause(
    error: &(dyn std::error::Error + 'static),
) -> (Option<std::io::ErrorKind>, Option<i32>) {
    let mut source = Some(error);
    // Bound traversal even for an incorrectly self-referencing external error.
    for _ in 0..16 {
        let Some(error) = source else {
            break;
        };
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            return (Some(io.kind()), io.raw_os_error());
        }
        source = error.source();
    }
    (None, None)
}

async fn next_wanted(shared: &Shared, changed: &mut watch::Receiver<()>) -> Result<usize, ()> {
    loop {
        changed.borrow_and_update();
        {
            let mut data = shared.data.lock().unwrap();
            if data.phase == Phase::Cancelled {
                return Err(());
            }
            while let Some(index) = data.wanted.pop_front() {
                if data.blocks[index].len() != BLOCK.min(data.length - index * BLOCK) {
                    return Ok(index);
                }
            }
        }
        changed.changed().await.map_err(|_| ())?;
    }
}

fn content_range(value: &str) -> Result<(usize, usize, usize), ()> {
    let (range, total) = value
        .strip_prefix("bytes ")
        .ok_or(())?
        .split_once('/')
        .ok_or(())?;
    let (start, end) = range.split_once('-').ok_or(())?;
    let start = start.parse::<usize>().map_err(|_| ())?;
    let end = end.parse::<usize>().map_err(|_| ())?;
    let total = total.parse::<usize>().map_err(|_| ())?;
    if start > end || end >= total {
        return Err(());
    }
    Ok((start, end, total))
}

/// Half-open range. Multiple ranges are intentionally unsupported.
fn byte_range(value: Option<&str>, length: usize) -> Result<(usize, usize), ()> {
    let Some(value) = value else {
        return Ok((0, length));
    };
    let (start, end) = value
        .strip_prefix("bytes=")
        .ok_or(())?
        .split_once('-')
        .ok_or(())?;
    if start.is_empty() {
        let suffix: usize = end.parse().map_err(|_| ())?;
        return if suffix == 0 {
            Err(())
        } else {
            Ok((length.saturating_sub(suffix), length))
        };
    }
    let start: usize = start.parse().map_err(|_| ())?;
    let end = if end.is_empty() {
        length
    } else {
        end.parse::<usize>()
            .map_err(|_| ())?
            .saturating_add(1)
            .min(length)
    };
    if start >= end {
        Err(())
    } else {
        Ok((start, end))
    }
}

struct Reader {
    shared: Arc<Shared>,
    changed: watch::Receiver<()>,
    position: usize,
    end: usize,
    _permit: OwnedSemaphorePermit,
}

struct ProxiedBody {
    response: reqwest::Response,
    idle_timeout: Duration,
    _permit: OwnedSemaphorePermit,
}

#[async_trait]
impl CachedMediaBody for ProxiedBody {
    async fn next_chunk(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        match tokio::time::timeout(self.idle_timeout, self.response.chunk()).await {
            Ok(Ok(chunk)) => Ok(chunk.map(|bytes| bytes.to_vec())),
            Ok(Err(error)) => {
                // reqwest errors may embed the signed origin URL. Keep it out
                // of the body error propagated through Hyper and diagnostics.
                transport_failure("bypass_body", &error);
                Err(std::io::Error::other("proxied audio response failed"))
            }
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "proxied audio response stalled",
            )),
        }
    }
}

#[async_trait]
impl CachedMediaBody for Reader {
    async fn next_chunk(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        loop {
            self.changed.borrow_and_update();
            {
                let mut data = self.shared.data.lock().unwrap();
                if matches!(data.phase, Phase::Failed | Phase::Cancelled) {
                    return Err(std::io::Error::other("audio cache unavailable"));
                }
                if self.position == self.end {
                    return Ok(None);
                }
                let index = self.position / BLOCK;
                let offset = self.position % BLOCK;
                let end = (index * BLOCK + data.blocks[index].len())
                    .min(self.end)
                    .min(self.position + CHUNK);
                if end > self.position {
                    let bytes = data.blocks[index][offset..offset + end - self.position].to_vec();
                    self.position = end;
                    return Ok(Some(bytes));
                }
                if !data.wanted.contains(&index) {
                    data.wanted.push_back(index);
                    self.shared.changed.send_replace(());
                    // Consume our own notification; only incoming data should wake us.
                    self.changed.borrow_and_update();
                }
            }
            self.changed
                .changed()
                .await
                .map_err(|_| std::io::Error::other("audio cache closed"))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn restart_cannot_complete_filled_cache_after_unvalidated_response_tail() {
        let hint = MediaCacheHint::default();
        let cache = AudioCache::new(
            reqwest::Client::new(),
            "http://invalid.invalid".into(),
            hint.clone(),
        );
        {
            let mut data = cache.shared.data.lock().unwrap();
            // A backfill filled the last hole, then the transport failed while
            // traversing an already cached tail. Byte count is not validation.
            data.phase = Phase::Streaming;
            data.length = BLOCK * 2;
            data.blocks = vec![vec![42; BLOCK], vec![42; BLOCK]];
            data.resume_index = 1;
        }
        assert!(download(
            &cache.http,
            &cache.url,
            &cache.shared,
            cache.limit,
            cache.idle_timeout
        )
        .await
        .is_err());
        assert!(cache.shared.data.lock().unwrap().integrity_failed);
        assert!(!hint.is_complete());
    }

    #[tokio::test]
    async fn backfill_drains_overlapping_cached_blocks_without_shifting_bytes() {
        let payload: Vec<u8> = (0..UPSTREAM_RANGE * 5 / 2)
            .map(|index| (index % 251) as u8)
            .collect();
        let origin = origin(&payload, Some(payload.len()), 200).await;
        origin.release.send(true).unwrap();
        let cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        assert_eq!(
            read_all(open(&cache, Some("bytes=0-9"), Method::GET).await).await,
            payload[..10]
        );
        let tail_start = UPSTREAM_RANGE * 3 / 2;
        let tail_range = format!("bytes={tail_start}-");
        assert_eq!(
            read_all(open(&cache, Some(&tail_range), Method::GET).await).await,
            payload[tail_start..]
        );
        assert!(!cache.hint.is_complete());
        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            payload
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while !cache.hint.is_complete() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            *origin.ranges.lock().unwrap(),
            [
                (0, UPSTREAM_RANGE),
                (tail_start, payload.len()),
                (UPSTREAM_RANGE, 2 * UPSTREAM_RANGE),
            ]
        );
    }

    #[tokio::test]
    async fn backfill_rejects_bytes_inconsistent_with_the_already_cached_prefix() {
        let payload = vec![7; UPSTREAM_RANGE * 5 / 2];
        let origin = origin(&payload, Some(payload.len()), 200).await;
        origin.release.send(true).unwrap();
        let (_ready, ready) = watch::channel(true);
        let hint = MediaCacheHint::default();
        hint.set_refresh(Arc::new(Refresh {
            url: origin.url.clone(),
            calls: AtomicUsize::new(0),
            ready,
        }));
        let cache = AudioCache::new(reqwest::Client::new(), origin.url.clone(), hint);
        read_all(open(&cache, Some("bytes=0-9"), Method::GET).await).await;
        let tail_start = UPSTREAM_RANGE * 3 / 2;
        let tail_range = format!("bytes={tail_start}-");
        read_all(open(&cache, Some(&tail_range), Method::GET).await).await;
        // Simulate bytes previously delivered by a different representation.
        // Backfill must compare its overlap, never silently overwrite it.
        cache.shared.data.lock().unwrap().blocks[tail_start / BLOCK][0] ^= 1;
        let mut response = open(&cache, None, Method::GET).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match response.body.as_mut().unwrap().next_chunk().await {
                    Ok(Some(_)) => {}
                    // Readers can finish from existing bytes before the
                    // downloader validates the tail; cache completion cannot.
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !cache.has_failed() {
                assert!(
                    !cache.hint.is_complete(),
                    "inconsistent overlap was accepted"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(cache.has_failed());
        assert!(cache.hint.is_failed());
        assert!(!cache.hint.is_complete());
    }

    #[test]
    fn transport_cause_extracts_only_safe_io_metadata() {
        #[derive(Debug)]
        struct Wrapper(std::io::Error);
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("error formatting can expose credentials")
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let error = Wrapper(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "private details",
        ));
        assert_eq!(
            transport_io_cause(&error),
            (Some(std::io::ErrorKind::ConnectionReset), None)
        );
        assert_eq!(
            transport_io_cause(&std::io::Error::from_raw_os_error(104)).1,
            Some(104)
        );
        assert_eq!(transport_io_cause(&std::fmt::Error), (None, None));
    }
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    struct Origin {
        url: String,
        count: Arc<AtomicUsize>,
        ranges: Arc<Mutex<Vec<(usize, usize)>>>,
        requests: Arc<Mutex<Vec<String>>>,
        release: watch::Sender<bool>,
        task: JoinHandle<()>,
    }
    impl Drop for Origin {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn origin(payload: &[u8], declared: Option<usize>, status: u16) -> Origin {
        origin_with_ranges(payload, declared, status, true).await
    }

    async fn origin_with_ranges(
        payload: &[u8],
        declared: Option<usize>,
        status: u16,
        supports_range: bool,
    ) -> Origin {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/audio", listener.local_addr().unwrap());
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let observed_ranges = ranges.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed_requests = requests.clone();
        let (release, rx) = watch::channel(false);
        let payload = payload.to_vec();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let payload = payload.clone();
                        let mut rx = rx.clone();
                        let observed = observed.clone();
                        let observed_ranges = observed_ranges.clone();
                        let observed_requests = observed_requests.clone();
                        children.spawn(async move {
                            let mut request = vec![0; 8192];
                            let n = socket.read(&mut request).await.unwrap();
                            observed.fetch_add(1, Ordering::SeqCst);
                            let request = String::from_utf8_lossy(&request[..n]).to_ascii_lowercase();
                            observed_requests.lock().unwrap().push(request.clone());
                            let range = request.lines().find_map(|l| l.strip_prefix("range: "));
                            let (status, length, content_range, payload) = if status == 200 && declared.is_some() && supports_range {
                                let total = declared.unwrap();
                                let (start, end) = byte_range(range, total).unwrap();
                                observed_ranges.lock().unwrap().push((start, end));
                                (206, Some(end - start), format!("Content-Range: bytes {start}-{}/{total}\r\n", end - 1),
                                    payload[start.min(payload.len())..end.min(payload.len())].to_vec())
                            } else { (status, declared, String::new(), payload) };
                            let length = length.map(|n| format!("Content-Length: {n}\r\n")).unwrap_or_default();
                            let header = format!("HTTP/1.1 {status} Test\r\n{length}{content_range}Content-Type: audio/mp4\r\nConnection: close\r\n\r\n");
                            if socket.write_all(header.as_bytes()).await.is_err() { return; }
                            let mid = payload.len() / 2;
                            if socket.write_all(&payload[..mid]).await.is_err() { return; }
                            while !*rx.borrow_and_update() {
                                if rx.changed().await.is_err() { return; }
                            }
                            let _ = socket.write_all(&payload[mid..]).await;
                        });
                    }
                    _ = children.join_next(), if !children.is_empty() => {}
                }
            }
        });
        Origin {
            url,
            count,
            ranges,
            requests,
            release,
            task,
        }
    }

    async fn open(cache: &AudioCache, range: Option<&str>, method: Method) -> CachedMediaResponse {
        let mut headers = HeaderMap::new();
        if let Some(range) = range {
            headers.insert(header::RANGE, HeaderValue::from_str(range).unwrap());
        }
        tokio::time::timeout(Duration::from_secs(2), cache.response(method, headers))
            .await
            .unwrap()
    }

    async fn read_all(mut response: CachedMediaResponse) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(2), async move {
            let mut out = Vec::new();
            if let Some(body) = &mut response.body {
                while let Some(chunk) = body.next_chunk().await.unwrap() {
                    out.extend(chunk);
                }
            }
            out
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn origin_requests_preserve_validated_context_and_cache_framing() {
        let origin = origin(b"abcdefghij", Some(10), 200).await;
        origin.release.send(true).unwrap();
        let hint = MediaCacheHint::default();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::USER_AGENT,
            HeaderValue::from_static("Recipe-Agent/1.0"),
        );
        headers.insert(
            header::REFERER,
            HeaderValue::from_static("https://www.youtube.com/"),
        );
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("session=in-memory-only"),
        );
        hint.set_request_headers(headers).unwrap();
        let cache = AudioCache::new(reqwest::Client::new(), origin.url.clone(), hint);

        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            b"abcdefghij"
        );
        let requests = origin.requests.lock().unwrap();
        let request = requests.first().expect("origin request");
        assert!(request.contains("user-agent: recipe-agent/1.0\r\n"));
        assert!(request.contains("referer: https://www.youtube.com/\r\n"));
        assert!(request.contains("cookie: session=in-memory-only\r\n"));
        assert!(request.contains("accept-encoding: identity\r\n"));
        assert!(request.contains(&format!("range: bytes=0-{}\r\n", UPSTREAM_RANGE - 1)));
    }

    #[tokio::test]
    async fn starts_before_download_finishes_then_replays_and_seeks_without_network() {
        let origin = origin(b"abcdefghij", Some(10), 200).await;
        let hint = MediaCacheHint::default();
        let cache = AudioCache::new(reqwest::Client::new(), origin.url.clone(), hint.clone());
        let mut first = open(&cache, None, Method::GET).await;
        assert_eq!(first.status, 200);
        assert_eq!(first.headers[header::CONTENT_LENGTH], "10");
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(2),
                first.body.as_mut().unwrap().next_chunk()
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
            b"abcde"
        );
        assert!(!hint.is_complete());
        assert_eq!(
            read_all(open(&cache, Some("bytes=1-3"), Method::GET).await).await,
            b"bcd"
        );
        origin.release.send(true).unwrap();
        assert_eq!(read_all(first).await, b"fghij");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !hint.is_complete() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        origin.task.abort(); // Offline repeat/seek must still work.
        for range in [None, Some("bytes=0-")] {
            assert_eq!(
                read_all(open(&cache, range, Method::GET).await).await,
                b"abcdefghij"
            );
        }
        let suffix = open(&cache, Some("bytes=-3"), Method::GET).await;
        assert_eq!(suffix.status, 206);
        assert_eq!(suffix.headers[header::CONTENT_RANGE], "bytes 7-9/10");
        assert_eq!(read_all(suffix).await, b"hij");
        assert_eq!(origin.count.load(Ordering::SeqCst), 1);
        let head = open(&cache, None, Method::HEAD).await;
        assert!(head.body.is_none());
        assert_eq!(head.headers[header::CONTENT_LENGTH], "10");
        assert_eq!(
            open(&cache, Some("bytes=10-"), Method::GET).await.status,
            416
        );
    }

    #[tokio::test]
    async fn cancel_releases_bytes_and_unblocks_readers_without_refetch() {
        let origin = origin(b"abcdefghij", Some(10), 200).await;
        let hint = MediaCacheHint::default();
        let cache = AudioCache::new(reqwest::Client::new(), origin.url.clone(), hint.clone());
        let mut tail = open(&cache, Some("bytes=8-"), Method::GET)
            .await
            .body
            .unwrap();
        cache.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), tail.next_chunk())
                .await
                .unwrap()
                .is_err()
        );
        assert!(cache.shared.data.lock().unwrap().blocks.is_empty());
        assert_eq!(cache.shared.data.lock().unwrap().blocks.capacity(), 0);
        assert!(hint.is_failed());
        assert_eq!(open(&cache, None, Method::GET).await.status, 502);
        assert_eq!(origin.count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unknown_or_oversized_sources_bypass_without_retained_audio() {
        for length in [None, Some(MAX_AUDIO_BYTES + 1)] {
            let origin = origin(b"abcdefghij", length, 200).await;
            let cache = AudioCache::new(
                reqwest::Client::new(),
                origin.url.clone(),
                MediaCacheHint::default(),
            );
            let response = open(&cache, None, Method::GET).await;
            // The deliberately permissive test origin answers 206 even to a
            // full request when it knows a length; the bypass must preserve
            // the upstream status rather than fabricate a redirect.
            assert!(matches!(response.status, 200 | 206));
            origin.release.send(true).unwrap();
            if length.is_none() {
                assert_eq!(read_all(response).await, b"abcdefghij");
            }
            assert!(cache.shared.data.lock().unwrap().blocks.is_empty());
            assert!(!cache.hint.is_complete());
        }
    }

    #[tokio::test]
    async fn stalled_bypass_body_releases_with_a_timeout() {
        let origin = origin(b"abcdefghij", None, 200).await;
        let mut cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        cache.idle_timeout = Duration::from_millis(30);
        let mut body = open(&cache, None, Method::GET).await.body.unwrap();
        assert_eq!(body.next_chunk().await.unwrap(), Some(b"abcde".to_vec()));
        let error = body.next_chunk().await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn truncated_or_rejected_audio_is_never_marked_complete() {
        for status in [200, 403, 429] {
            let origin = origin(b"short", Some(20), status).await;
            let cache = AudioCache::new(
                reqwest::Client::new(),
                origin.url.clone(),
                MediaCacheHint::default(),
            );
            origin.release.send(true).unwrap();
            let mut response = open(&cache, None, Method::GET).await;
            if let Some(body) = &mut response.body {
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        if body.next_chunk().await.is_err() {
                            break;
                        }
                    }
                })
                .await
                .unwrap();
            } else {
                assert_eq!(response.status, 502);
            }
            assert!(!cache.hint.is_complete());
            assert!(cache.hint.is_failed());
            assert!(cache.shared.data.lock().unwrap().blocks.is_empty());
        }
    }

    #[tokio::test]
    async fn later_block_http_failure_is_a_body_error_not_clean_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/audio", listener.local_addr().unwrap());
        let origin = tokio::spawn(async move {
            for index in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 8192];
                let count = socket.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..count]).to_ascii_lowercase();
                assert!(request.contains(&format!(
                    "range: bytes={}-{}",
                    index * UPSTREAM_RANGE,
                    (index + 1) * UPSTREAM_RANGE - 1
                )));
                if index == 0 {
                    let header = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {UPSTREAM_RANGE}\r\nContent-Range: bytes 0-{}/{}\r\nConnection: close\r\n\r\n",
                        UPSTREAM_RANGE - 1,
                        UPSTREAM_RANGE * 2
                    );
                    socket.write_all(header.as_bytes()).await.unwrap();
                    socket.write_all(&vec![42; UPSTREAM_RANGE]).await.unwrap();
                } else {
                    socket.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await.unwrap();
                }
            }
        });
        let cache = AudioCache::new(reqwest::Client::new(), url, MediaCacheHint::default());
        let mut response = open(&cache, None, Method::GET).await;
        assert_eq!(
            response.headers[header::CONTENT_LENGTH],
            (UPSTREAM_RANGE * 2).to_string()
        );
        let body = response.body.as_mut().unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), async {
            let mut received = 0;
            loop {
                match body.next_chunk().await {
                    Ok(Some(bytes)) => received += bytes.len(),
                    Ok(None) => panic!("failed cache must not report clean EOF"),
                    Err(_) => break received,
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(received, UPSTREAM_RANGE);
        assert!(cache.hint.is_failed());
        assert!(!cache.hint.is_complete());
        assert!(cache.has_failed());
        assert_eq!(open(&cache, None, Method::GET).await.status, 502);
        cache.cancel();
        assert!(
            !cache.has_failed(),
            "cancellation is not an upstream failure"
        );
        origin.await.unwrap();
    }

    #[tokio::test]
    async fn stalled_download_fails_and_releases_partial_bytes() {
        let origin = origin(b"abcdefghij", Some(10), 200).await;
        let mut cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        cache.idle_timeout = Duration::from_millis(100);
        let mut response = open(&cache, Some("bytes=8-"), Method::GET).await;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            response.body.as_mut().unwrap().next_chunk(),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert!(cache.hint.is_failed());
        assert_eq!(cache.shared.data.lock().unwrap().blocks.capacity(), 0);
    }

    #[test]
    fn range_validation_and_bounds() {
        assert_eq!(content_range("bytes 0-9/10"), Ok((0, 9, 10)));
        for value in [
            "bytes 0-10/10",
            "bytes 9-1/10",
            "bytes 0-9/*",
            "items 0-9/10",
        ] {
            assert!(content_range(value).is_err());
        }
        assert_eq!(byte_range(Some("bytes=2-99"), 10), Ok((2, 10)));
        assert_eq!(byte_range(Some("bytes=-99"), 10), Ok((0, 10)));
        for value in [
            "bytes=-0",
            "bytes=9-3",
            "bytes=0-1,3-4",
            "items=0-1",
            "bytes=20-",
            "bytes=a-b",
        ] {
            assert_eq!(byte_range(Some(value), 10), Err(()));
        }
    }

    #[tokio::test]
    async fn drains_open_response_and_reuses_cached_bytes_for_sparse_seeks() {
        let payload: Vec<u8> = (0..BLOCK * 5 + 13).map(|i| (i % 251) as u8).collect();
        let origin = origin(&payload, Some(payload.len()), 200).await;
        origin.release.send(true).unwrap();
        let cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        assert_eq!(
            read_all(open(&cache, Some("bytes=0-9"), Method::GET).await).await,
            payload[..10]
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            origin.count.load(Ordering::SeqCst),
            1,
            "one native-sized response covers this title"
        );
        let tail = format!("bytes={}-", BLOCK * 5);
        let (a, b) = tokio::join!(
            read_all(open(&cache, Some(&tail), Method::GET).await),
            read_all(open(&cache, Some(&tail), Method::GET).await)
        );
        assert_eq!(a, payload[BLOCK * 5..]);
        assert_eq!(a, b);
        assert_eq!(
            origin.count.load(Ordering::SeqCst),
            1,
            "concurrent reads reuse the promptly completed response"
        );
        assert_eq!(*origin.ranges.lock().unwrap(), vec![(0, payload.len())]);
        assert!(
            cache.hint.is_complete(),
            "the open response should finish even without more local demand"
        );
        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            payload
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while !cache.hint.is_complete() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(origin.count.load(Ordering::SeqCst), 1);
        origin.task.abort();
        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            payload
        );
        assert_eq!(origin.count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn sequential_reads_reuse_response_and_pause_is_not_a_network_stall() {
        let payload = vec![42; UPSTREAM_RANGE + BLOCK * 4 + 13];
        let origin = origin(&payload, Some(payload.len()), 200).await;
        origin.release.send(true).unwrap();
        let mut cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        cache.idle_timeout = Duration::from_millis(30);
        assert_eq!(
            read_all(open(&cache, Some(&format!("bytes=0-{}", BLOCK - 1)), Method::GET).await)
                .await,
            payload[..BLOCK]
        );
        // Finish the open response, then wait without another HTTP request.
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(!cache.has_failed());
        assert!(!cache.hint.is_complete());
        assert_eq!(
            cache
                .shared
                .data
                .lock()
                .unwrap()
                .blocks
                .iter()
                .map(Vec::len)
                .sum::<usize>(),
            UPSTREAM_RANGE,
            "idle cache drains exactly the open upstream response"
        );
        assert_eq!(origin.count.load(Ordering::SeqCst), 1);
        assert_eq!(
            read_all(open(&cache, Some(&format!("bytes={BLOCK}-")), Method::GET).await).await,
            payload[BLOCK..]
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while !cache.hint.is_complete() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            origin.count.load(Ordering::SeqCst),
            2,
            "crossing the completed upstream range opens only one new response"
        );
        assert_eq!(
            *origin.ranges.lock().unwrap(),
            [(0, UPSTREAM_RANGE), (UPSTREAM_RANGE, payload.len())]
        );
    }

    #[tokio::test]
    async fn truncated_continuous_response_fails_without_waiting_for_later_demand() {
        let origin = origin(&vec![42; BLOCK + 13], Some(BLOCK * 3), 200).await;
        origin.release.send(true).unwrap();
        let cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        let mut response = open(&cache, None, Method::GET).await;
        let body = response.body.as_mut().unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), async {
            let mut received = 0;
            loop {
                match body.next_chunk().await {
                    Ok(Some(bytes)) => received += bytes.len(),
                    Ok(None) => panic!("truncated response must not produce EOF"),
                    Err(_) => break received,
                }
            }
        })
        .await
        .unwrap();
        assert!(received <= BLOCK + 13);
        assert!(cache.has_failed());
        assert!(!cache.hint.is_complete());
        assert_eq!(origin.count.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    struct Refresh {
        url: String,
        calls: AtomicUsize,
        ready: watch::Receiver<bool>,
    }

    #[async_trait]
    impl vibecast_sdk::MediaSourceRefresh for Refresh {
        async fn refresh(&self) -> Option<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut ready = self.ready.clone();
            while !*ready.borrow_and_update() {
                ready.changed().await.ok()?;
            }
            Some(self.url.clone())
        }
    }

    #[derive(Debug)]
    struct ContextRefresh {
        url: String,
        headers: HeaderMap,
    }

    #[async_trait]
    impl vibecast_sdk::MediaSourceRefresh for ContextRefresh {
        async fn refresh(&self) -> Option<String> {
            Some(self.url.clone())
        }

        async fn refresh_with_context(&self) -> Option<vibecast_sdk::RefreshedMediaSource> {
            vibecast_sdk::RefreshedMediaSource::new(self.url.clone())
                .with_request_headers(self.headers.clone())
                .ok()
        }
    }

    #[tokio::test]
    async fn refresh_switches_url_and_request_headers_together() {
        let rejected = origin(b"", Some(0), 403).await;
        let payload = b"fresh-audio";
        let fresh = origin(payload, Some(payload.len()), 200).await;
        fresh.release.send(true).unwrap();
        let hint = MediaCacheHint::default();
        let mut initial_headers = HeaderMap::new();
        initial_headers.insert(
            header::USER_AGENT,
            HeaderValue::from_static("initial-agent"),
        );
        hint.set_request_headers(initial_headers).unwrap();
        let mut refreshed_headers = HeaderMap::new();
        refreshed_headers.insert(
            header::USER_AGENT,
            HeaderValue::from_static("refreshed-agent"),
        );
        refreshed_headers.insert(
            header::COOKIE,
            HeaderValue::from_static("session=refreshed"),
        );
        hint.set_refresh(Arc::new(ContextRefresh {
            url: fresh.url.clone(),
            headers: refreshed_headers,
        }));
        let cache = AudioCache::new(reqwest::Client::new(), rejected.url.clone(), hint);
        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            payload
        );
        assert!(rejected.requests.lock().unwrap()[0].contains("user-agent: initial-agent"));
        let requests = fresh.requests.lock().unwrap();
        assert!(requests[0].contains("user-agent: refreshed-agent"));
        assert!(requests[0].contains("cookie: session=refreshed"));
    }

    #[tokio::test]
    async fn refresh_preserves_reader_and_partial_bytes_before_decoder_eof() {
        let payload: Vec<u8> = (0..BLOCK * 3 + 19).map(|i| (i % 251) as u8).collect();
        let broken = origin(&payload[..BLOCK + 13], Some(payload.len()), 200).await;
        broken.release.send(true).unwrap();
        let fresh = origin(&payload, Some(payload.len()), 200).await;
        fresh.release.send(true).unwrap();
        let (ready, gate) = watch::channel(false);
        let refresh = Arc::new(Refresh {
            url: fresh.url.clone(),
            calls: AtomicUsize::new(0),
            ready: gate,
        });
        let hint = MediaCacheHint::default();
        hint.set_refresh(refresh.clone());
        let cache = AudioCache::new(reqwest::Client::new(), broken.url.clone(), hint.clone());
        let mut response = open(&cache, None, Method::GET).await;
        let body = response.body.as_mut().unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            while refresh.calls.load(Ordering::SeqCst) == 0 {
                tokio::select! {
                    result = body.next_chunk() => bytes.extend(result.unwrap().unwrap()),
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
        })
        .await
        .unwrap();
        assert!(
            !cache.has_failed(),
            "refresh must not close the playing response"
        );
        assert!(!hint.is_failed());
        assert_eq!(hint.recovery_attempts(), 1);
        assert_eq!(
            cache
                .shared
                .data
                .lock()
                .unwrap()
                .blocks
                .iter()
                .map(Vec::len)
                .sum::<usize>(),
            BLOCK + 13
        );
        ready.send(true).unwrap();
        while let Some(chunk) = tokio::time::timeout(Duration::from_secs(2), body.next_chunk())
            .await
            .unwrap()
            .unwrap()
        {
            bytes.extend(chunk);
        }
        assert_eq!(
            bytes, payload,
            "partial block must resume without duplicated/skipped bytes"
        );
        assert_eq!(*fresh.ranges.lock().unwrap(), [(BLOCK + 13, payload.len())]);
        assert_eq!(refresh.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            payload
        );
    }

    #[tokio::test]
    async fn refresh_rejects_changed_length_and_reserves_decoder_reload() {
        let broken = origin(&vec![42; BLOCK + 13], Some(BLOCK * 3), 200).await;
        broken.release.send(true).unwrap();
        let wrong = origin(&vec![42; BLOCK * 3 + 1], Some(BLOCK * 3 + 1), 200).await;
        wrong.release.send(true).unwrap();
        let (_ready, gate) = watch::channel(true);
        let refresh = Arc::new(Refresh {
            url: wrong.url.clone(),
            calls: AtomicUsize::new(0),
            ready: gate,
        });
        let hint = MediaCacheHint::default();
        hint.set_refresh(refresh.clone());
        let cache = AudioCache::new(reqwest::Client::new(), broken.url.clone(), hint.clone());
        let mut response = open(&cache, None, Method::GET).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match response.body.as_mut().unwrap().next_chunk().await {
                    Ok(Some(_)) => {}
                    Err(_) => break,
                    Ok(None) => panic!("changed representation cannot be spliced into old bytes"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(refresh.calls.load(Ordering::SeqCst), 1);
        assert_eq!(hint.recovery_attempts(), 1);
        assert!(cache.has_failed());
        assert_eq!(wrong.count.load(Ordering::SeqCst), 1);
        assert!(hint.try_recovery(), "one decoder reload must remain");
        assert!(!hint.try_recovery(), "no third recovery round");
    }

    #[tokio::test]
    async fn cancellation_aborts_pending_refresh_without_new_origin_request() {
        let broken = origin(b"x", Some(20), 200).await;
        broken.release.send(true).unwrap();
        let fresh = origin(&[42; 20], Some(20), 200).await;
        let (ready, gate) = watch::channel(false);
        let refresh = Arc::new(Refresh {
            url: fresh.url.clone(),
            calls: AtomicUsize::new(0),
            ready: gate,
        });
        let hint = MediaCacheHint::default();
        hint.set_refresh(refresh.clone());
        let cache = AudioCache::new(reqwest::Client::new(), broken.url.clone(), hint.clone());
        let mut response = open(&cache, None, Method::GET).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while refresh.calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cache.cancel();
        ready.send(true).unwrap();
        assert!(response.body.as_mut().unwrap().next_chunk().await.is_err());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(fresh.count.load(Ordering::SeqCst), 0);
        assert_eq!(hint.recovery_attempts(), 1);
        assert!(hint.is_failed());
    }

    #[tokio::test]
    async fn initial_forbidden_response_recovers_inside_same_pending_request() {
        let rejected = origin(b"", Some(0), 403).await;
        let fresh = origin(b"playable audio", Some(14), 200).await;
        fresh.release.send(true).unwrap();
        let (_ready, gate) = watch::channel(true);
        let refresh = Arc::new(Refresh {
            url: fresh.url.clone(),
            calls: AtomicUsize::new(0),
            ready: gate,
        });
        let hint = MediaCacheHint::default();
        hint.set_refresh(refresh.clone());
        let cache = AudioCache::new(reqwest::Client::new(), rejected.url.clone(), hint.clone());
        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            b"playable audio"
        );
        assert_eq!(refresh.calls.load(Ordering::SeqCst), 1);
        assert_eq!(hint.recovery_attempts(), 1);
        assert!(!hint.is_failed());
    }

    #[tokio::test]
    async fn inherited_selection_budget_reserves_remaining_round_for_decoder() {
        let rejected = origin(b"", Some(0), 403).await;
        let (_ready, gate) = watch::channel(true);
        let refresh = Arc::new(Refresh {
            url: rejected.url.clone(),
            calls: AtomicUsize::new(0),
            ready: gate,
        });
        let hint = MediaCacheHint::default();
        hint.set_recovery_attempts(1);
        hint.set_refresh(refresh.clone());
        let cache = AudioCache::new(reqwest::Client::new(), rejected.url.clone(), hint.clone());
        assert_eq!(open(&cache, None, Method::GET).await.status, 502);
        assert_eq!(refresh.calls.load(Ordering::SeqCst), 0);
        assert_eq!(hint.recovery_attempts(), 1);
        assert!(hint.try_recovery());
        assert!(!hint.try_recovery());
        hint.set_recovery_attempts(0);
        assert_eq!(
            hint.recovery_attempts(),
            2,
            "replacement must not replenish selection budget"
        );
    }

    #[tokio::test]
    async fn refreshed_range_ignoring_origin_bypasses_to_fresh_not_rejected_url() {
        let rejected = origin(b"", Some(0), 403).await;
        let fresh = origin_with_ranges(b"audio", Some(5), 200, false).await;
        let (_ready, gate) = watch::channel(true);
        let hint = MediaCacheHint::default();
        hint.set_refresh(Arc::new(Refresh {
            url: fresh.url.clone(),
            calls: AtomicUsize::new(0),
            ready: gate,
        }));
        let cache = AudioCache::new(reqwest::Client::new(), rejected.url.clone(), hint);
        let response = open(&cache, None, Method::GET).await;
        assert_eq!(response.status, 200);
        assert!(response.body.is_some());
        assert_eq!(fresh.count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn sequential_demand_crosses_upstream_range_boundary() {
        let payload = vec![42; UPSTREAM_RANGE + BLOCK + 7];
        let origin = origin(&payload, Some(payload.len()), 200).await;
        origin.release.send(true).unwrap();
        let cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            payload
        );
        assert_eq!(origin.count.load(Ordering::SeqCst), 2);
        assert_eq!(
            *origin.ranges.lock().unwrap(),
            [(0, UPSTREAM_RANGE), (UPSTREAM_RANGE, payload.len())]
        );
    }

    #[tokio::test]
    async fn cancellation_releases_response_while_waiting_for_demand() {
        let payload = vec![42; BLOCK * 4];
        let origin = origin(&payload, Some(payload.len()), 200).await;
        origin.release.send(true).unwrap();
        let cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        assert_eq!(
            read_all(open(&cache, Some(&format!("bytes=0-{}", BLOCK - 1)), Method::GET).await)
                .await,
            payload[..BLOCK]
        );
        // Session teardown releases retained bytes and any active response.
        tokio::task::yield_now().await;
        cache.cancel();
        assert!(cache.task.lock().unwrap().is_none());
        assert_eq!(cache.shared.data.lock().unwrap().blocks.capacity(), 0);
        assert!(cache.hint.is_failed());
        assert!(!cache.has_failed());
        assert_eq!(open(&cache, None, Method::GET).await.status, 502);
        assert_eq!(origin.count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn origin_ignoring_ranges_bypasses_instead_of_eager_download() {
        let origin = origin_with_ranges(b"abcdefghij", Some(10), 200, false).await;
        let hint = MediaCacheHint::default();
        let mut source_headers = HeaderMap::new();
        source_headers.insert(header::USER_AGENT, HeaderValue::from_static("recipe-agent"));
        source_headers.insert(
            header::COOKIE,
            HeaderValue::from_static("session=memory-only"),
        );
        hint.set_request_headers(source_headers).unwrap();
        let cache = AudioCache::new(reqwest::Client::new(), origin.url.clone(), hint);
        let response = open(&cache, None, Method::GET).await;
        assert_eq!(response.status, 200);
        assert!(response.body.is_some());
        assert!(cache.shared.data.lock().unwrap().blocks.is_empty());
        assert!(!cache.hint.is_complete());
        assert_eq!(origin.count.load(Ordering::SeqCst), 2);
        let requests = origin.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests
            .iter()
            .all(|request| request.contains("user-agent: recipe-agent")));
        assert!(requests
            .iter()
            .all(|request| request.contains("cookie: session=memory-only")));
    }

    #[tokio::test]
    #[ignore = "requires installed mpv; silent real-decoder buffering check"]
    async fn real_mpv_pause_does_not_leave_origin_response_open() {
        let audio_size = 180_u32 * 8_000 * 2;
        let mut wav = Vec::new();
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(audio_size + 36).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16_u32.to_le_bytes());
        for value in [1_u16, 1] {
            wav.extend_from_slice(&value.to_le_bytes());
        }
        for value in [8_000_u32, 16_000] {
            wav.extend_from_slice(&value.to_le_bytes());
        }
        for value in [2_u16, 16] {
            wav.extend_from_slice(&value.to_le_bytes());
        }
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&audio_size.to_le_bytes());
        wav.resize(44 + audio_size as usize, 0);
        let origin = origin(&wav, Some(wav.len()), 200).await;
        origin.release.send(true).unwrap();
        let cache = Arc::new(AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        ));
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_send_buffer_size(64 * 1024).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(128).unwrap();
        let address = listener.local_addr().unwrap();
        let serving = cache.clone();
        let server = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let cache = serving.clone();
                        children.spawn(async move {
                            let mut input = vec![0; 8192];
                            let n = socket.read(&mut input).await.unwrap();
                            let input = String::from_utf8_lossy(&input[..n]).to_ascii_lowercase();
                            let range = input.lines().find_map(|l| l.strip_prefix("range: "));
                            let mut response = open(&cache, range, Method::GET).await;
                            let mut headers = format!("HTTP/1.1 {} Test\r\nConnection: close\r\n", response.status);
                            for (key, value) in response.headers.iter() {
                                headers.push_str(&format!("{key}: {}\r\n", value.to_str().unwrap()));
                            }
                            headers.push_str("\r\n");
                            if socket.write_all(headers.as_bytes()).await.is_err() { return; }
                            if let Some(body) = &mut response.body {
                                while let Ok(Some(bytes)) = body.next_chunk().await {
                                    if socket.write_all(&bytes).await.is_err() { return; }
                                }
                            }
                        });
                    }
                    _ = children.join_next(), if !children.is_empty() => {}
                }
            }
        });
        let mut player = std::process::Command::new("mpv")
            .args([
                "--no-config",
                "--ao=null",
                "--vo=null",
                "--pause",
                "--cache=yes",
                "--cache-secs=30",
                "--demuxer-max-bytes=512KiB",
                "--stream-buffer-size=64KiB",
            ])
            .arg(format!("http://{address}/cache/audio.wav"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let bytes: usize = cache
            .shared
            .data
            .lock()
            .unwrap()
            .blocks
            .iter()
            .map(Vec::len)
            .sum();
        eprintln!("paused real decoder retained {bytes}/{} bytes", wav.len());
        player.kill().unwrap();
        player.wait().unwrap();
        server.abort();
        assert_eq!(
            bytes,
            wav.len(),
            "finish the current native-sized response even while mpv is paused"
        );
        assert!(cache.hint.is_complete());
        assert_eq!(origin.count.load(Ordering::SeqCst), 1);
    }
}
