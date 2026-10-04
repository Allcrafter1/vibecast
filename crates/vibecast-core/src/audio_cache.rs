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

    fn start(&self) {
        let mut task = self.task.lock().unwrap();
        if task.is_some() || self.shared.data.lock().unwrap().phase != Phase::Pending {
            return;
        }
        let shared = self.shared.clone();
        let http = self.http.clone();
        let url = self.url.clone();
        let limit = self.limit;
        let idle_timeout = self.idle_timeout;
        *task = Some(tokio::spawn(async move {
            if download(&http, &url, &shared, limit, idle_timeout)
                .await
                .is_err()
            {
                let mut data = shared.data.lock().unwrap();
                if data.phase != Phase::Cancelled {
                    data.phase = Phase::Failed;
                    data.blocks = Vec::new();
                    data.wanted.clear();
                    shared.hint.invalidate();
                    tracing::warn!("current audio cache fetch failed");
                    shared.changed.send_replace(());
                }
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
            let mut response = empty(307);
            if let Ok(location) = HeaderValue::from_str(&self.url) {
                response.headers.insert(header::LOCATION, location);
            } else {
                response.status = 502;
            }
            return response;
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
    // Fetch only requested blocks, not the rest of the song. A single worker
    // coalesces concurrent decoder reads; holes from seeking stay unallocated.
    let mut changed = shared.changed.subscribe();
    let mut index = 0;
    loop {
        let start = index * BLOCK;
        let known_length = shared.data.lock().unwrap().length;
        let end = if known_length == 0 {
            start + BLOCK
        } else {
            (start + BLOCK).min(known_length)
        };
        let request = http
            .get(url)
            .header(header::ACCEPT_ENCODING, "identity")
            .header(header::RANGE, format!("bytes={start}-{}", end - 1))
            .timeout(Duration::from_secs(120));
        let mut response = tokio::time::timeout(Duration::from_secs(15), request.send())
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;
        if response.status().as_u16() == 200 && known_length == 0 {
            // Do not silently turn a range-ignoring origin into an eager download.
            let mut data = shared.data.lock().unwrap();
            if data.phase != Phase::Cancelled {
                data.phase = Phase::Bypass;
                shared.changed.send_replace(());
            }
            return Ok(());
        }
        if response.status().as_u16() != 206 {
            tracing::warn!(
                status = response.status().as_u16(),
                "current audio cache HTTP failure"
            );
            return Err(());
        }
        let range_header = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|h| h.to_str().ok())
            .ok_or(())?;
        if known_length == 0 && range_header.ends_with("/*") {
            let mut data = shared.data.lock().unwrap();
            if data.phase != Phase::Cancelled {
                data.phase = Phase::Bypass;
                shared.changed.send_replace(());
            }
            return Ok(());
        }
        let (first, last, length) = content_range(range_header)?;
        if length == 0 || length > limit {
            if known_length != 0 {
                return Err(());
            }
            let mut data = shared.data.lock().unwrap();
            if data.phase != Phase::Cancelled {
                data.phase = Phase::Bypass;
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
            return Err(());
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
        let expected = last + 1 - first;
        while let Some(chunk) = tokio::time::timeout(idle_timeout, response.chunk())
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?
        {
            let mut data = shared.data.lock().unwrap();
            if data.phase == Phase::Cancelled {
                return Ok(());
            }
            let bytes = &mut data.blocks[index];
            if chunk.len() > expected - bytes.len() {
                return Err(());
            }
            bytes.extend_from_slice(&chunk);
            shared.changed.send_replace(());
        }
        {
            let mut data = shared.data.lock().unwrap();
            if data.phase == Phase::Cancelled {
                return Ok(());
            }
            if data.blocks[index].len() != expected {
                return Err(());
            }
            if data.blocks.iter().map(Vec::len).sum::<usize>() == length {
                data.phase = Phase::Complete;
                shared.hint.mark_complete();
                shared.changed.send_replace(());
                tracing::info!(bytes = length, "current audio cache complete");
                return Ok(());
            }
        }
        // Being paused/no demand is not a network stall. Wait without a timer.
        index = loop {
            changed.borrow_and_update();
            {
                let mut data = shared.data.lock().unwrap();
                if data.phase == Phase::Cancelled {
                    return Ok(());
                }
                let mut next = None;
                while let Some(i) = data.wanted.pop_front() {
                    if data.blocks[i].len() != BLOCK.min(length - i * BLOCK) {
                        next = Some(i);
                        break;
                    }
                }
                if let Some(i) = next {
                    break i;
                }
            }
            changed.changed().await.map_err(|_| ())?;
        };
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    struct Origin {
        url: String,
        count: Arc<AtomicUsize>,
        ranges: Arc<Mutex<Vec<(usize, usize)>>>,
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
                        children.spawn(async move {
                            let mut request = vec![0; 8192];
                            let n = socket.read(&mut request).await.unwrap();
                            observed.fetch_add(1, Ordering::SeqCst);
                            let request = String::from_utf8_lossy(&request[..n]).to_ascii_lowercase();
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
            assert_eq!(response.status, 307);
            assert_eq!(response.headers[header::LOCATION], origin.url);
            assert!(cache.shared.data.lock().unwrap().blocks.is_empty());
            assert!(!cache.hint.is_complete());
        }
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
    async fn demand_fetches_only_needed_blocks_and_retains_sparse_seeks() {
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
            "idle cache must not download ahead"
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
            2,
            "concurrent reads share one fetch"
        );
        assert_eq!(
            *origin.ranges.lock().unwrap(),
            vec![(0, BLOCK), (BLOCK * 5, payload.len())]
        );
        assert!(
            !cache.hint.is_complete(),
            "a hole is not a complete cached song"
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
        assert_eq!(origin.count.load(Ordering::SeqCst), 6);
        origin.task.abort();
        assert_eq!(
            read_all(open(&cache, None, Method::GET).await).await,
            payload
        );
        assert_eq!(origin.count.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn origin_ignoring_ranges_bypasses_instead_of_eager_download() {
        let origin = origin_with_ranges(b"abcdefghij", Some(10), 200, false).await;
        let cache = AudioCache::new(
            reqwest::Client::new(),
            origin.url.clone(),
            MediaCacheHint::default(),
        );
        let response = open(&cache, None, Method::GET).await;
        assert_eq!(response.status, 307);
        assert!(cache.shared.data.lock().unwrap().blocks.is_empty());
        assert!(!cache.hint.is_complete());
    }

    #[tokio::test]
    #[ignore = "requires installed mpv; silent real-decoder buffering check"]
    async fn real_mpv_does_not_eagerly_fetch_whole_song() {
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
        assert!(bytes > 0, "decoder did not load");
        assert!(
            bytes < wav.len() / 2,
            "paused decoder fetched {bytes}/{} bytes",
            wav.len()
        );
    }
}
