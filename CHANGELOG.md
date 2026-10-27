# Changelog

## Cast Audio Receiver Lab 0.7.0-dev1 — 2026-10-27

- Preserve YouTube playback and Lounge state across a lost Cast control socket
  for up to 60 seconds. Reconnect identity now uses the sender source together
  with its network host and SDK metadata, and supports the parallel sockets
  used by mobile Cast SDKs. Explicit app CLOSE, receiver STOP and replacement
  remain immediate.
- Recognize the iOS media-queue bootstrap as a guarded ownership request when
  it does not send `getMdxSessionStatus`. A real device takeover still removes
  and closes every proven connection of the previous controller, while passive
  CONNECT/GET_STATUS observers and delayed queue probes cannot seize or retain
  media control.
- Resolve the exact previous/next YouTube Music queue endpoints through
  InnerTube `/next` and keep CTT plus opaque player parameters bound to the
  video that supplied them. One-title prefetch now uses that derived context
  when available. A failed anonymous prefetch cannot poison a later regular
  selection carrying fresh CTT context.
- Preserve newer sender parameters when an exact prefetched next title is
  selected, reuse the prepared media at the requested start position, and
  carry the effective context into Lounge `nowPlaying` feedback and bounded
  playback recovery. Queue additions, reorders and sender takeovers no longer
  let stale item context select or overwrite another title.
- Treat yt-dlp output as an in-memory download recipe: URL, media type and a
  strict allowlist of required request headers travel together through the
  local audio proxy and refresh path. Cookie/CTT values remain redacted and are
  not persisted. Header-dependent media is rejected if no capable local proxy
  is available instead of being played with silently incomplete context.
- Keep progressive audio loading demand-driven with bounded range requests.
  Range-ignoring or oversized origins are proxied locally, including request
  context and an idle-body timeout, rather than redirected or eagerly retained.
- Keep automatic recovery limited to two fresh-media rounds per selection,
  preserve position and pause state, and isolate terminal failures to the
  affected title. Stop, disconnect and a newer selection still cancel obsolete
  resolution work.
- Expose queue navigation to Cast controllers and Home Assistant. Cast
  `QUEUE_UPDATE` next/previous requests now pass through the YouTube Lounge
  state machine, so they operate on the real YouTube Music queue rather than a
  local decoder shortcut. Play, pause, stop and seek use the same app-owned
  control path; receiver volume and mute remain synchronized with the output.
  Passive Home Assistant control does not claim sender ownership, while a
  displaced sender remains unable to control the session or global volume.
  Local controls continue to reach the queue/player during Lounge feedback
  stalls, reconnect backoff and rebind instead of waiting behind the network.
- Desktop/laptop, iPad and mobile sender acceptance passed on the test receiver,
  including queue navigation, seek, repeat-one, age-restricted playback,
  controller takeover and network reconnection. This revision is published as
  the frontend source for Cast Audio Receiver Lab `0.7.0-dev1`; product images
  and installation guidance remain in that project. See
  `docs/youtube-audio-resolution.md`.

## Audio receiver fork — 2026-09-26

- Reuse the current resolved stream for repeat-one EOF, avoiding another
  YouTube/yt-dlp lookup. Keep the existing next-title preparation intact.
  Reuse stays session-local and bounded to ten minutes from original resolution;
  Stop, another selection or a codec change invalidates it. No audio-file cache
  or additional speculative resolver is introduced. YouTube suite: 51 passed,
  one explicit live-network test ignored.

- Support YouTube Music's multi-state repeat control (`mlm`), initial playlist
  loop mode, `setLoopMode` and `onLoopModeChanged` feedback. Repeat One restarts
  at natural EOF; All wraps the queue. Manual Next still advances in One mode.
- Keep repeat session-local and route repeated selections through the existing
  playback/cancellation path. Add protocol and asynchronous EOF regressions.
- Validation: 149 tests passed across the seven affected frontend crates;
  one opt-in live resolver test ignored. Sender acceptance is separate.

## [0.1.0](https://github.com/emilsvennesson/vibecast/releases/tag/v0.1.0) (2026-07-09)

Initial release of **vibecast** — a native Google Cast receiver written in Rust
that turns any computer into a Chromecast.

### Features

* Full CastV2 TLS protocol: device authentication, heartbeat, and the receiver
  namespace.
* Advertises as a Chromecast over mDNS and the eureka `/setup/eureka_info`
  HTTP/HTTPS endpoints.
* **Per-player receivers** — each connected player (browser Shaka page, Kodi
  add-on, native frontend) registers its capabilities and gets its own dedicated
  Cast device advertising that player's real DRM systems, codecs, resolution,
  HDR, and HDCP.
* Embedded Shaka Player bridge over HTTP/WebSocket with DRM license and
  DASH/HLS manifest proxying + normalization.
* Bundled apps: SVT Play, TV4 Play, Viaplay, and Prime Video.
* Desktop server (the `vibecast` CLI, Linux/macOS) and a native Android TV
  frontend via a UniFFI facade.
* Kodi add-on client for boxes that prefer Kodi's player.

### Artifacts

* Linux binaries (`x86_64`, `aarch64`) and a macOS binary (Apple Silicon).
* Multi-arch container image on GHCR (`linux/amd64`, `linux/arm64`).
* Signed Android APK.
* Homebrew formula (`brew install emilsvennesson/vibecast/vibecast`).
