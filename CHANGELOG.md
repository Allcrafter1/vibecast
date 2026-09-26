# Changelog

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
