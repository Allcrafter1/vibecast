# Vibecast — Cast Audio Receiver Lab frontend

This is the **audio-oriented frontend fork** used by
[Cast Audio Receiver Lab](https://github.com/Allcrafter1/cast-audio-receiver-lab).
It is built on [Nils Emil Svensson's Vibecast](https://github.com/emilsvennesson/vibecast),
not an independently developed Cast implementation. Thank you to the upstream
author and contributors for the protocol, application and player architecture
on which this project depends.

The maintained project branch is **`cast-audio-receiver`**. The `main` branch
retains the upstream snapshot; upstream releases, Homebrew packages and container
images are **not builds of this fork**. Product releases and deployment guidance
belong in the linked main project. If that repository is still private, its
public release has not yet been completed.

## What this repository does

Vibecast supplies the Rust CastV2 transport, certificate handling, mDNS/device
discovery, per-player receivers, application routing and player bridge. The fork
adapts that foundation for an experimental audio receiver, focused on YouTube
Music and supported direct-media Cast loads.

Our changes include:

- audio-speaker advertisement and stable installation-scoped identities;
- YouTube audio-source resolution, queue/preloading and session/control fixes;
- a Default Media Receiver for supported direct-media Cast loads;
- playback, volume, position, end-state and artwork feedback improvements;
- an internal player bridge, bound to loopback by default, with the inherited
  browser player disabled in normal builds.

The Python supervisor, speaker management UI, local mpv output, AirPlay sender
integration, DLNA/Sonos adapters and Home Assistant packaging live in the **main
project**, not here. Running this Rust binary alone does not install that product
or create its management interface. Each connected output adapter registers a
player, which becomes a separately advertised Cast speaker.

## Build and integration

The main project's lock records select an immutable source revision. Follow
those records for a product build, rather than blindly following the newest
branch commit. The reviewed product toolchain is Rust 1.98.1:

```sh
cargo +1.98.1 build --locked --release -p vibecast-cli
```

The executable is `target/release/vibecast`. The product supervisor supplies its
data directory, authentication bundle and registered player adapters. The normal
bridge is `ws://127.0.0.1:8010/player`; it is not a second management UI. Keep it
internal. The earlier dev13 bridge overlay is incorporated into this branch;
do not apply it again on top of this branch.

## Scope and limitations

This is not an official Google Cast device or a universal replacement for all
Cast receiver applications. It does not use the official Google Cast SDK.
Google Home adoption/groups and Spotify Cast are not supported product goals.
Inherited app crates, Android/Kodi code and upstream documentation remain in the
tree; their presence does not establish support or testing in the audio product.

Compatible authentication material must be provided separately. No private
bundle or keys are included here. Service changes or identity revocation can
break reception independently of software updates. A successful build is not
evidence of compatibility with every sender, renderer or future service version.

Much of the product integration was developed with AI assistance, including
GPT/Astra, and iterated through automated and user-operated playback tests.
Independent reviews, fixes and contributions are welcome. Please do not include
private keys, account tokens, pairing data or signed media URLs in issues.

## YouTube sender takeover and resolver validation

An ordinary Cast `CONNECT` is also used by status observers; it must not
transfer ownership or rotate the Lounge screen. The failed 2026-10-04 test
build treated those subscriptions as takeovers, causing two observers to
repeatedly eject each other and preventing playback.

The corrected implementation claims ownership on the app-specific
`getMdxSessionStatus` handshake (or a new `LAUNCH`). Observer status requests
remain available; observer reconnects and closes cannot take over or stop
the owner. On takeover, the old controller is closed, its Lounge connection
is replaced and pending playback resolution is stopped. The new controller
receives a fresh screen identity. The desktop `previous` command is now parsed.
The displaced sender also receives a targeted `receiver-0` status with its
application removed before its app channels close, so the browser can end its
Cast route. This notification is not broadcast to the incoming controller.

When a new `LAUNCH` replaces a running application, receiver status now also
announces the old application's removal before the correlated new launch
response. Previously that intermediate state was skipped. Chromium keeps a
pending old-route removal during launch; leaving it unreported can interfere
with establishing the replacement route. This lifecycle correction is covered
by a protocol-order regression test. The observed mobile-to-desktop first-try
hang (no MDX request and an offline cached Lounge screen) still requires live
validation with this correction; cache mismatch alone does not prove its cause.

Device feedback subsequently confirmed switching in both directions. A separate
slow-start investigation found HTTP 429 watch metadata responses and approximately
seven-second external extraction attempts. The mobile-web fallback could report
success with a media URL returning HTTP 403, causing an immediate player failure
and another full resolution attempt. It now uses `--check-formats`, like the
primary extractor. A process-level regression test covers rejection of an
unplayable fallback and preservation of a playable one. This prevents false
success/retry cycles; it does not remove YouTube's external throttling or promise
instant resolution. Subsequent device acceptance confirmed playback and fast
resolution (about 2.0–2.3 seconds), with no rate-limit or fallback events in those
attempts. This reflects the primary path recovering, not a guaranteed speedup
from fallback validation. HLS playlists remain direct manifest URLs, never
progressive audio-cache entries; regression coverage preserves that boundary.

Regression tests cover repeated observer subscriptions/status queries,
old-controller reconnects, ownership transfer, old-controller STOP rejection,
and multiple logical senders on one connection. Desktop/mobile playback and
takeover in both directions have now passed device acceptance. Previous and
owner-stop semantics have regression coverage; automated tests alone do not
establish every sender interaction.

## Origin and licence

The audio work starts from upstream commit
[`b4616f8f399be706a1409ed21922aa2df892e303`](https://github.com/emilsvennesson/vibecast/commit/b4616f8f399be706a1409ed21922aa2df892e303).
Git history preserves that origin and the subsequent integration changes.

This fork retains Vibecast's **MIT licence** and copyright notice: see
[`LICENSE`](LICENSE). The Chromium Cast envelope schema retains its
[BSD notice](crates/vibecast-proto/proto/LICENSE); other third-party files retain
their own notices. The separate Python/product repository uses GPL-3.0-or-later;
that does not relicense Vibecast or imply endorsement by upstream authors.
