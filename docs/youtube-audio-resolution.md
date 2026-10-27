# YouTube audio resolution and device acceptance

Status: **accepted for Cast Audio Receiver Lab 0.7.0-dev1 (2026-10-27)**.
The device checklist below passed on the laptop receiver before publication.

## Resolution and prefetch model

The receiver keeps one speculative result: the exact next queue item. After the
current selection has loaded, it asks YouTube's InnerTube `/next` endpoint for
the neighbouring queue entries using the current list ID, index, opaque player
parameters, CTT and active MDX remote-device IDs. Returned CTT and parameters
are accepted only for the video and queue slot named by that endpoint.

The next item is then resolved with its derived CTT when one is available. If
navigation lookup supplies no credential, anonymous resolution remains a
best-effort fallback. Its result is isolated from a later explicit selection:

- an explicit CTT always starts a fresh credential-bound resolution;
- a failed anonymous prefetch does not become a cached terminal failure for a
  later selection with changed context;
- an exact, still-current prefetched result may be reused at any requested seek
  position, but newer sender-supplied parameters take precedence;
- a different title, queue or CTT never consumes the speculative result;
- Stop, disconnect, takeover and a superseding selection cancel obsolete work.

Queue additions and reorders retain context only for the demonstrably unchanged
active item. Previous/Next use known context only when video ID, slot and queue
generation agree. The worker reports the effective context back to the Lounge
connection, which includes it in `nowPlaying` and reuses the matching CTT for a
bounded recovery of the same title.

The InnerTube request was implemented locally and checked against
[patrickkfkan/yt-cast-receiver](https://github.com/patrickkfkan/yt-cast-receiver)
and its
[`DefaultPlaylistRequestHandler`](https://github.com/patrickkfkan/yt-cast-receiver/blob/master/src/lib/app/DefaultPlaylistRequestHandler.ts)
as protocol/behaviour references. No source code was copied into this project.

## Download recipe and credentials

yt-dlp remains the checked external fallback rather than a second playback
engine. It returns a recipe containing the selected media URL, content type,
object identity and a strict allowlist of request headers. The local progressive
audio proxy applies that recipe to initial, ranged and refreshed requests. A
range-ignoring or oversized origin is streamed through the same local proxy so
headers are not lost in a redirect; stalled response bodies have an idle
timeout.

CTTs, cookies and signed media URLs are session-memory data. Their values are
redacted from diagnostics and are not written to disk. The process starts
yt-dlp with `--ignore-config`, so an operator's global cookie configuration
cannot silently change receiver behaviour. A persistent YouTube account cookie
jar is intentionally not maintained: it would add account lifecycle, expiry,
refresh, storage and disclosure risks while still not replacing the
sender-provided per-item CTT. If authenticated yt-dlp operation is ever added,
it needs an explicit product-level credential design rather than incidental
state in the receiver.

Header-dependent HLS media is not sent directly to a player that cannot carry
the required request context. The receiver instead uses a viable InnerTube
stream when available or reports the original resolution failure. Ordinary HLS
without special headers retains the existing direct-manifest behaviour.

## Failure isolation and recovery

Transport or decoder failure triggers at most two fresh-media recovery rounds
for the same selection. Position, autoplay/pause state and the matching CTT are
preserved. Successful playback does not replenish this budget. A terminal
credentials-required failure suppresses identical Play retries until selection
context changes, while Reload/Play with new context resolves afresh. Failure of
one title does not discard the next queue entry or advance playback as though
the failed title had reached natural EOF.

Diagnostics record operation, video ID, status/error class, timing, selected
format identity and whether request context was present. They do not record CTT,
cookie or header values.

## Device acceptance

The following checks were completed on the laptop receiver. Retain this list as
the regression checklist for later resolver, queue or session changes:

1. Connect from YouTube Music Desktop, start the selected title, seek forward
   and backward, then use Previous and Next several times.
2. Let a playlist advance through at least three titles. Confirm the transition
   is quick, the correct title starts, and a prefetch failure does not skip it.
3. Put a known credential-requiring/age-restricted title in the next slot. Let
   its anonymous fallback fail if applicable, then select it normally and use
   Reload. The fresh CTT-bound start must succeed.
4. Enable repeat-one long enough to cross several boundaries. Confirm local
   replay, seek and Stop all remain responsive.
5. Take over desktop playback from mobile, then mobile playback from desktop.
   The displaced sender must leave the route and the incoming sender must work
   on its first attempt.
6. Join an existing desktop/mobile route from iPad and verify that its queue
   bootstrap takes ownership while the previous sender is removed. Briefly
   interrupt the iPad network or lock/unlock it: reconnecting within 60 seconds
   must retain playback and queue state without rotating the Lounge session.
7. Repeat ordinary playback, queue control, seek, repeat-one and Stop from the
   mobile YouTube Music app to guard the established mobile path.
8. Induce or observe one player/download interruption. Confirm no premature queue
   advance, no endless load, at most two automatic recovery rounds and a usable
   manual Play/Reload afterwards.

These checks passed before the main project pinned this frontend revision and
published `0.7.0-dev1`. Do not persist or attach raw CTTs, cookies or signed
media URLs when collecting future test evidence.
