# StreamGateway

**A dropped connection should cost a reconnect, not a second prompt.**

StreamGateway is the system-design half of streaming an LLM response to a phone: a gateway that makes **one** upstream model call per stream and keeps a bounded replay log, and an iOS client that resumes with `Last-Event-ID`, drops duplicates, and refuses to build a transcript with a hole in it. Backpressure is handled by *cutting* a slow connection and letting it resume from the shared log, so one stalled phone never grows the server's memory.

It is written in Swift 6 with strict concurrency, has no dependencies, and builds on Linux (where a Vapor or Hummingbird host would run it) and on Apple platforms.

> **Demo app:** [resumable-stream-gateway-demo-app](https://github.com/rajatslakhina/resumable-stream-gateway-demo-app). Six network faults, one LLM answer, real Simulator screenshots.

---

## Why this matters

Token streaming is now the default UX for AI features, and mobile networks are hostile to long-lived HTTP responses. The phone switches from Wi-Fi to cellular, iOS suspends the app, a corporate proxy closes an "idle" socket. Most apps handle this the simple way: on any error they re-send the prompt.

That has three costs a lead should not accept by default:

1. **Money.** Every reconnect is a second billed generation. On a flaky train commute a single answer can be paid for three or four times.
2. **Correctness.** Sampling is non-deterministic, so the second attempt produces a *different* answer. The user watches the text they were reading get replaced.
3. **Load.** A cell-tower blip reconnects thousands of clients at once. If each reconnect is a new model call, an outage at the edge becomes an outage at the provider quota.

The fix is an old one from messaging systems: give every event a position, keep a bounded log, and let consumers resume from their last position. This repo turns that into a concrete gateway and client, with the trade-offs written down.

## The shape of it

```
 iOS app                                Gateway (one per region, or sticky-routed)          Model
┌──────────────────────────┐   POST    ┌───────────────────────────────────────────┐       ┌──────┐
│ ResumableStreamClient    │──open────▶│ StreamGateway (actor)                      │──1×──▶│Claude│
│  · TranscriptAssembler   │           │  · streamID = client idempotency key       │◀─SSE──│      │
│    (Codable checkpoint)  │   GET     │  · ReplayLog per stream (ring buffer,      │       └──────┘
│  · ReconnectPolicy       │◀─SSE──────│    bounded by frames AND bytes)            │
│    (decorrelated jitter) │ Last-     │  · per-connection queue = backpressure     │
│  · SSEParser (bounded)   │ Event-ID  │    valve: full queue ⇒ cut, client resumes │
└──────────────────────────┘           │  · retention, abandonment, admission       │
                                       └───────────────────────────────────────────┘
```

Every frame has a cursor `epoch:sequence`. The **sequence** is contiguous per stream. The **epoch** is random per stream *generation*, so a cursor from another stream, or from a stream that was evicted and re-opened under the same ID, is rejected instead of silently resuming at the wrong offset.

## Guarantees, and the test that holds each one

| Guarantee | Enforced by | Test |
|---|---|---|
| One model call per stream, however many reconnects or duplicate opens | `open` is idempotent on `streamID` | `testOpenIsSingleFlightAndIdempotent`, `testResumesAcrossDroppedConnectionsWithOneUpstreamCall` |
| A subscriber attaching mid-stream sees every frame after its cursor exactly once | replay and registration happen in one actor turn with no `await` between them | `testReplayToLiveSeamUnderAConcurrentWriter` (24 subscribers, concurrent writer) |
| A slow consumer is cut after exactly `subscriberQueueLimit` live frames, never silently skipped | the live buffer is `.bufferingOldest(subscriberQueueLimit)`: a full buffer rejects the *new* frame, the gateway ends the connection, the client resumes | `testSlowConsumerIsCutAndResumesWithoutLossOrDuplication`, `testResumedConnectionLiveLagIsCappedRegardlessOfBacklog`, `testStalledHostConnectionIsCutAndTheClientResumes` |
| The client never applies a frame out of order | `TranscriptAssembler.apply` only accepts `lastSequence + 1` | `testAMissingFrameIsAProtocolViolationNotACorruptTranscript` |
| At-least-once delivery is safe | duplicates (`sequence <= lastSequence`) are dropped and counted | `testAtLeastOnceDeliveryIsDeduplicated` |
| Evicted history is a `410`, never a transcript with a hole | `ReplayLog.replay` returns `.gap` | `testRejectionsAreSpecificAndCounted`, `testReplayGapIsReportedAsRestartRequired` |
| A connection that closes without a stop reason is a truncation, not a completion | `upstreamEnded` always appends `.error` | `testTruncationAndUpstreamErrorsAreNeverReportedAsDone` |
| Model output survives byte for byte, including `\r\n` | payloads are JSON-encoded inside `data:` | `testWireCodecRoundTripsArbitraryPayloadsAtArbitrarySplits` |
| Frame payload memory held by the gateway and its live connections has a computed bound (`worstCaseBytes`; see decision 4 for what it assumes of the host) | per-stream log caps, frame-size cap (even inside one huge grapheme), byte-capped terminal payloads, per-stream connection cap, stream admission control, immutable clamped configuration | `testEvictsByBytesButAlwaysKeepsTheNewestFrame`, `testOversizedDeltaIsSplitSoTheByteBudgetHolds`, `testSplitBoundsEvenASingleHugeGrapheme`, `testSubscriberCapRejectsWithoutDisturbingExistingConnections`, `testAdmissionControlEvictsFinishedStreamsButNeverLiveOnes`, `testGatewayConfigurationClampsAndWorstCaseFollowsTheFormula` |

Tests are only worth something if they fail when the code is wrong. `scripts/mutation-check.sh` re-introduces 12 specific bugs (accepting gaps, applying duplicates, skipping replay, silently dropping slow-consumer frames, ignoring the epoch, reporting truncation as done, an off-by-one in abandonment, a ring-buffer growth bug, resuming past evicted frames, a failure budget that never resets, CRLF as two line endings, ignoring the server's `retry:`). Each one must make the suite fail. It runs in CI.

## Design decisions

### 1. The client owns the stream ID, and the gateway makes one upstream call per ID

The client generates `streamID` before the first request and uses it for every reconnect. `open` is idempotent: a retried `POST` after a lost response *joins* the existing generation. This is the same idea as an idempotency key on a payment API, and for the same reason: the expensive side effect (a billed generation) must happen at most once.

*Rejected:* re-sending the prompt on reconnect (pays again and produces a different answer); deduplicating by a hash of the prompt (two identical prompts from the same user are legitimately two requests).

One deliberate simplification: reusing a `streamID` with a *different* prompt silently joins the existing stream (`testSameStreamIDWithADifferentUpstreamJoinsAndIgnoresIt`). Payment APIs reject that case with a 422. A production host should store a digest of the request with the stream and refuse a mismatch; the gateway leaves that to the host because it never sees the prompt, only the upstream.

### Restarting after a 410

A permanent rejection of a resume (`410 replayGap`, `410 unknownStream`, `409 cursorMismatch`, `400`) means *this generation* can't be resumed. The recovery is a new generation under a **new** `streamID`. Re-opening the old ID would join the retained, gapped stream and get the same 410 until retention expires (`testRestartAfterReplayGapNeedsANewStreamID`). This is the only path that re-sends the prompt, and it is explicit.

### 2. Cursors are `epoch:sequence`, not bare integers

A bare sequence number is ambiguous once a stream can be evicted and re-opened under the same ID: `Last-Event-ID: 42` would resume the *new* generation at frame 42 and splice two answers together. The random per-generation epoch makes that a `409 cursorMismatch`.

*Rejected:* byte offsets (they couple the cursor to the wire encoding); timestamps (not unique, not contiguous, so gaps are undetectable).

### 3. Backpressure is a cut, not a buffer

Each connection may fall at most `subscriberQueueLimit` live frames behind. When it does, the gateway ends that connection. The client already treats a closed connection as "resume from my last cursor", so the frames it missed come from the shared replay log, which exists anyway.

A resume delivers its whole backlog in one connection. The subscription is an `AsyncSequence` that yields the backlog from an array (which shares string storage with the log, so it costs array slots, not a second copy of the text) and then forwards a separate live stream whose buffer is exactly `subscriberQueueLimit`. Reading the backlog never turns into extra room for live lag (`testResumedConnectionLiveLagIsCappedRegardlessOfBacklog`).

Two earlier versions got this wrong, and the review history is worth keeping. The first paged replay by the live limit; in the demo's slow-consumer scenario the backlog grew during every reconnect's backoff faster than one page drained it, and the client was cut 22 times before it caught up. The second put backlog and live frames in one buffer of `backlog + limit`, so once the backlog was read, a resumed connection could fall `backlog + limit` live frames behind. Separating the two fixed both.

Connections per stream are capped too (`maxSubscribersPerStream`, default 8; the next one gets `429 tooManySubscribers`), because a connection that stops reading still pins the frames in its buffer after the log has evicted them. A connection holds its slot through a lease that lasts as long as the host keeps its subscription alive, including after the gateway has cut it and for replays of finished streams (`testLeasesOutliveTheCutUntilTheSubscriptionIsDropped`). The admission check runs after the replay-range check, so a resume that can never work gets its permanent 410 instead of a retryable 429.

The effect is that the text in server memory is bounded by the number of *streams* and a fixed fan-out, not by how many clients are slow.

*Rejected:*
- **Unbounded per-connection buffers.** One phone on a bad network holds a full copy of the answer in server memory, and a thousand of them take the gateway down.
- **Pausing the upstream.** You cannot usefully pause a model mid-generation: the provider keeps the request open, it can time out, and the faster consumers are held hostage by the slowest.
- **Dropping frames for the slow client** (`.bufferingNewest`). The cheapest option and the worst one: the transcript silently loses words. The mutation check includes exactly this bug.

### 4. The replay log is bounded twice, and running out of it is an explicit error

`ReplayLog` is a ring buffer capped by frame count *and* by bytes, so neither many tiny deltas nor a few huge ones can exceed the budget. Oversized upstream chunks are split to at most `maxFramePayloadBytes` per frame: on `Character` boundaries where possible, and on Unicode-scalar boundaries inside a single oversized grapheme (an "e" followed by a million combining marks is one `Character`). The client concatenates payloads, so the transcript is byte-identical either way. Terminal payloads (stop reasons, error messages) are capped at 256 bytes. The newest frame is always kept even if it alone exceeds the budget, because no subscriber has seen it yet.

When a client asks for frames that were evicted, the answer is `410 replayGap`. The client surfaces "restart the generation". *Rejected:* resuming from the oldest frame still held. It looks like graceful degradation and is actually a corrupted answer.

`GatewayConfiguration.worstCaseBytes` computes the bound in the log's own accounting (UTF-8 payload bytes plus a fixed 64-byte overhead per frame; allocator overhead is not modelled): per stream, the log plus, for each allowed connection, one stale replay backlog and `subscriberQueueLimit` live frames, times `maxStreams`. All configuration values are clamped in `init` and immutable afterwards (`with(…)` re-clamps), so the bound cannot be switched off after construction. Because every live subscription holds a lease, stuck host handlers can't grow memory past the bound; what they can do is use up a stream's connection slots, so later resumes get 429 until the handler is dropped. Hosts should therefore drop a subscription when its socket write stalls or the client disconnects (a write timeout). With the defaults it is large (17,994,304,000 bytes, about 18 GB, for 1,000 streams with 8 stalled connections each); size `maxStreams` and `maxSubscribersPerStream` for your fleet, not the defaults.

### 5. Truncation is never "done"

If the upstream connection closes cleanly without a stop reason, the gateway records `.error("upstream closed without a stop reason")`. An LLM stream that ends early looks exactly like one that finished, unless you require the explicit terminal event. Treating EOF as success is how apps end up showing half an answer as if it were the whole thing.

### 6. Abandoned streams are cancelled: tokens versus resumability

A live stream with no subscriber for `abandonTimeout` has its upstream call cancelled and is finished with `.error("abandoned: no subscriber")`, which late resumers also see. This is a real trade-off. A short timeout saves tokens when users close the app mid-answer; a long one lets a user who backgrounded the app for a phone call come back to a finished answer. The default is 120 s. Set it from your own data on how long users actually spend away mid-stream.

Retention and abandonment are enforced by `sweep()`, which runs on every `open` and `subscribe`. A quiet gateway gets no traffic to trigger it, so the host should also call `sweep()` on a timer (every few seconds is plenty). `sweep()` is O(streams).

### 7. JSON inside `data:`

SSE normalises line endings: `\r\n` and `\r` both arrive as `\n`. If model output is written raw into `data:` lines, code blocks with Windows line endings come back changed. Encoding the payload as a JSON string escapes every control character, so each frame is a single `data:` line and the transcript is byte-exact. `testRoundTripCheckFailsForARawTextEncoder` shows the round-trip check catching the raw approach.

### 8. Idle detection belongs to the transport

The client has no watchdog timer. `URLSessionConfiguration.timeoutIntervalForRequest` is already an *idle* timeout between packets, and the gateway host sends `: keepalive` comments (the parser surfaces them as `.comment`). *Rejected:* a second timer in the client, which duplicates the transport's and races it.

### 9. What this deliberately does not do: cross-instance replay

The replay log lives in one gateway process. To run several instances you need either **sticky routing by `streamID`** (consistent hashing at the load balancer, the cheap option) or a **shared log** (Redis Streams, NATS JetStream) so any instance can serve a resume. A gateway restart loses in-flight streams, and clients get `410 unknownStream` and restart cleanly rather than resuming wrongly. The `ReplayLog` boundary is where a shared store would plug in; I left it in-process so the core logic stays testable without infrastructure.

## Using it

```swift
// Package.swift
.package(url: "https://github.com/rajatslakhina/resumable-stream-gateway-kit.git", from: "1.0.0")
```

### Server side (host sketch, not compiled in this repo)

The gateway is transport-agnostic. A Vapor or Hummingbird host maps two routes onto it:

```swift
// POST /streams/:id  { prompt }  → 202, or rejection.httpStatus
try await gateway.open(streamID: id, upstream: ClaudeUpstream(prompt: prompt))

// GET /streams/:id   Last-Event-ID: <cursor>  → text/event-stream
let subscription = try await gateway.subscribe(streamID: id, lastEventID: request.headers.first(name: "Last-Event-ID"))
for await frame in subscription.frames {
    try await writer.write(WireCodec.encode(frame))   // plus SSEEncoder.comment("keepalive") every ~15 s
}
// On StreamRejection, respond with rejection.httpStatus and an X-Stream-Rejection: <rawValue> header.
// Also call gateway.sweep() on a timer.
```

`ClaudeUpstream` would wrap a streaming Messages API call and yield `.text` for each text delta and `.finished(stopReason:)` for `message_stop`.

### Client side

`URLSessionStreamTransport` (Apple platforms) is the real transport: one `GET {base}/streams/{id}` per connection with `Last-Event-ID`, rejections mapped from the status code and `X-Stream-Rejection` header. Its request building and status mapping are unit-tested; there is no HTTP host in this repo, so it has not been run against a live server.

```swift
let transport = URLSessionStreamTransport(baseURL: URL(string: "https://gateway.example.com/v1")!)
let client = ResumableStreamClient(streamID: streamID, transport: transport,
                                   checkpoint: try? load())        // resume after a relaunch
for await event in await client.start() {
    switch event {
    case .delta(let text):        transcript += text
    case .finished(let terminal): show(terminal)
    case .failed(.rejected(let rejection)) where rejection.isPermanent:
        restartGeneration(with: UUID().uuidString)                  // new streamID; the only time the prompt is re-sent
    case .failed(let failure):    showError(failure)                // retries exhausted, protocol violation, …
    default: break
    }
}
// On scenePhase == .background:
try save(await client.checkpoint)                                   // TranscriptAssembler is Codable
```

`InMemoryTransport` is the reference implementation of `StreamTransport`: it speaks real SSE bytes to an in-process gateway and can inject drops, stalls, stale replays and missing frames. The tests and the demo app both use it.

## Run it

```bash
git clone https://github.com/rajatslakhina/resumable-stream-gateway-kit.git
cd resumable-stream-gateway-kit
swift test                       # macOS or Linux
bash scripts/mutation-check.sh   # ~1 build + test run per mutant; every mutant must be caught
```

## Verification

What was actually run before publishing, on the author's Mac:

- **macOS** (Xcode 27.2, Swift 6.4.2): clean `swift build --build-tests -Xswiftc -warnings-as-errors`, then the XCTest bundle: **61/61 passed** (59 cross-platform + 2 `URLSessionStreamTransport` tests that only build on Apple platforms).
- **Linux** (Docker `swift:6.0`, Swift 6.0.3): clean build with `-warnings-as-errors`, then `swift test`: **59/59 passed**.
- **Mutation check** (Linux, Docker `swift:6.0`): the unmutated baseline passes, then **12/12 mutants caught**.
- **Demo app on a Simulator:** see the [demo repo](https://github.com/rajatslakhina/resumable-stream-gateway-demo-app) for exactly what was run and what was not.
- **CI:** `.github/workflows/ci.yml` runs the Linux job (warnings as errors), a macOS job (tests plus a generic iOS Simulator build of both products) and the mutation check on every push to `main` and every `v*` tag. The current result is on the repository's Actions tab.

Not verified: `URLSessionStreamTransport` against a live server (there is no HTTP host in this repo), and a multi-instance deployment (see decision 9).

## Layout

```
Sources/StreamGateway/
  SSEParser.swift              WHATWG event-stream parser with line/event byte limits, and the encoder
  Cursor.swift                 StreamCursor, FrameKind, GatewayFrame, WireCodec (JSON-in-data)
  ReplayLog.swift              bounded ring buffer: frames + bytes, gap detection
  StreamGateway.swift          the actor: open/subscribe/cancel/sweep, backpressure, retention, admission
  Upstream.swift               StreamUpstream protocol, ScriptedUpstream
  TranscriptAssembler.swift    idempotent, order-checking, Codable client state
  ReconnectPolicy.swift        capped decorrelated jitter with the server retry: hint as a floor
  ResumableStreamClient.swift  the client actor and the StreamTransport protocol
  URLSessionStreamTransport.swift  the Apple-platform transport (GET + Last-Event-ID)
  InMemoryTransport.swift      reference transport with fault injection (tests, demo)
  Clocks.swift, Arithmetic.swift  injected time; saturating arithmetic
Sources/StreamGatewayUI/       SwiftUI fault lab used by the demo app (Apple platforms only)
Tests/StreamGatewayTests/      61 XCTest cases (59 on Linux)
scripts/mutation-check.sh      12 mutants; each must fail the suite
```

## License

MIT
