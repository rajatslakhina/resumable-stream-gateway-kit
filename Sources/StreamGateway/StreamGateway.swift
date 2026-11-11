/// Tunables for a `StreamGateway`. Every value is clamped to a safe range in
/// `init`, and the properties are immutable afterwards, so no configuration
/// can trap or switch off a bound.
public struct GatewayConfiguration: Sendable, Equatable {
    /// Frames each stream keeps for replay. Older frames are evicted first.
    public let replayFrameCapacity: Int
    /// Approximate bytes each stream keeps for replay.
    public let replayByteCapacity: Int
    /// Largest `.delta` payload in one frame. Larger upstream chunks are split,
    /// on `Character` boundaries where possible and on Unicode-scalar
    /// boundaries inside a single oversized grapheme, so no frame exceeds it.
    public let maxFramePayloadBytes: Int
    /// Live frames a connection may fall behind by before the gateway cuts it.
    /// This is the backpressure valve (see README, decision 3). A resume's
    /// replay backlog is handed over separately and doesn't count against it.
    public let subscriberQueueLimit: Int
    /// Connections per stream whose subscriptions are still alive (including
    /// cut ones the host hasn't dropped, and replays of finished streams).
    /// More are refused with `.tooManySubscribers`.
    public let maxSubscribersPerStream: Int
    /// How long a finished stream stays resumable.
    public let retention: Duration
    /// How long a live stream may have no subscriber before its upstream is cancelled.
    public let abandonTimeout: Duration
    /// Live + retained streams. Admission fails with `.overCapacity` beyond it.
    public let maxStreams: Int

    public init(replayFrameCapacity: Int = 4096,
                replayByteCapacity: Int = 1 << 20,
                maxFramePayloadBytes: Int = 4096,
                subscriberQueueLimit: Int = 256,
                maxSubscribersPerStream: Int = 8,
                retention: Duration = .seconds(300),
                abandonTimeout: Duration = .seconds(120),
                maxStreams: Int = 1000) {
        self.replayFrameCapacity = min(max(1, replayFrameCapacity), 1 << 20)
        self.replayByteCapacity = min(max(1, replayByteCapacity), 1 << 30)
        self.maxFramePayloadBytes = min(max(16, maxFramePayloadBytes), 1 << 20)
        self.subscriberQueueLimit = min(max(1, subscriberQueueLimit), 1 << 16)
        self.maxSubscribersPerStream = min(max(1, maxSubscribersPerStream), 1024)
        self.retention = max(.zero, retention)
        self.abandonTimeout = max(.zero, abandonTimeout)
        self.maxStreams = min(max(1, maxStreams), 1 << 20)
    }

    /// A copy with some values replaced (re-clamped through `init`).
    public func with(replayFrameCapacity: Int? = nil, subscriberQueueLimit: Int? = nil,
                     maxSubscribersPerStream: Int? = nil, retention: Duration? = nil,
                     abandonTimeout: Duration? = nil, maxStreams: Int? = nil) -> GatewayConfiguration {
        GatewayConfiguration(replayFrameCapacity: replayFrameCapacity ?? self.replayFrameCapacity,
                             replayByteCapacity: replayByteCapacity,
                             maxFramePayloadBytes: maxFramePayloadBytes,
                             subscriberQueueLimit: subscriberQueueLimit ?? self.subscriberQueueLimit,
                             maxSubscribersPerStream: maxSubscribersPerStream ?? self.maxSubscribersPerStream,
                             retention: retention ?? self.retention,
                             abandonTimeout: abandonTimeout ?? self.abandonTimeout,
                             maxStreams: maxStreams ?? self.maxStreams)
    }

    /// Largest accounted size of one frame: a delta is at most
    /// `maxFramePayloadBytes`; a terminal payload is at most
    /// `StreamGateway.terminalPayloadLimit` bytes.
    var maxFrameCost: Int {
        Saturating.add(max(maxFramePayloadBytes, StreamGateway.terminalPayloadLimit), ReplayLog.frameOverhead)
    }

    /// Upper bound on frame payload bytes the gateway can keep alive, in the
    /// same accounting the replay log uses (UTF-8 bytes plus a fixed per-frame
    /// overhead; allocator and bookkeeping overhead are not modelled).
    ///
    /// Per stream: the log (`replayByteCapacity` plus its newest frame), plus,
    /// for each of up to `maxSubscribersPerStream` connections, frames the log
    /// has since evicted but the connection still buffers: at most one replay
    /// backlog (a past copy of the log) and `subscriberQueueLimit` live frames.
    public var worstCaseBytes: Int {
        let log = Saturating.add(replayByteCapacity, maxFrameCost)
        let perConnection = Saturating.add(log, Saturating.multiply(subscriberQueueLimit, maxFrameCost))
        let perStream = Saturating.add(log, Saturating.multiply(maxSubscribersPerStream, perConnection))
        return Saturating.multiply(perStream, maxStreams)
    }
}

/// Why a gateway refused an open or a subscribe. Each maps to an HTTP status
/// in a real host (`httpStatus`).
public enum StreamRejection: String, Error, Sendable, Codable, CaseIterable {
    /// No such stream (never opened, evicted after retention, or gateway restarted).
    case unknownStream
    /// `Last-Event-ID` is not a gateway cursor.
    case malformedCursor
    /// The cursor belongs to a different stream generation.
    case cursorMismatch
    /// The cursor names a frame the stream never produced.
    case cursorAhead
    /// The frames after the cursor were evicted; resuming would leave a hole.
    case replayGap
    /// Admission control: too many streams.
    case overCapacity
    /// Admission control: this stream already has `maxSubscribersPerStream` connections.
    case tooManySubscribers

    public var httpStatus: Int {
        switch self {
        case .unknownStream, .replayGap: return 410 // Gone: the client must restart the generation.
        case .malformedCursor, .cursorAhead: return 400
        case .cursorMismatch: return 409
        case .overCapacity: return 503
        case .tooManySubscribers: return 429
        }
    }

    /// Whether retrying the same request can ever succeed. A permanent
    /// rejection of a resume means this generation is unusable: restart with
    /// a **new** `streamID` (see README, "Restarting after a 410").
    public var isPermanent: Bool { self != .overCapacity && self != .tooManySubscribers }
}

public enum OpenOutcome: Sendable, Equatable {
    /// A new generation started; exactly one upstream request was made.
    case started(epoch: String)
    /// The stream already existed (live or retained); no upstream request.
    case joined(epoch: String)

    public var epoch: String {
        switch self { case .started(let e), .joined(let e): return e }
    }
}

/// One connection's view of a stream. `frames` ends when the stream
/// finishes, when the consumer falls behind (the client then resumes), or
/// when the consumer cancels.
public struct GatewaySubscription: Sendable {
    public let id: UInt64
    /// The replay backlog, then live frames.
    public let frames: Frames

    /// An `AsyncSequence` that yields the replay backlog from an array, then
    /// forwards the live stream. Keeping them separate is what makes the live
    /// limit exact: the live buffer holds at most `subscriberQueueLimit`
    /// frames, however large the backlog was.
    public struct Frames: AsyncSequence, Sendable {
        public typealias Element = GatewayFrame
        let backlog: [GatewayFrame]
        let live: AsyncStream<GatewayFrame>
        let lease: SubscriptionLease

        public func makeAsyncIterator() -> Iterator {
            Iterator(backlog: backlog, live: live.makeAsyncIterator(), lease: lease)
        }

        public struct Iterator: AsyncIteratorProtocol {
            let backlog: [GatewayFrame]
            var index = 0
            var live: AsyncStream<GatewayFrame>.Iterator
            let lease: SubscriptionLease

            public mutating func next() async -> GatewayFrame? {
                if backlog.indices.contains(index) {
                    defer { index += 1 }
                    return backlog[index]
                }
                return await live.next()
            }
        }
    }
}

/// Counts a connection against `maxSubscribersPerStream` for as long as any
/// copy of its subscription (or an iterator over it) is alive, including
/// after the gateway has cut it, because until then it still pins its frames.
final class SubscriptionLease: Sendable {
    private let onRelease: @Sendable () -> Void
    init(onRelease: @escaping @Sendable () -> Void) { self.onRelease = onRelease }
    deinit { onRelease() }
}

public struct GatewayStats: Sendable, Equatable {
    public internal(set) var upstreamRequests = 0
    public internal(set) var framesAppended = 0
    public internal(set) var framesReplayed = 0
    public internal(set) var framesDeliveredLive = 0
    public internal(set) var slowConsumerCuts = 0
    public internal(set) var rejections = 0
    public internal(set) var streamsEvicted = 0
    public internal(set) var streamsAbandoned = 0
    public internal(set) var liveStreams = 0
    public internal(set) var retainedStreams = 0
    public internal(set) var activeSubscribers = 0
    public internal(set) var bytesHeld = 0
    public init() {}
}

public struct StreamSnapshot: Sendable, Equatable {
    public enum Phase: Sendable, Equatable { case live, finished }
    public let epoch: String
    public let phase: Phase
    public let oldestSequence: UInt64?
    public let newestSequence: UInt64
    public let framesHeld: Int
    public let bytesHeld: Int
    public let subscribers: Int
}

/// The server half: one upstream model call per stream, a bounded replay log
/// per stream, and any number of resumable subscribers.
///
/// Concurrency contract. All state is actor-isolated. The two operations that
/// must be atomic, *replay-then-register* in `subscribe` and
/// *append-then-fan-out* in `ingest`, contain no `await`, so a frame is either
/// in a new subscriber's replay batch or delivered to it live, never both and
/// never neither. Upstream tasks re-check the stream's epoch and phase after
/// every `await` before touching state, because the stream may have been
/// abandoned, cancelled or evicted and re-opened meanwhile.
public actor StreamGateway {
    private final class StreamState {
        var log: ReplayLog
        var finishedAt: Duration?
        var subscribers: [UInt64: AsyncStream<GatewayFrame>.Continuation] = [:]
        var unattendedSince: Duration?
        var task: Task<Void, Never>?
        /// Connections whose subscription is still alive (see `SubscriptionLease`).
        var leases = 0

        init(log: ReplayLog, now: Duration) {
            self.log = log
            self.unattendedSince = now
        }
    }

    public let configuration: GatewayConfiguration
    private let clock: any MonotonicClock
    private let epochFactory: @Sendable () -> String
    private var streams: [String: StreamState] = [:]
    private var nextSubscriberID: UInt64 = 1
    private var counters = GatewayStats()

    public init(configuration: GatewayConfiguration = .init(),
                clock: any MonotonicClock = SystemClock(),
                epochFactory: (@Sendable () -> String)? = nil) {
        self.configuration = configuration
        self.clock = clock
        self.epochFactory = epochFactory ?? {
            var generator = SystemRandomNumberGenerator()
            return StreamCursor.makeEpoch(using: &generator)
        }
    }

    // MARK: Open

    /// Opens a stream, or joins it if `streamID` is already known.
    ///
    /// `streamID` is the client's idempotency key: a retried POST after a lost
    /// response joins the same generation instead of paying for the prompt twice.
    @discardableResult
    public func open(streamID: String, upstream: some StreamUpstream) throws(StreamRejection) -> OpenOutcome {
        sweep()
        if let existing = streams[streamID] { return .joined(epoch: existing.log.epoch) }
        if streams.count >= configuration.maxStreams { evictFinishedStreams(toFit: 1) }
        guard streams.count < configuration.maxStreams else {
            counters.rejections += 1
            throw .overCapacity
        }
        let epoch = Self.validEpoch(epochFactory())
        let state = StreamState(log: ReplayLog(epoch: epoch,
                                               frameCapacity: configuration.replayFrameCapacity,
                                               byteCapacity: configuration.replayByteCapacity),
                                now: clock.now())
        streams[streamID] = state
        counters.upstreamRequests += 1
        let events = upstream.events()
        state.task = Task { [weak self] in
            do {
                for try await event in events {
                    guard let self, await self.ingest(streamID: streamID, epoch: epoch, event: event) else { return }
                }
                await self?.upstreamEnded(streamID: streamID, epoch: epoch, failure: nil)
            } catch {
                await self?.upstreamEnded(streamID: streamID, epoch: epoch, failure: error)
            }
        }
        return .started(epoch: epoch)
    }

    // MARK: Subscribe

    /// Attaches a connection. `lastEventID` is the SSE `Last-Event-ID` header:
    /// nil or empty means "from the first frame".
    public func subscribe(streamID: String, lastEventID: String?) throws(StreamRejection) -> GatewaySubscription {
        sweep()
        guard let state = streams[streamID] else { throw reject(.unknownStream) }
        var after: UInt64?
        if let lastEventID, !lastEventID.isEmpty {
            guard let cursor = StreamCursor(lastEventID) else { throw reject(.malformedCursor) }
            guard cursor.epoch == state.log.epoch else { throw reject(.cursorMismatch) }
            after = cursor.sequence
        }
        let backlog: [GatewayFrame]
        switch state.log.replay(after: after) {
        case .cursorAhead: throw reject(.cursorAhead)
        case .gap: throw reject(.replayGap)
        case .frames(let frames): backlog = frames
        }
        // Admission after the replay check, so a resume that can never work
        // gets its permanent 410 now rather than a transient 429.
        guard state.leases < configuration.maxSubscribersPerStream else { throw reject(.tooManySubscribers) }

        let id = nextSubscriberID
        nextSubscriberID &+= 1 // Wrapping is harmless: 2^64 subscriptions will not coexist.
        let epoch = state.log.epoch
        // Live frames only: the backlog is handed over as an array (it shares
        // string storage with the log), so this buffer bounds live lag exactly.
        let (live, continuation) = AsyncStream.makeStream(
            of: GatewayFrame.self,
            bufferingPolicy: .bufferingOldest(configuration.subscriberQueueLimit))
        continuation.onTermination = { [weak self] _ in
            Task { await self?.detach(streamID: streamID, epoch: epoch, subscriberID: id) }
        }
        state.leases += 1
        let lease = SubscriptionLease { [weak self] in
            Task { await self?.releaseLease(streamID: streamID, epoch: epoch) }
        }
        let subscription = GatewaySubscription(id: id, frames: .init(backlog: backlog, live: live, lease: lease))
        counters.framesReplayed = Saturating.add(counters.framesReplayed, backlog.count)

        // No await between the replay snapshot above and registration below.
        if state.finishedAt != nil {
            continuation.finish() // The backlog already ends with the terminal frame.
            return subscription
        }
        state.subscribers[id] = continuation
        state.unattendedSince = nil
        return subscription
    }

    private func releaseLease(streamID: String, epoch: String) {
        guard let state = streams[streamID], state.log.epoch == epoch else { return }
        state.leases = max(0, state.leases - 1)
    }

    // MARK: Control

    /// Client-requested stop ("Stop generating"). Cancels the upstream call and
    /// ends the stream with an `.error("cancelled")` frame that late resumers also see.
    /// Returns false when there was nothing to cancel (unknown or already finished).
    @discardableResult
    public func cancel(streamID: String) -> Bool {
        guard let state = streams[streamID], state.finishedAt == nil else { return false }
        state.task?.cancel()
        finish(state, kind: .error, payload: "cancelled")
        return true
    }

    /// Housekeeping: drops finished streams past `retention` and cancels live
    /// streams that have had no subscriber for `abandonTimeout`. Called on
    /// every open/subscribe; a host should also call it on a timer.
    public func sweep() {
        let now = clock.now()
        for (streamID, state) in streams {
            if let finishedAt = state.finishedAt {
                if now - finishedAt >= configuration.retention {
                    streams[streamID] = nil
                    counters.streamsEvicted += 1
                }
            } else if state.subscribers.isEmpty, let since = state.unattendedSince,
                      now - since >= configuration.abandonTimeout {
                state.task?.cancel()
                counters.streamsAbandoned += 1
                finish(state, kind: .error, payload: "abandoned: no subscriber")
            }
        }
    }

    public func stats() -> GatewayStats {
        var result = counters
        result.liveStreams = streams.values.filter { $0.finishedAt == nil }.count
        result.retainedStreams = streams.count - result.liveStreams
        result.activeSubscribers = streams.values.reduce(0) { $0 + $1.subscribers.count }
        result.bytesHeld = streams.values.reduce(0) { Saturating.add($0, $1.log.byteCount) }
        return result
    }

    public func snapshot(streamID: String) -> StreamSnapshot? {
        guard let state = streams[streamID] else { return nil }
        return StreamSnapshot(epoch: state.log.epoch,
                              phase: state.finishedAt == nil ? .live : .finished,
                              oldestSequence: state.log.oldestSequence,
                              newestSequence: state.log.nextSequence - 1,
                              framesHeld: state.log.count,
                              bytesHeld: state.log.byteCount,
                              subscribers: state.subscribers.count)
    }

    // MARK: Upstream side

    /// Returns false when the upstream should stop (stream gone or finished).
    private func ingest(streamID: String, epoch: String, event: UpstreamEvent) -> Bool {
        guard let state = live(streamID, epoch) else { return false }
        switch event {
        case .text(let text):
            for piece in Self.split(text, maxBytes: configuration.maxFramePayloadBytes) {
                append(piece, kind: .delta, to: state)
                if state.finishedAt != nil { return false } // sequence space exhausted
            }
            return true
        case .finished(let stopReason):
            finish(state, kind: .done, payload: Self.truncate(stopReason))
            return false
        }
    }

    private func upstreamEnded(streamID: String, epoch: String, failure: (any Error)?) {
        guard let state = live(streamID, epoch) else { return }
        // A connection that closes cleanly without a stop reason is a
        // truncation, not a completion. Never report it as `.done`.
        let message = failure.map { Self.truncate("upstream failed: " + String(describing: $0)) }
            ?? "upstream closed without a stop reason"
        finish(state, kind: .error, payload: message)
    }

    /// The epoch check is defensive: it stops an upstream task that was
    /// mid-`await` when its stream was cancelled, evicted and re-opened under
    /// the same ID from writing into the new generation.
    private func live(_ streamID: String, _ epoch: String) -> StreamState? {
        guard let state = streams[streamID], state.log.epoch == epoch, state.finishedAt == nil else { return nil }
        return state
    }

    private func append(_ payload: String, kind: FrameKind, to state: StreamState) {
        guard let frame = state.log.append(kind: kind, payload: payload) else {
            // 2^64 - 1 frames. Unreachable in practice; handled so it cannot trap.
            terminate(state)
            return
        }
        counters.framesAppended += 1
        for (id, continuation) in state.subscribers {
            switch continuation.yield(frame) {
            case .enqueued:
                counters.framesDeliveredLive += 1
            case .dropped:
                // This consumer is a full queue behind. Cut it rather than
                // buffer per connection; it resumes from the replay log.
                counters.slowConsumerCuts += 1
                state.subscribers[id] = nil
                continuation.finish()
            case .terminated:
                state.subscribers[id] = nil
            @unknown default:
                state.subscribers[id] = nil
                continuation.finish()
            }
        }
        if state.subscribers.isEmpty, state.unattendedSince == nil {
            state.unattendedSince = clock.now()
        }
    }

    private func finish(_ state: StreamState, kind: FrameKind, payload: String) {
        guard state.finishedAt == nil else { return }
        append(payload, kind: kind, to: state)
        terminate(state)
    }

    private func terminate(_ state: StreamState) {
        state.finishedAt = clock.now()
        state.task = nil
        let subscribers = state.subscribers
        state.subscribers = [:]
        for continuation in subscribers.values { continuation.finish() }
    }

    private func detach(streamID: String, epoch: String, subscriberID: UInt64) {
        guard let state = streams[streamID], state.log.epoch == epoch,
              state.subscribers.removeValue(forKey: subscriberID) != nil else { return }
        if state.subscribers.isEmpty, state.finishedAt == nil { state.unattendedSince = clock.now() }
    }

    private func reject(_ rejection: StreamRejection) -> StreamRejection {
        counters.rejections += 1
        return rejection
    }

    private func evictFinishedStreams(toFit needed: Int) {
        let finished = streams.compactMap { key, value in value.finishedAt.map { (key, $0) } }
            .sorted { $0.1 < $1.1 }
        var index = 0
        // maxStreams >= 1 (clamped, immutable) and needed is 1, so this cannot overflow.
        while streams.count > configuration.maxStreams - min(needed, configuration.maxStreams), index < finished.count {
            streams[finished[index].0] = nil
            counters.streamsEvicted += 1
            index += 1
        }
    }

    // MARK: Helpers

    /// An injected epoch must be non-empty, contain no `:` (the cursor
    /// separator) and only visible ASCII, or cursors built from it would not
    /// parse and the client would skip every frame. Anything else is replaced
    /// by a random epoch. Uniqueness per generation is the factory's job; a
    /// constant epoch (fine in tests) disables `cursorMismatch` protection.
    static func validEpoch(_ candidate: String) -> String {
        let ok = !candidate.isEmpty && candidate.utf8.count <= 64
            && candidate.utf8.allSatisfy { $0 > 0x20 && $0 < 0x7F && $0 != UInt8(ascii: ":") }
        if ok { return candidate }
        var generator = SystemRandomNumberGenerator()
        return StreamCursor.makeEpoch(using: &generator)
    }

    /// Bytes allowed in a terminal (`.done` / `.error`) payload.
    static let terminalPayloadLimit = 256

    /// Splits `text` into pieces of at most `maxBytes` UTF-8 bytes (`maxBytes`
    /// is at least 16, so any scalar fits). Splits fall on `Character`
    /// boundaries; a single grapheme larger than `maxBytes` (for example a
    /// letter followed by thousands of combining marks) is split on
    /// Unicode-scalar boundaries. Concatenation always reproduces `text`
    /// exactly, which is what the client does, so the transcript is unchanged.
    static func split(_ text: String, maxBytes: Int) -> [String] {
        let limit = max(4, maxBytes)
        guard text.utf8.count > limit else { return [text] }
        var pieces: [String] = []
        var current = String.UnicodeScalarView()
        var currentBytes = 0
        func flush() {
            if !current.isEmpty { pieces.append(String(current)) }
            current = .init()
            currentBytes = 0
        }
        for character in text {
            let size = character.utf8.count
            if size > limit {
                flush()
                for scalar in character.unicodeScalars {
                    let scalarSize = scalar.utf8.count
                    if currentBytes > limit - scalarSize { flush() }
                    current.append(scalar)
                    currentBytes += scalarSize
                }
                flush()
                continue
            }
            if currentBytes > limit - size { flush() }
            current.append(contentsOf: character.unicodeScalars)
            currentBytes += size
        }
        flush()
        return pieces
    }

    /// Truncates to at most `limit` UTF-8 bytes on a `Character` boundary,
    /// appending "…" (3 bytes, counted) when something was cut.
    static func truncate(_ text: String, limit: Int = terminalPayloadLimit) -> String {
        guard text.utf8.count > limit else { return text }
        let budget = max(0, limit - 3)
        var result = ""
        var used = 0
        for character in text {
            let size = character.utf8.count
            if used > budget - min(size, budget) || size > budget { break }
            result.append(character)
            used += size
        }
        return result + "…"
    }
}
