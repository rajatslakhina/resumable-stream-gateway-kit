/// The wire seam between the client and a gateway: one SSE connection.
///
/// A URLSession implementation sends `GET /streams/{id}` with
/// `Last-Event-ID: <lastEventID>` and yields the response body's bytes. Map
/// HTTP 4xx/5xx rejections to `StreamRejection` (see `httpStatus`) so the
/// client can tell "retry" from "restart".
///
/// Idle detection belongs here too: `URLSessionConfiguration.timeoutIntervalForRequest`
/// is an *idle* timeout between packets, which is exactly the watchdog an SSE
/// client needs when the gateway sends `: keepalive` comments.
public protocol StreamTransport: Sendable {
    func connect(streamID: String, lastEventID: String?) async throws -> AsyncThrowingStream<[UInt8], any Error>
}

public enum ClientFailure: Error, Sendable, Equatable {
    /// The gateway said resuming can never work (stream gone, replay gap, …).
    case rejected(StreamRejection)
    /// The server delivered frames out of order or from another generation.
    /// This is a server bug; the client refuses to build a corrupt transcript.
    case protocolViolation(String)
    /// `attempts` connection attempts in a row (= `maxConsecutiveFailures`) made no progress.
    case retriesExhausted(attempts: Int)
    /// The SSE stream broke a parser limit.
    case malformedStream(String)
    /// `start()` was called twice.
    case alreadyStarted
}

public enum ClientEvent: Sendable, Equatable {
    case connected(attempt: Int, resumingFrom: String?)
    case delta(String)
    /// `attempt` is the number the next `.connected` will carry.
    case reconnecting(attempt: Int, after: Duration)
    case finished(TranscriptAssembler.Terminal)
    case failed(ClientFailure)
}

/// The client half: reconnects with `Last-Event-ID`, de-duplicates, refuses
/// gaps, and exposes a persistable checkpoint.
///
/// Only `run` mutates `assembler`. Other actor methods may interleave at its
/// `await`s, but they only read, so reentrancy cannot corrupt the transcript.
public actor ResumableStreamClient {
    public let streamID: String
    private let transport: any StreamTransport
    private let policy: ReconnectPolicy
    private let sleeper: any Sleeper
    private let random: @Sendable () -> Double
    private var assembler: TranscriptAssembler
    private var started = false
    private var task: Task<Void, Never>?
    private(set) public var connectionAttempts = 0

    /// - Parameter checkpoint: a previously persisted assembler to resume from.
    public init(streamID: String,
                transport: any StreamTransport,
                policy: ReconnectPolicy = .init(),
                checkpoint: TranscriptAssembler? = nil,
                sleeper: any Sleeper = TaskSleeper(),
                random: @escaping @Sendable () -> Double = { Double.random(in: 0..<1) }) {
        self.streamID = streamID
        self.transport = transport
        self.policy = policy
        self.assembler = checkpoint ?? TranscriptAssembler()
        self.sleeper = sleeper
        self.random = random
    }

    /// A snapshot to persist (for example on `scenePhase == .background`).
    public var checkpoint: TranscriptAssembler { assembler }

    /// Starts the connect/resume loop. The stream ends after `.finished` or `.failed`.
    public func start() -> AsyncStream<ClientEvent> {
        let (stream, continuation) = AsyncStream.makeStream(of: ClientEvent.self, bufferingPolicy: .unbounded)
        guard !started else {
            continuation.yield(.failed(.alreadyStarted))
            continuation.finish()
            return stream
        }
        started = true
        if let terminal = assembler.terminal {
            continuation.yield(.finished(terminal))
            continuation.finish()
            return stream
        }
        task = Task { await self.run(continuation) }
        continuation.onTermination = { [weak self] _ in
            Task { await self?.stop() }
        }
        return stream
    }

    /// Stops the loop (for example when the app suspends). The checkpoint is kept.
    public func stop() {
        task?.cancel()
        task = nil
    }

    private func run(_ out: AsyncStream<ClientEvent>.Continuation) async {
        defer { out.finish() }
        var failures = 0
        var previousDelay: Duration?
        var serverHint: Duration?
        while !Task.isCancelled {
            connectionAttempts = Saturating.add(connectionAttempts, 1)
            let resumeFrom = assembler.lastEventID
            var progressed = false
            do {
                let bytes = try await transport.connect(streamID: streamID, lastEventID: resumeFrom)
                out.yield(.connected(attempt: connectionAttempts, resumingFrom: resumeFrom))
                var parser = SSEParser()
                for try await chunk in bytes {
                    let outputs: [SSEOutput]
                    do { outputs = try parser.feed(chunk) } catch {
                        out.yield(.failed(.malformedStream(String(describing: error))))
                        return
                    }
                    for output in outputs {
                        switch output {
                        case .retry(let ms):
                            serverHint = .milliseconds(ms)
                        case .comment:
                            continue
                        case .event(let event):
                            guard let frame = WireCodec.decode(event) else { continue }
                            switch assembler.apply(cursor: frame.cursor, kind: frame.kind, payload: frame.payload) {
                            case .applied:
                                progressed = true
                                if frame.kind == .delta { out.yield(.delta(frame.payload)) }
                                if let terminal = assembler.terminal {
                                    out.yield(.finished(terminal))
                                    return
                                }
                            case .duplicate, .afterTerminal:
                                continue
                            case .gap(let expected, let received):
                                out.yield(.failed(.protocolViolation("expected frame \(expected), received \(received)")))
                                return
                            case .epochMismatch:
                                out.yield(.failed(.protocolViolation("frame from another stream generation")))
                                return
                            }
                        }
                    }
                }
                parser.finish() // A half-received frame is discarded, never applied.
            } catch let rejection as StreamRejection {
                if rejection.isPermanent {
                    out.yield(.failed(.rejected(rejection)))
                    return
                }
            } catch {
                if Task.isCancelled { return }
                // Transient: fall through to backoff.
            }
            if Task.isCancelled { return }
            if progressed {
                // An attempt that delivered a frame is not a failure: reset
                // the budget and the backoff.
                failures = 0
                previousDelay = nil
            } else {
                failures += 1
                // After the Nth consecutive attempt without progress, give up
                // instead of sleeping before an (N+1)th.
                guard failures < policy.maxConsecutiveFailures else {
                    out.yield(.failed(.retriesExhausted(attempts: failures)))
                    return
                }
            }
            let delay = policy.nextDelay(previous: previousDelay, serverHint: serverHint, unit: random())
            previousDelay = delay
            out.yield(.reconnecting(attempt: Saturating.add(connectionAttempts, 1), after: delay))
            do { try await sleeper.sleep(for: delay) } catch { return }
        }
    }
}
