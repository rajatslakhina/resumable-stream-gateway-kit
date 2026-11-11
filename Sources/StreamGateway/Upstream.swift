/// What the model side of the gateway produces.
public enum UpstreamEvent: Sendable, Equatable {
    case text(String)
    case finished(stopReason: String)
}

/// The model call behind a stream: in production a streaming Messages API
/// request; here anything that yields `UpstreamEvent`s.
///
/// A gateway calls `events()` at most once per stream generation. That is the
/// single-flight guarantee: reconnects and duplicate opens never re-bill a prompt.
public protocol StreamUpstream: Sendable {
    func events() -> AsyncThrowingStream<UpstreamEvent, any Error>
}

/// A deterministic upstream for demos and tests: emits `tokens` one at a time,
/// `interval` apart, then `.finished`. Optionally fails after `failAfter` tokens.
public struct ScriptedUpstream: StreamUpstream {
    public struct Failure: Error, Equatable, Sendable { public let message: String }

    public let tokens: [String]
    public let interval: Duration
    public let stopReason: String
    public let failAfter: Int?
    public let sleeper: any Sleeper

    public init(tokens: [String], interval: Duration = .zero, stopReason: String = "end_turn",
                failAfter: Int? = nil, sleeper: any Sleeper = TaskSleeper()) {
        self.tokens = tokens
        self.interval = interval
        self.stopReason = stopReason
        self.failAfter = failAfter
        self.sleeper = sleeper
    }

    /// Splits text into word-ish tokens (each keeps its trailing whitespace),
    /// so concatenating the tokens reproduces `text` exactly.
    public static func tokenize(_ text: String) -> [String] {
        var tokens: [String] = []
        var current = ""
        var inSpace = false
        for character in text {
            let isSpace = character.isWhitespace
            if inSpace && !isSpace { tokens.append(current); current = "" }
            current.append(character)
            inSpace = isSpace
        }
        if !current.isEmpty { tokens.append(current) }
        return tokens
    }

    public func events() -> AsyncThrowingStream<UpstreamEvent, any Error> {
        let tokens = tokens, interval = interval, stopReason = stopReason
        let failAfter = failAfter, sleeper = sleeper
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for (index, token) in tokens.enumerated() {
                        if let failAfter, index >= failAfter {
                            throw Failure(message: "upstream failed after \(failAfter) tokens")
                        }
                        try await sleeper.sleep(for: interval)
                        continuation.yield(.text(token))
                    }
                    continuation.yield(.finished(stopReason: stopReason))
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}
