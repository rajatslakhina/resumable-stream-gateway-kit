/// The client's source of truth: an idempotent, order-checking fold over frames.
///
/// It is `Codable` on purpose. Persist it when the app backgrounds and a
/// relaunched process can resume the same generation (within the gateway's
/// retention window) instead of re-sending the prompt.
public struct TranscriptAssembler: Sendable, Equatable, Codable {
    public enum Terminal: Sendable, Equatable, Codable {
        case done(stopReason: String)
        case failed(message: String)
    }

    public enum ApplyResult: Sendable, Equatable {
        case applied
        /// Already applied (at-least-once delivery). Ignored, and counted.
        case duplicate
        /// The frame skips sequences. Applying it would leave a hole in the text.
        case gap(expected: UInt64, received: UInt64)
        /// The frame belongs to a different stream generation.
        case epochMismatch
        /// A frame after the terminal one. Ignored.
        case afterTerminal
    }

    public private(set) var epoch: String?
    public private(set) var lastSequence: UInt64 = 0
    public private(set) var text = ""
    public private(set) var terminal: Terminal?
    public private(set) var duplicatesDropped = 0
    public private(set) var unknownFramesSkipped = 0

    public init() {}

    /// The value to send as `Last-Event-ID`, or nil before the first frame.
    public var lastEventID: String? {
        guard let epoch, lastSequence > 0 else { return nil }
        return StreamCursor(epoch: epoch, sequence: lastSequence).description
    }

    public var isFinished: Bool { terminal != nil }

    /// Applies one frame. `kind == nil` is a frame type this client does not
    /// understand: it still advances the cursor (so contiguity holds) but does
    /// not change the text.
    @discardableResult
    public mutating func apply(cursor: StreamCursor, kind: FrameKind?, payload: String) -> ApplyResult {
        if let epoch, epoch != cursor.epoch { return .epochMismatch }
        if cursor.sequence <= lastSequence {
            duplicatesDropped = Saturating.add(duplicatesDropped, 1)
            return .duplicate
        }
        if terminal != nil { return .afterTerminal }
        // lastSequence < cursor.sequence <= UInt64.max, so lastSequence + 1 cannot overflow.
        let expected = lastSequence + 1
        guard cursor.sequence == expected else { return .gap(expected: expected, received: cursor.sequence) }
        epoch = cursor.epoch
        lastSequence = cursor.sequence
        switch kind {
        case .delta?: text += payload
        case .done?: terminal = .done(stopReason: payload)
        case .error?: terminal = .failed(message: payload)
        case nil: unknownFramesSkipped = Saturating.add(unknownFramesSkipped, 1)
        }
        return .applied
    }
}
