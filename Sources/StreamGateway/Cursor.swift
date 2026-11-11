import Foundation

/// The position of one frame in one stream generation.
///
/// It is serialised as the SSE `id:` (and so comes back as `Last-Event-ID`).
/// The `epoch` is random per stream *generation*, so a cursor from a different
/// stream, or from a stream that was evicted and re-opened under the same id,
/// is rejected instead of silently resuming at the wrong offset.
public struct StreamCursor: Sendable, Hashable, Codable, CustomStringConvertible {
    public let epoch: String
    public let sequence: UInt64

    public init(epoch: String, sequence: UInt64) {
        self.epoch = epoch
        self.sequence = sequence
    }

    public var description: String { "\(epoch):\(sequence)" }

    /// Parses `epoch:sequence`. Returns nil for anything malformed, including
    /// an empty epoch, a non-decimal or overflowing sequence, or sequence 0
    /// (sequences start at 1, so 0 can never name a delivered frame).
    public init?(_ text: String) {
        guard let colon = text.lastIndex(of: ":") else { return nil }
        let epochPart = text[text.startIndex..<colon]
        let seqPart = text[text.index(after: colon)...]
        guard !epochPart.isEmpty, !epochPart.contains(":"),
              !seqPart.isEmpty, seqPart.utf8.allSatisfy({ $0 >= 0x30 && $0 <= 0x39 }),
              let seq = UInt64(seqPart), seq > 0 else { return nil }
        self.epoch = String(epochPart)
        self.sequence = seq
    }

    static func makeEpoch<G: RandomNumberGenerator>(using generator: inout G) -> String {
        let value = UInt64.random(in: 1...UInt64.max, using: &generator)
        let hex = String(value, radix: 16)
        return String(repeating: "0", count: max(0, 16 - hex.count)) + hex
    }
}

/// What a frame means to the client.
public enum FrameKind: String, Sendable, Codable, Equatable {
    /// A slice of model output.
    case delta
    /// The model finished. Payload carries the stop reason.
    case done
    /// The stream ended abnormally. Payload carries a message.
    case error
}

/// One entry in a stream's replay log.
public struct GatewayFrame: Sendable, Equatable {
    public let cursor: StreamCursor
    public let kind: FrameKind
    /// For `.delta` the text; for `.done` the stop reason; for `.error` the message.
    public let payload: String

    public init(cursor: StreamCursor, kind: FrameKind, payload: String) {
        self.cursor = cursor
        self.kind = kind
        self.payload = payload
    }

    public var isTerminal: Bool { kind != .delta }
}

/// The wire format of a frame's `data:` line.
///
/// Payloads are JSON-encoded rather than written raw because SSE normalises
/// line endings: a raw `\r\n` inside model output would come back as `\n`,
/// and the reassembled transcript would differ from what the model produced.
/// JSON escapes every control character, so `data:` is always a single line.
public enum WireCodec {
    private struct Body: Codable { let v: String }

    public static func encode(_ frame: GatewayFrame) -> [UInt8] {
        let data = (try? JSONEncoder().encode(Body(v: frame.payload))) ?? Data(#"{"v":""}"#.utf8)
        return SSEEncoder.encode(id: frame.cursor.description,
                                 event: frame.kind.rawValue,
                                 data: String(decoding: data, as: UTF8.self))
    }

    /// Decodes a dispatched SSE event. Returns nil when the event is not a
    /// gateway frame (no parseable cursor, or an undecodable body).
    /// `kind` is nil for an event type this client does not know, which a
    /// client must still count for contiguity (forward compatibility).
    public static func decode(_ event: SSEEvent) -> (cursor: StreamCursor, kind: FrameKind?, payload: String)? {
        guard let cursor = StreamCursor(event.lastEventID) else { return nil }
        guard let body = try? JSONDecoder().decode(Body.self, from: Data(event.data.utf8)) else { return nil }
        return (cursor, FrameKind(rawValue: event.type), body.v)
    }
}
