// Incremental Server-Sent Events parser.
//
// Follows the WHATWG "event stream interpretation" algorithm, with two
// deliberate additions a production client needs and the spec leaves open:
// a hard per-line byte limit and a hard per-event byte limit, so a hostile or
// broken proxy cannot make the client buffer without bound.

/// One dispatched SSE event.
public struct SSEEvent: Sendable, Equatable {
    /// The `event:` field, or `"message"` when absent.
    public var type: String
    /// The concatenated `data:` lines, joined with `\n`, without the trailing newline.
    public var data: String
    /// The value of the parser's *last event ID buffer* at dispatch time.
    /// Per spec this persists across events until another `id:` line changes it.
    public var lastEventID: String

    public init(type: String = "message", data: String, lastEventID: String = "") {
        self.type = type
        self.data = data
        self.lastEventID = lastEventID
    }
}

/// What the parser can emit for a chunk of bytes.
public enum SSEOutput: Sendable, Equatable {
    case event(SSEEvent)
    /// A valid `retry:` field (milliseconds). The spec treats this as a
    /// reconnection-time hint, separate from event dispatch.
    case retry(milliseconds: Int)
    /// A comment line (`: keepalive`). Useful as a liveness signal.
    case comment(String)
}

public enum SSEParserError: Error, Equatable, Sendable {
    case lineTooLong(limit: Int)
    case eventTooLarge(limit: Int)
}

/// A value-type, chunk-boundary-agnostic SSE parser.
///
/// Bytes may be split anywhere, including between the CR and LF of a CRLF pair
/// and inside a multi-byte UTF-8 scalar or the leading BOM.
public struct SSEParser: Sendable {
    public let maxLineBytes: Int
    public let maxEventBytes: Int

    private var line: [UInt8] = []
    private var skipNextLF = false
    private var bomMatched = 0
    private var bomDone = false
    private var dataBuffer = ""
    private var dataBytes = 0
    private var eventType = ""
    private var idBuffer = ""

    private static let bom: [UInt8] = [0xEF, 0xBB, 0xBF]
    private static let cr: UInt8 = 0x0D
    private static let lf: UInt8 = 0x0A

    public init(maxLineBytes: Int = 64 * 1024, maxEventBytes: Int = 1024 * 1024) {
        // Clamp to at least 1 so a zero or negative limit cannot make every line "too long".
        self.maxLineBytes = max(1, maxLineBytes)
        self.maxEventBytes = max(1, maxEventBytes)
    }

    /// The parser's *last event ID buffer*. Note: per spec this changes as
    /// soon as an `id:` line is read, before its event is dispatched, so it is
    /// **not** what a client should send as `Last-Event-ID` after a drop mid-event.
    /// Use the `lastEventID` of the last *dispatched* `SSEEvent` instead (the
    /// bundled client uses `TranscriptAssembler.lastEventID`, which only moves
    /// when a frame is applied).
    public var lastEventIDBuffer: String { idBuffer }

    /// Feeds a chunk and returns everything that became complete.
    public mutating func feed<Bytes: Sequence>(_ bytes: Bytes) throws(SSEParserError) -> [SSEOutput]
    where Bytes.Element == UInt8 {
        var out: [SSEOutput] = []
        for byte in bytes {
            if !bomDone {
                if bomMatched < Self.bom.count, byte == Self.bom[bomMatched] {
                    bomMatched += 1
                    if bomMatched == Self.bom.count { bomDone = true }
                    continue
                }
                bomDone = true
                // A partial BOM prefix was real content; replay it.
                for index in 0..<bomMatched where index < Self.bom.count {
                    try consume(Self.bom[index], into: &out)
                }
            }
            try consume(byte, into: &out)
        }
        return out
    }

    /// Ends the stream. Per spec, an event that was not terminated by a blank
    /// line is discarded, so a half-received frame is never applied.
    public mutating func finish() {
        line.removeAll(keepingCapacity: false)
        dataBuffer = ""
        dataBytes = 0
        eventType = ""
        skipNextLF = false
    }

    private mutating func consume(_ byte: UInt8, into out: inout [SSEOutput]) throws(SSEParserError) {
        if skipNextLF {
            skipNextLF = false
            if byte == Self.lf { return }
        }
        if byte == Self.cr {
            try finishLine(into: &out)
            skipNextLF = true
        } else if byte == Self.lf {
            try finishLine(into: &out)
        } else {
            guard line.count < maxLineBytes else { throw .lineTooLong(limit: maxLineBytes) }
            line.append(byte)
        }
    }

    private mutating func finishLine(into out: inout [SSEOutput]) throws(SSEParserError) {
        // Lossy decoding replaces invalid UTF-8 with U+FFFD, as the spec requires.
        let text = String(decoding: line, as: UTF8.self)
        line.removeAll(keepingCapacity: true)

        if text.isEmpty {
            dispatch(into: &out)
            return
        }
        if text.hasPrefix(":") {
            out.append(.comment(String(text.dropFirst())))
            return
        }
        let field: Substring
        var value: Substring
        if let colon = text.firstIndex(of: ":") {
            field = text[text.startIndex..<colon]
            value = text[text.index(after: colon)...]
            if value.hasPrefix(" ") { value = value.dropFirst() }
        } else {
            field = Substring(text)
            value = ""
        }
        switch field {
        case "event":
            eventType = String(value)
        case "data":
            let added = value.utf8.count + 1
            // Written as two comparisons so neither side can overflow.
            guard added <= maxEventBytes, dataBytes <= maxEventBytes - added else {
                throw .eventTooLarge(limit: maxEventBytes)
            }
            dataBytes += added
            dataBuffer += value
            dataBuffer += "\n"
        case "id":
            if !value.contains("\u{0}") { idBuffer = String(value) }
        case "retry":
            if !value.isEmpty, value.utf8.allSatisfy({ $0 >= 0x30 && $0 <= 0x39 }), let ms = Int(value) {
                // Int(_:) returns nil on overflow, so an absurd value is ignored, not trapped on.
                out.append(.retry(milliseconds: ms))
            }
        default:
            break // Unknown fields are ignored per spec.
        }
    }

    private mutating func dispatch(into out: inout [SSEOutput]) {
        defer {
            dataBuffer = ""
            dataBytes = 0
            eventType = ""
        }
        guard !dataBuffer.isEmpty else { return }
        var data = dataBuffer
        if data.hasSuffix("\n") { data.removeLast() }
        out.append(.event(SSEEvent(type: eventType.isEmpty ? "message" : eventType,
                                   data: data,
                                   lastEventID: idBuffer)))
    }
}

/// Encodes events in the exact shape `SSEParser` (and browsers) decode.
public enum SSEEncoder {
    /// Encodes one event. Multi-line data becomes several `data:` lines, and
    /// CR, LF and NUL are stripped from `id` and `event`, because any of them
    /// would either split the field or (for NUL in `id`) make the client ignore it.
    public static func encode(id: String?, event: String?, data: String) -> [UInt8] {
        var out = ""
        if let id { out += "id: " + sanitize(id) + "\n" }
        if let event { out += "event: " + sanitize(event) + "\n" }
        for line in splitLines(data) {
            out += "data: " + line + "\n"
        }
        out += "\n"
        return Array(out.utf8)
    }

    public static func retry(milliseconds: Int) -> [UInt8] {
        Array("retry: \(max(0, milliseconds))\n\n".utf8)
    }

    public static func comment(_ text: String) -> [UInt8] {
        Array((": " + sanitize(text) + "\n").utf8)
    }

    static func sanitize(_ value: String) -> String {
        String(value.unicodeScalars.filter { $0 != "\r" && $0 != "\n" && $0 != "\u{0}" }.map(Character.init))
    }

    /// Splits on CRLF, CR or LF, the same line terminators the parser accepts.
    static func splitLines(_ text: String) -> [String] {
        var lines: [String] = []
        var current = String.UnicodeScalarView()
        var previousWasCR = false
        for scalar in text.unicodeScalars {
            if scalar == "\n" {
                if previousWasCR { previousWasCR = false; continue }
                lines.append(String(current)); current = .init()
            } else if scalar == "\r" {
                lines.append(String(current)); current = .init()
                previousWasCR = true
            } else {
                previousWasCR = false
                current.append(scalar)
            }
        }
        lines.append(String(current))
        return lines
    }
}
