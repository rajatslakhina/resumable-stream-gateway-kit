import XCTest
@testable import StreamGateway

final class SSEParserTests: XCTestCase {
    private func parseAll(_ text: String, splitEvery size: Int? = nil) throws -> [SSEOutput] {
        var parser = SSEParser()
        let bytes = Array(text.utf8)
        guard let size else { return try parser.feed(bytes) }
        var out: [SSEOutput] = []
        var index = 0
        while index < bytes.count {
            out += try parser.feed(bytes[index..<min(index + size, bytes.count)])
            index += size
        }
        return out
    }

    private func events(_ outputs: [SSEOutput]) -> [SSEEvent] {
        outputs.compactMap { if case .event(let e) = $0 { return e } else { return nil } }
    }

    func testBasicEventWithAllFields() throws {
        let out = events(try parseAll("id: 7\nevent: delta\ndata: hello\n\n"))
        XCTAssertEqual(out, [SSEEvent(type: "delta", data: "hello", lastEventID: "7")])
    }

    func testMultiLineDataAndAllLineEndings() throws {
        let out = events(try parseAll("data: a\r\ndata: b\rdata: c\n\r\n"))
        XCTAssertEqual(out.first?.data, "a\nb\nc")
        XCTAssertEqual(out.count, 1)
    }

    func testEveryByteSplitGivesSameResultIncludingCRLFAndUTF8() throws {
        let text = "\u{FEFF}id: é1\r\nevent: x\r\ndata: 日本語 🚀\r\n\r\n: ping\r\ndata: two\r\n\r\n"
        let whole = try parseAll(text)
        XCTAssertEqual(try parseAll(text, splitEvery: 1), whole)
        XCTAssertEqual(events(whole).map(\.data), ["日本語 🚀", "two"])
        XCTAssertEqual(events(whole).last?.lastEventID, "é1", "id persists to later events")
    }

    func testBOMOnlyStrippedOnceAndPartialBOMIsContent() throws {
        XCTAssertEqual(events(try parseAll("\u{FEFF}data: x\n\n")).first?.data, "x")
        // A second BOM is part of the field name, so the line is an unknown field.
        XCTAssertEqual(events(try parseAll("\u{FEFF}\u{FEFF}data: x\n\n")), [])
        // A lone 0xEF that is not the start of a full BOM is content and must
        // not be swallowed: swallowing it would turn this line into a comment.
        var parser = SSEParser()
        let out = try parser.feed([0xEF] + Array(":c\n".utf8))
        XCTAssertEqual(out, [], "U+FFFD:c is an unknown field, not the comment \":c\"")
    }

    func testCommentAndRetry() throws {
        let out = try parseAll(": keepalive\nretry: 1500\nretry: 15x\nretry:\nretry: 99999999999999999999999999\n")
        XCTAssertEqual(out, [.comment(" keepalive"), .retry(milliseconds: 1500)])
    }

    func testIdWithNulIsIgnoredAndIdPersists() throws {
        let out = events(try parseAll("id: 1\ndata: a\n\nid: 2\u{0}\ndata: b\n\n"))
        XCTAssertEqual(out.map(\.lastEventID), ["1", "1"])
    }

    func testNoDataMeansNoDispatchButFieldWithoutColonIsEmptyData() throws {
        XCTAssertEqual(events(try parseAll("event: x\nid: 3\n\n")), [])
        XCTAssertEqual(events(try parseAll("data\n\n")).first?.data, "")
    }

    func testOnlyOneLeadingSpaceIsStripped() throws {
        XCTAssertEqual(events(try parseAll("data:  two spaces\n\n")).first?.data, " two spaces")
        XCTAssertEqual(events(try parseAll("data:none\n\n")).first?.data, "none")
    }

    func testUnterminatedEventIsDiscardedAtFinish() throws {
        var parser = SSEParser()
        XCTAssertEqual(try parser.feed(Array("data: half".utf8)), [])
        parser.finish()
        XCTAssertEqual(try parser.feed(Array("\n\n".utf8)), [], "the half line must not be applied later")
    }

    func testLineAndEventLimitsThrowInsteadOfGrowing() {
        var lines = SSEParser(maxLineBytes: 8)
        XCTAssertThrowsError(try lines.feed(Array("data: 123456789\n".utf8))) {
            XCTAssertEqual($0 as? SSEParserError, .lineTooLong(limit: 8))
        }
        var eventLimit = SSEParser(maxEventBytes: 10)
        XCTAssertThrowsError(try eventLimit.feed(Array("data: 12345\ndata: 12345\n".utf8))) {
            XCTAssertEqual($0 as? SSEParserError, .eventTooLarge(limit: 10))
        }
        // A single line larger than the event limit is also rejected (no overflow path).
        var single = SSEParser(maxEventBytes: 3)
        XCTAssertThrowsError(try single.feed(Array("data: 12345\n".utf8)))
        // Nonsense limits are clamped, not trapped on.
        XCTAssertEqual(SSEParser(maxLineBytes: -5, maxEventBytes: 0).maxLineBytes, 1)
    }

    // MARK: Round-trip property, plus proof that it catches a broken encoder.

    private static let alphabet: [String] = ["a", "Z", " ", "\n", "\r", "\r\n", ":", "é", "🚀", "\u{0}", "\"", "\\", "data: "]

    private func randomPayload(_ g: inout SeededGenerator) -> String {
        (0..<Int.random(in: 0...12, using: &g)).map { _ in Self.alphabet.randomElement(using: &g) ?? "a" }.joined()
    }

    private func roundTrips(_ payloads: [String], encode: (GatewayFrame) -> [UInt8],
                            decode: (SSEEvent) -> String? = { WireCodec.decode($0)?.payload },
                            seed: UInt64) throws -> Bool {
        var g = SeededGenerator(seed: seed)
        var bytes: [UInt8] = []
        let frames = payloads.enumerated().map {
            GatewayFrame(cursor: StreamCursor(epoch: "e", sequence: UInt64($0.offset + 1)), kind: .delta, payload: $0.element)
        }
        for frame in frames { bytes += encode(frame) }
        var parser = SSEParser()
        var decoded: [String] = []
        var index = 0
        while index < bytes.count {
            let size = Int.random(in: 1...7, using: &g)
            for case .event(let event) in try parser.feed(bytes[index..<min(index + size, bytes.count)]) {
                guard let payload = decode(event) else { return false }
                decoded.append(payload)
            }
            index += size
        }
        return decoded == payloads
    }

    func testWireCodecRoundTripsArbitraryPayloadsAtArbitrarySplits() throws {
        var g = SeededGenerator(seed: 42)
        for seed in 1...200 as ClosedRange<UInt64> {
            let payloads = (0..<5).map { _ in randomPayload(&g) }
            XCTAssertTrue(try roundTrips(payloads, encode: WireCodec.encode, seed: seed), "payloads \(payloads)")
        }
    }

    func testRoundTripCheckFailsForARawTextEncoder() throws {
        // The obvious alternative to JSON: write the payload straight into
        // `data:` (SSEEncoder splits it into lines) and read `event.data` back.
        // It is fine for plain text, so the check must pass on that...
        let raw: (GatewayFrame) -> [UInt8] = { SSEEncoder.encode(id: $0.cursor.description, event: "delta", data: $0.payload) }
        let rawDecode: (SSEEvent) -> String? = { $0.data }
        XCTAssertTrue(try roundTrips(["plain", "two\nlines"], encode: raw, decode: rawDecode, seed: 1))
        // ...and must catch what SSE line-ending normalisation does to model
        // output: "\r\n" and "\r" both come back as "\n".
        XCTAssertFalse(try roundTrips(["line1\r\nline2"], encode: raw, decode: rawDecode, seed: 1))
        XCTAssertFalse(try roundTrips(["a\rb"], encode: raw, decode: rawDecode, seed: 1))
        XCTAssertTrue(try roundTrips(["line1\r\nline2", "a\rb"], encode: WireCodec.encode, seed: 1))
    }

    func testEncoderStripsFieldBreakingCharacters() {
        let text = String(decoding: SSEEncoder.encode(id: "a\nb\u{0}", event: "x\ry", data: "d"), as: UTF8.self)
        XCTAssertEqual(text, "id: ab\nevent: xy\ndata: d\n\n")
    }
}
