import XCTest
@testable import StreamGateway

final class ReplayLogTests: XCTestCase {
    func testEvictsByFrameCountAndStaysContiguous() {
        var log = ReplayLog(epoch: "e", frameCapacity: 3, byteCapacity: .max)
        for i in 1...5 { log.append(kind: .delta, payload: "\(i)") }
        XCTAssertEqual(log.oldestSequence, 3)
        XCTAssertEqual(log.replay(after: 2), .frames(log.framesForTest(3...5)))
        XCTAssertEqual(log.replay(after: 1), .gap(oldestAvailable: 3))
        XCTAssertEqual(log.replay(after: nil), .gap(oldestAvailable: 3), "a fresh subscriber cannot start mid-stream")
        XCTAssertEqual(log.replay(after: 5), .frames([]))
        XCTAssertEqual(log.replay(after: 6), .cursorAhead)
    }

    func testEvictsByBytesButAlwaysKeepsTheNewestFrame() {
        let big = String(repeating: "x", count: 500)
        var log = ReplayLog(epoch: "e", frameCapacity: 100, byteCapacity: 300)
        log.append(kind: .delta, payload: "a")
        log.append(kind: .delta, payload: big)
        XCTAssertEqual(log.count, 1, "the oversized newest frame is kept, the older one evicted")
        XCTAssertEqual(log.newest?.payload, big)
        log.append(kind: .delta, payload: "b")
        XCTAssertEqual(log.count, 1)
        XCTAssertLessThanOrEqual(log.byteCount, 300)
    }

    /// Model-based test against a deliberately naive reference (an array with
    /// removeFirst). Mixed byte- and count-driven evictions exercise the ring's
    /// grow-while-wrapped path, which once overwrote the oldest frame.
    func testMatchesNaiveReferenceUnderRandomOperations() {
        var g = SeededGenerator(seed: 7)
        for trial in 0..<300 {
            let frameCap = Int.random(in: 1...9, using: &g)
            let byteCap = Int.random(in: 70...400, using: &g)
            var log = ReplayLog(epoch: "e", frameCapacity: frameCap, byteCapacity: byteCap)
            var reference: [(UInt64, String)] = []
            var next: UInt64 = 1
            for _ in 0..<Int.random(in: 1...40, using: &g) {
                let payload = String(repeating: "p", count: Int.random(in: 0...120, using: &g))
                log.append(kind: .delta, payload: payload)
                reference.append((next, payload)); next += 1
                if reference.count > frameCap { reference.removeFirst() }
                while reference.count > 1,
                      reference.reduce(0, { $0 + $1.1.utf8.count + ReplayLog.frameOverhead }) > byteCap {
                    reference.removeFirst()
                }
                guard case .frames(let held) = log.replay(after: reference.first.map { $0.0 - 1 }) else {
                    return XCTFail("trial \(trial): expected frames")
                }
                XCTAssertEqual(held.map(\.cursor.sequence), reference.map(\.0), "trial \(trial)")
                XCTAssertEqual(held.map(\.payload), reference.map(\.1), "trial \(trial)")
            }
        }
    }

    func testAbsurdCapacitiesAreClampedAndDoNotPreallocate() {
        var log = ReplayLog(epoch: "e", frameCapacity: .max, byteCapacity: .max)
        XCTAssertEqual(log.allocatedSlots, 0)
        log.append(kind: .delta, payload: "x")
        XCTAssertEqual(log.count, 1)
        XCTAssertEqual(log.allocatedSlots, 1, "storage grows with use, not with the configured capacity")
        XCTAssertEqual(log.frameCapacity, 1 << 20)
        XCTAssertEqual(log.byteCapacity, 1 << 30)
        let tiny = ReplayLog(epoch: "e", frameCapacity: -3, byteCapacity: 0)
        XCTAssertEqual(tiny.frameCapacity, 1)
        XCTAssertEqual(tiny.byteCapacity, 1)
    }
}

extension ReplayLog {
    func framesForTest(_ range: ClosedRange<UInt64>) -> [GatewayFrame] {
        guard case .frames(let all) = replay(after: (oldestSequence ?? 1) - 1) else { return [] }
        return all.filter { range.contains($0.cursor.sequence) }
    }
}

final class CursorTests: XCTestCase {
    func testRoundTrip() {
        let cursor = StreamCursor(epoch: "00ab", sequence: 42)
        XCTAssertEqual(StreamCursor(cursor.description), cursor)
    }

    func testRejectsMalformed() {
        for text in ["", "abc", ":5", "a:", "a:0", "a:-1", "a:+1", "a: 1", "a:1x",
                     "a:18446744073709551616", "a:b:3"] {
            XCTAssertNil(StreamCursor(text), text)
        }
        XCTAssertEqual(StreamCursor("a:18446744073709551615")?.sequence, .max)
    }

    func testEpochIsSixteenHexDigits() {
        var g = SeededGenerator(seed: 1)
        let epoch = StreamCursor.makeEpoch(using: &g)
        XCTAssertEqual(epoch.count, 16)
        XCTAssertTrue(epoch.allSatisfy(\.isHexDigit))
    }
}

final class TranscriptAssemblerTests: XCTestCase {
    private func c(_ s: UInt64, _ e: String = "e") -> StreamCursor { StreamCursor(epoch: e, sequence: s) }

    func testAppliesInOrderAndRejectsEverythingElse() {
        var a = TranscriptAssembler()
        XCTAssertEqual(a.apply(cursor: c(2), kind: .delta, payload: "x"), .gap(expected: 1, received: 2))
        XCTAssertNil(a.epoch, "a rejected frame must not adopt an epoch")
        XCTAssertEqual(a.apply(cursor: c(1), kind: .delta, payload: "Hel"), .applied)
        XCTAssertEqual(a.apply(cursor: c(1), kind: .delta, payload: "Hel"), .duplicate)
        XCTAssertEqual(a.apply(cursor: c(2, "other"), kind: .delta, payload: "!"), .epochMismatch)
        XCTAssertEqual(a.apply(cursor: c(3), kind: .delta, payload: "?"), .gap(expected: 2, received: 3))
        XCTAssertEqual(a.apply(cursor: c(2), kind: nil, payload: "future"), .applied)
        XCTAssertEqual(a.apply(cursor: c(3), kind: .delta, payload: "lo"), .applied)
        XCTAssertEqual(a.apply(cursor: c(4), kind: .done, payload: "end_turn"), .applied)
        XCTAssertEqual(a.apply(cursor: c(5), kind: .delta, payload: "late"), .afterTerminal)
        XCTAssertEqual(a.text, "Hello")
        XCTAssertEqual(a.terminal, .done(stopReason: "end_turn"))
        XCTAssertEqual(a.duplicatesDropped, 1)
        XCTAssertEqual(a.unknownFramesSkipped, 1)
        XCTAssertEqual(a.lastEventID, "e:4")
    }

    func testCodableCheckpointPreservesResumePoint() throws {
        var a = TranscriptAssembler()
        a.apply(cursor: c(1), kind: .delta, payload: "a\r\nb")
        let restored = try JSONDecoder().decode(TranscriptAssembler.self, from: JSONEncoder().encode(a))
        XCTAssertEqual(restored, a)
        XCTAssertEqual(restored.lastEventID, "e:1")
        XCTAssertNil(TranscriptAssembler().lastEventID)
    }
}

final class ReconnectPolicyTests: XCTestCase {
    func testDelaysStayInBoundsForAnyUnitIncludingNonFinite() {
        let p = ReconnectPolicy(baseDelay: .milliseconds(100), maxDelay: .seconds(2))
        var previous: Duration?
        for unit in [0, 0.5, 0.999, 1, -1, 7, .nan, .infinity, -.infinity] {
            let d = p.nextDelay(previous: previous, serverHint: nil, unit: unit)
            XCTAssertGreaterThanOrEqual(d, .milliseconds(100))
            XCTAssertLessThanOrEqual(d, .seconds(2))
            previous = d
        }
    }

    func testGrowsTowardTheCapAndHonoursTheServerHint() {
        let p = ReconnectPolicy(baseDelay: .milliseconds(100), maxDelay: .seconds(2))
        var d = p.nextDelay(previous: nil, serverHint: nil, unit: 1)
        XCTAssertEqual(d, .milliseconds(300))
        d = p.nextDelay(previous: d, serverHint: nil, unit: 1)
        XCTAssertEqual(d, .milliseconds(900))
        d = p.nextDelay(previous: d, serverHint: nil, unit: 1)
        XCTAssertEqual(d, .seconds(2))
        XCTAssertEqual(p.nextDelay(previous: nil, serverHint: .milliseconds(1500), unit: 0), .milliseconds(1500))
        XCTAssertEqual(p.nextDelay(previous: nil, serverHint: .seconds(60), unit: 0), .seconds(2), "hint is capped")
    }

    func testExtremeConfigurationDoesNotTrap() {
        let p = ReconnectPolicy(baseDelay: .seconds(Int64.max), maxDelay: .seconds(Int64.max),
                                maxConsecutiveFailures: .max)
        let d = p.nextDelay(previous: .seconds(Int64.max), serverHint: .seconds(Int64.max), unit: 1)
        XCTAssertGreaterThan(d, .zero)
        XCTAssertEqual(p.maxConsecutiveFailures, 1000)
        let negative = ReconnectPolicy(baseDelay: .seconds(-5), maxDelay: .seconds(-9), maxConsecutiveFailures: -1)
        XCTAssertEqual(negative.baseDelay, .milliseconds(1))
        XCTAssertEqual(negative.maxConsecutiveFailures, 1)
    }
}

final class SaturatingTests: XCTestCase {
    func testNoTrapAtTheEdges() {
        XCTAssertEqual(Saturating.add(.max, 1), .max)
        XCTAssertEqual(Saturating.add(.min, -1), .min)
        XCTAssertEqual(Saturating.multiply(.max, 2), .max)
        XCTAssertEqual(Saturating.multiply(.min, 2), .min)
        XCTAssertEqual(Saturating.multiply(-1, .min), .max)
        XCTAssertEqual(Saturating.int(.nan), 0)
        XCTAssertEqual(Saturating.int(.infinity), .max)
        XCTAssertEqual(Saturating.int(-.infinity), .min)
        XCTAssertEqual(Saturating.int(1e300), .max)
        XCTAssertEqual(Saturating.milliseconds(.seconds(Int64.max)), (Int.max / 1000) * 1000)
        XCTAssertEqual(Saturating.milliseconds(.milliseconds(1500)), 1500)
    }

    func testGatewayConfigurationClampsAndWorstCaseFollowsTheFormula() {
        let c = GatewayConfiguration(replayFrameCapacity: .max, replayByteCapacity: .max, maxFramePayloadBytes: -1,
                                     subscriberQueueLimit: 0, maxSubscribersPerStream: .max,
                                     retention: .seconds(-1), abandonTimeout: .seconds(-1), maxStreams: .max)
        XCTAssertEqual(c.replayFrameCapacity, 1 << 20)
        XCTAssertEqual(c.replayByteCapacity, 1 << 30)
        XCTAssertEqual(c.maxFramePayloadBytes, 16)
        XCTAssertEqual(c.subscriberQueueLimit, 1)
        XCTAssertEqual(c.maxSubscribersPerStream, 1024)
        XCTAssertEqual(c.retention, .zero)
        XCTAssertEqual(c.maxStreams, 1 << 20)
        // Small numbers, computed by hand: frame cost = max(100, 256) + 64 = 320;
        // log = 1000 + 320 = 1320; per connection = 1320 + 4 * 320 = 2600;
        // per stream = 1320 + 2 * 2600 = 6520; three streams = 19560.
        let small = GatewayConfiguration(replayByteCapacity: 1000, maxFramePayloadBytes: 100,
                                         subscriberQueueLimit: 4, maxSubscribersPerStream: 2, maxStreams: 3)
        XCTAssertEqual(small.worstCaseBytes, 19_560)
        // `with` re-clamps.
        XCTAssertEqual(small.with(subscriberQueueLimit: -9).subscriberQueueLimit, 1)
    }

    func testSplitBoundsEvenASingleHugeGrapheme() {
        let zalgo = "e" + String(repeating: "\u{0301}", count: 1000) // one Character, 2001 bytes
        XCTAssertEqual(zalgo.count, 1)
        let pieces = StreamGateway.split("ab" + zalgo + "cd", maxBytes: 16)
        XCTAssertEqual(pieces.joined(), "ab" + zalgo + "cd")
        XCTAssertTrue(pieces.allSatisfy { $0.utf8.count <= 16 }, "no piece may exceed the frame limit")
    }

    func testTerminalPayloadsAreBoundedInBytes() {
        let long = String(repeating: "日", count: 500) // 1500 bytes
        let cut = StreamGateway.truncate(long)
        XCTAssertLessThanOrEqual(cut.utf8.count, StreamGateway.terminalPayloadLimit)
        XCTAssertTrue(cut.hasSuffix("…"))
        XCTAssertEqual(StreamGateway.truncate("short"), "short")
    }

    func testSplitKeepsGraphemesAndReproducesText() {
        let text = String(repeating: "ab👨‍👩‍👧‍👦", count: 50)
        let pieces = StreamGateway.split(text, maxBytes: 32)
        XCTAssertEqual(pieces.joined(), text)
        XCTAssertGreaterThan(pieces.count, 1)
        // The family emoji is 25 bytes, under the 32-byte limit, so no piece
        // may cut it: every piece is a whole number of graphemes.
        for piece in pieces {
            XCTAssertLessThanOrEqual(piece.utf8.count, 32)
            XCTAssertFalse(piece.unicodeScalars.first.map { $0.properties.isJoinControl } ?? false, piece)
        }
        XCTAssertEqual(pieces.map(\.count).reduce(0, +), text.count, "no grapheme was split across pieces")
        XCTAssertEqual(StreamGateway.split("", maxBytes: 16), [""])
    }
}

#if canImport(Darwin)
final class URLSessionStreamTransportTests: XCTestCase {
    func testRequestCarriesTheResumeCursor() throws {
        let base = try XCTUnwrap(URL(string: "https://gateway.example.com/v1"))
        let transport = URLSessionStreamTransport(baseURL: base)
        let request = transport.request(streamID: "abc", lastEventID: "e:7")
        XCTAssertEqual(request.url?.absoluteString, "https://gateway.example.com/v1/streams/abc")
        XCTAssertEqual(request.value(forHTTPHeaderField: "Last-Event-ID"), "e:7")
        XCTAssertEqual(request.value(forHTTPHeaderField: "Accept"), "text/event-stream")
        XCTAssertNil(transport.request(streamID: "abc", lastEventID: nil).value(forHTTPHeaderField: "Last-Event-ID"))
    }

    func testStatusMappingSeparatesRestartFromRetry() {
        for rejection in StreamRejection.allCases {
            XCTAssertEqual(URLSessionStreamTransport.rejection(status: rejection.httpStatus, header: rejection.rawValue),
                           rejection)
        }
        XCTAssertEqual(URLSessionStreamTransport.rejection(status: 410, header: nil), .unknownStream)
        XCTAssertNil(URLSessionStreamTransport.rejection(status: 502, header: nil), "a bad gateway is transient")
        XCTAssertNil(URLSessionStreamTransport.rejection(status: 500, header: "replayGap"),
                     "a header that disagrees with the status is not trusted")
    }
}
#endif
