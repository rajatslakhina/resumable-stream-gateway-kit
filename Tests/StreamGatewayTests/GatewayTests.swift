import XCTest
@testable import StreamGateway

final class GatewayTests: XCTestCase {
    private func finished(_ g: StreamGateway, _ id: String) async -> Bool {
        await Wait.until { await g.snapshot(streamID: id)?.phase == .finished }
    }

    private func newest(_ g: StreamGateway, _ id: String, atLeast n: UInt64) async -> Bool {
        await Wait.until { (await g.snapshot(streamID: id)?.newestSequence ?? 0) >= n }
    }

    func testOpenIsSingleFlightAndIdempotent() async throws {
        let gateway = StreamGateway()
        let upstream = ManualUpstream()
        let first = try await gateway.open(streamID: "s", upstream: upstream)
        let second = try await gateway.open(streamID: "s", upstream: upstream)
        guard case .started(let epoch) = first else { return XCTFail("expected started") }
        XCTAssertEqual(second, .joined(epoch: epoch))
        let stats = await gateway.stats()
        XCTAssertEqual(stats.upstreamRequests, 1)
        XCTAssertEqual(upstream.eventsCalls, 1, "the model was called once, not once per open")
    }

    func testSubscriberGetsEverythingAndResumeGetsExactlyTheRest() async throws {
        let gateway = StreamGateway()
        try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: words(10)))
        let ok = await finished(gateway, "s")
        XCTAssertTrue(ok)
        let all = await collect(try await gateway.subscribe(streamID: "s", lastEventID: nil))
        XCTAssertEqual(all.count, 11)
        XCTAssertEqual(all.last?.kind, .done)
        XCTAssertEqual(all.map(\.cursor.sequence), Array(1...11))
        let rest = await collect(try await gateway.subscribe(streamID: "s", lastEventID: all[3].cursor.description))
        XCTAssertEqual(rest, Array(all[4...]))
    }

    /// The seam test: 24 subscribers attach while a writer is appending, each
    /// resuming from a random earlier cursor. Every one must see a contiguous
    /// run ending in `.done`, with no frame missing or repeated.
    func testReplayToLiveSeamUnderAConcurrentWriter() async throws {
        let gateway = StreamGateway(configuration: .init(subscriberQueueLimit: 4096, maxSubscribersPerStream: 64))
        let upstream = ManualUpstream()
        try await gateway.open(streamID: "s", upstream: upstream)
        let total = 400
        let writer = Task {
            for i in 0..<total {
                upstream.send(.text("t\(i) "))
                if i % 7 == 0 { await Task.yield() }
                if i == total / 2 {
                    // Guarantee overlap: hold the second half until at least
                    // one subscriber is attached live, so the live path is
                    // always exercised (on a loaded machine the writer could
                    // otherwise finish before anyone subscribes).
                    _ = await Wait.until { (await gateway.snapshot(streamID: "s")?.subscribers ?? 0) > 0 }
                }
            }
            upstream.send(.finished(stopReason: "end_turn"))
        }
        let epoch = await gateway.snapshot(streamID: "s")?.epoch ?? ""
        let results = await withTaskGroup(of: (UInt64, [GatewayFrame]).self) { group in
            for i in 0..<24 {
                group.addTask {
                    try? await Task.sleep(for: .microseconds(i * 150))
                    let seen = await gateway.snapshot(streamID: "s")?.newestSequence ?? 0
                    let after = seen == 0 ? 0 : UInt64(i) % (seen + 1)
                    let id = after == 0 ? nil : StreamCursor(epoch: epoch, sequence: after).description
                    guard let sub = try? await gateway.subscribe(streamID: "s", lastEventID: id) else { return (after, []) }
                    return (after, await collect(sub))
                }
            }
            var all: [(UInt64, [GatewayFrame])] = []
            for await r in group { all.append(r) }
            return all
        }
        await writer.value
        let live = await gateway.stats().framesDeliveredLive
        XCTAssertGreaterThan(live, 0, "at least one subscriber must have attached while the writer was running")
        for (after, frames) in results {
            XCTAssertEqual(frames.map(\.cursor.sequence), Array((after + 1)...UInt64(total + 1)), "resumed after \(after)")
            XCTAssertEqual(frames.last?.kind, .done)
        }
    }

    func testSlowConsumerIsCutAndResumesWithoutLossOrDuplication() async throws {
        let gateway = StreamGateway(configuration: .init(subscriberQueueLimit: 4))
        let upstream = ManualUpstream()
        try await gateway.open(streamID: "s", upstream: upstream)
        let stalled = try await gateway.subscribe(streamID: "s", lastEventID: nil) // never read while writing
        for i in 1...20 { upstream.send(.text("\(i),")) }
        upstream.send(.finished(stopReason: "end_turn"))
        let done = await finished(gateway, "s")
        XCTAssertTrue(done)
        let firstPage = await collect(stalled)
        XCTAssertEqual(firstPage.map(\.cursor.sequence), [1, 2, 3, 4], "the queue held exactly its limit, then the connection ended")
        let cuts = await gateway.stats().slowConsumerCuts
        XCTAssertEqual(cuts, 1)

        // One resume delivers the whole backlog (17 frames, more than the
        // live limit of 4): replay is bounded by the log, not by the live queue.
        let rest = await collect(try await gateway.subscribe(streamID: "s", lastEventID: firstPage.last?.cursor.description))
        let received = firstPage + rest
        XCTAssertEqual(received.map(\.cursor.sequence), Array(1...21))
        XCTAssertEqual(received.filter { $0.kind == .delta }.map(\.payload).joined(),
                       (1...20).map { "\($0)," }.joined())
        let cutsAfter = await gateway.stats().slowConsumerCuts
        XCTAssertEqual(cutsAfter, 1, "the resume itself was not cut")
    }

    func testRejectionsAreSpecificAndCounted() async throws {
        let gateway = StreamGateway(configuration: .init(replayFrameCapacity: 5),
                                    epochFactory: { "fixed" })
        try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: words(20)))
        let done = await finished(gateway, "s")
        XCTAssertTrue(done)
        func rejection(_ id: String, _ cursor: String?) async -> StreamRejection? {
            do { _ = try await gateway.subscribe(streamID: id, lastEventID: cursor); return nil } catch { return error }
        }
        let unknown = await rejection("nope", nil)
        let malformed = await rejection("s", "garbage")
        let mismatch = await rejection("s", "other:3")
        let ahead = await rejection("s", "fixed:99")
        let gap = await rejection("s", "fixed:2")
        let fromStart = await rejection("s", nil)
        let ok = await rejection("s", "fixed:18")
        XCTAssertEqual(unknown, .unknownStream)
        XCTAssertEqual(malformed, .malformedCursor)
        XCTAssertEqual(mismatch, .cursorMismatch)
        XCTAssertEqual(ahead, .cursorAhead)
        XCTAssertEqual(gap, .replayGap)
        XCTAssertEqual(fromStart, .replayGap, "frame 1 was evicted, so a fresh subscriber would get a hole")
        XCTAssertNil(ok)
        let count = await gateway.stats().rejections
        XCTAssertEqual(count, 6)
        XCTAssertEqual(StreamRejection.replayGap.httpStatus, 410)
        XCTAssertFalse(StreamRejection.overCapacity.isPermanent)
    }

    func testRetentionThenEvictionAndReopenGetsANewEpoch() async throws {
        let clock = ManualClock()
        let gateway = StreamGateway(configuration: .init(retention: .seconds(60)), clock: clock)
        let first = try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: words(3)))
        let done = await finished(gateway, "s")
        XCTAssertTrue(done)
        clock.advance(by: .seconds(59))
        let stillThere = try? await gateway.subscribe(streamID: "s", lastEventID: nil)
        XCTAssertNotNil(stillThere)
        clock.advance(by: .seconds(1))
        do {
            _ = try await gateway.subscribe(streamID: "s", lastEventID: nil)
            XCTFail("expected eviction")
        } catch {
            XCTAssertEqual(error, .unknownStream)
        }
        let evicted = await gateway.stats().streamsEvicted
        XCTAssertEqual(evicted, 1)
        let second = try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: words(3)))
        XCTAssertNotEqual(first.epoch, second.epoch)
        do {
            _ = try await gateway.subscribe(streamID: "s", lastEventID: StreamCursor(epoch: first.epoch, sequence: 2).description)
            XCTFail("an old generation's cursor must not resume the new one")
        } catch {
            XCTAssertEqual(error, .cursorMismatch)
        }
    }

    func testUnattendedLiveStreamIsAbandonedAndUpstreamCancelled() async throws {
        let clock = ManualClock()
        let gateway = StreamGateway(configuration: .init(abandonTimeout: .seconds(30)), clock: clock)
        let upstream = ManualUpstream()
        try await gateway.open(streamID: "s", upstream: upstream)
        // A connection that reads until its task is cancelled (the socket closes).
        let reader = Task { () -> Int in
            var received = 0
            if let sub = try? await gateway.subscribe(streamID: "s", lastEventID: nil) {
                for await _ in sub.frames { received += 1 }
            }
            return received
        }
        let attached = await Wait.until { await gateway.snapshot(streamID: "s")?.subscribers == 1 }
        XCTAssertTrue(attached)
        clock.advance(by: .seconds(31))
        await gateway.sweep()
        let stillLive = await gateway.snapshot(streamID: "s")?.phase
        XCTAssertEqual(stillLive, .live, "an attached subscriber keeps the stream alive")
        upstream.send(.text("a"))
        _ = await newest(gateway, "s", atLeast: 1)
        reader.cancel() // the connection goes away
        _ = await reader.value
        let detached = await Wait.until { await gateway.snapshot(streamID: "s")?.subscribers == 0 }
        XCTAssertTrue(detached)
        clock.advance(by: .seconds(29))
        await gateway.sweep()
        let v1 = await gateway.snapshot(streamID: "s")?.phase
        XCTAssertEqual(v1, .live)
        clock.advance(by: .seconds(1))
        await gateway.sweep()
        let v2 = await gateway.snapshot(streamID: "s")?.phase
        XCTAssertEqual(v2, .finished)
        let cancelled = await Wait.until { upstream.terminated }
        XCTAssertTrue(cancelled, "the model call is cancelled, so abandoned streams stop costing tokens")
        let frames = await collect(try await gateway.subscribe(streamID: "s", lastEventID: nil))
        XCTAssertEqual(frames.last?.kind, .error)
        XCTAssertEqual(frames.last?.payload, "abandoned: no subscriber")
        let abandoned = await gateway.stats().streamsAbandoned
        XCTAssertEqual(abandoned, 1)
    }

    func testTruncationAndUpstreamErrorsAreNeverReportedAsDone() async throws {
        let gateway = StreamGateway()
        let truncated = ManualUpstream()
        try await gateway.open(streamID: "t", upstream: truncated)
        truncated.send(.text("partial"))
        truncated.continuation.finish()
        try await gateway.open(streamID: "f", upstream: ScriptedUpstream(tokens: words(5), failAfter: 2))
        let a = await finished(gateway, "t"), b = await finished(gateway, "f")
        XCTAssertTrue(a && b)
        let t = await collect(try await gateway.subscribe(streamID: "t", lastEventID: nil))
        let f = await collect(try await gateway.subscribe(streamID: "f", lastEventID: nil))
        XCTAssertEqual(t.map(\.kind), [.delta, .error])
        XCTAssertEqual(t.last?.payload, "upstream closed without a stop reason")
        XCTAssertEqual(f.map(\.kind), [.delta, .delta, .error])
        XCTAssertTrue(f.last?.payload.hasPrefix("upstream failed:") ?? false)
    }

    func testCancelEndsTheStreamForLateResumersToo() async throws {
        let gateway = StreamGateway()
        let upstream = ManualUpstream()
        try await gateway.open(streamID: "s", upstream: upstream)
        upstream.send(.text("a"))
        _ = await newest(gateway, "s", atLeast: 1)
        await gateway.cancel(streamID: "s")
        let gone = await Wait.until { upstream.terminated }
        XCTAssertTrue(gone)
        let frames = await collect(try await gateway.subscribe(streamID: "s", lastEventID: nil))
        XCTAssertEqual(frames.map(\.payload), ["a", "cancelled"])
    }

    func testAdmissionControlEvictsFinishedStreamsButNeverLiveOnes() async throws {
        let gateway = StreamGateway(configuration: .init(maxStreams: 2))
        try await gateway.open(streamID: "live1", upstream: ManualUpstream())
        try await gateway.open(streamID: "live2", upstream: ManualUpstream())
        do {
            try await gateway.open(streamID: "third", upstream: ManualUpstream())
            XCTFail("expected overCapacity")
        } catch {
            XCTAssertEqual(error, .overCapacity)
        }
        await gateway.cancel(streamID: "live1")
        try await gateway.open(streamID: "third", upstream: ManualUpstream())
        let v3 = await gateway.snapshot(streamID: "live1")
        XCTAssertNil(v3, "the finished stream made room")
        let v4 = await gateway.snapshot(streamID: "live2")
        XCTAssertNotNil(v4)
    }

    func testOversizedDeltaIsSplitSoTheByteBudgetHolds() async throws {
        let gateway = StreamGateway(configuration: .init(maxFramePayloadBytes: 4096))
        let huge = String(repeating: "é", count: 5000) // 10,000 bytes
        try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: [huge]))
        let done = await finished(gateway, "s")
        XCTAssertTrue(done)
        let frames = await collect(try await gateway.subscribe(streamID: "s", lastEventID: nil))
        let deltas = frames.filter { $0.kind == .delta }
        XCTAssertEqual(deltas.count, 3)
        XCTAssertTrue(deltas.allSatisfy { $0.payload.utf8.count <= 4096 })
        XCTAssertEqual(deltas.map(\.payload).joined(), huge)
    }

    func testSubscriberCapRejectsWithoutDisturbingExistingConnections() async throws {
        let gateway = StreamGateway(configuration: .init(maxSubscribersPerStream: 2))
        let upstream = ManualUpstream()
        try await gateway.open(streamID: "s", upstream: upstream)
        let a = try await gateway.subscribe(streamID: "s", lastEventID: nil)
        let b = try await gateway.subscribe(streamID: "s", lastEventID: nil)
        do {
            _ = try await gateway.subscribe(streamID: "s", lastEventID: nil)
            XCTFail("third connection must be refused")
        } catch {
            XCTAssertEqual(error, .tooManySubscribers)
            XCTAssertEqual(error.httpStatus, 429)
            XCTAssertFalse(error.isPermanent)
        }
        upstream.send(.text("x"))
        upstream.send(.finished(stopReason: "end_turn"))
        let fa = await collect(a), fb = await collect(b)
        XCTAssertEqual(fa.map(\.payload), ["x", "end_turn"])
        XCTAssertEqual(fb, fa)
    }

    func testSameStreamIDWithADifferentUpstreamJoinsAndIgnoresIt() async throws {
        let gateway = StreamGateway()
        let first = ManualUpstream(), second = ManualUpstream()
        try await gateway.open(streamID: "s", upstream: first)
        try await gateway.open(streamID: "s", upstream: second)
        XCTAssertEqual(first.eventsCalls, 1)
        XCTAssertEqual(second.eventsCalls, 0, "documented: the stream ID alone is the idempotency key")
    }

    func testInvalidInjectedEpochIsReplacedSoCursorsStayParseable() async throws {
        let gateway = StreamGateway(epochFactory: { "bad:epoch" })
        let outcome = try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: words(2)))
        XCTAssertFalse(outcome.epoch.contains(":"))
        XCTAssertEqual(StreamGateway.validEpoch("ok-epoch_1"), "ok-epoch_1")
        XCTAssertNotEqual(StreamGateway.validEpoch(""), "")
        let frames = await collect(try await gateway.subscribe(streamID: "s", lastEventID: nil))
        XCTAssertNotNil(frames.first.flatMap { StreamCursor($0.cursor.description) })
    }

    /// A resumed connection with a large backlog may still fall only
    /// `subscriberQueueLimit` *live* frames behind: the backlog is not extra
    /// room for live frames once it has been read.
    func testResumedConnectionLiveLagIsCappedRegardlessOfBacklog() async throws {
        let gateway = StreamGateway(configuration: .init(subscriberQueueLimit: 4))
        let upstream = ManualUpstream()
        try await gateway.open(streamID: "s", upstream: upstream)
        for i in 1...50 { upstream.send(.text("b\(i)")) }
        let backlogReady = await newest(gateway, "s", atLeast: 50)
        XCTAssertTrue(backlogReady)
        let sub = try await gateway.subscribe(streamID: "s", lastEventID: nil)
        var iterator = sub.frames.makeAsyncIterator()
        var read: [GatewayFrame] = []
        for _ in 0..<50 { if let f = await iterator.next() { read.append(f) } }
        XCTAssertEqual(read.map(\.cursor.sequence), Array(1...50), "backlog delivered in full")
        // Now stall: 20 live frames arrive, nobody reads.
        for i in 1...20 { upstream.send(.text("l\(i)")) }
        let written = await newest(gateway, "s", atLeast: 70)
        XCTAssertTrue(written)
        let cuts = await gateway.stats().slowConsumerCuts
        XCTAssertEqual(cuts, 1)
        var rest: [GatewayFrame] = []
        while let f = await iterator.next() { rest.append(f) }
        XCTAssertEqual(rest.map(\.cursor.sequence), [51, 52, 53, 54], "exactly subscriberQueueLimit live frames were buffered")
    }

    func testTerminalPayloadsAreTruncatedAtTheGateway() async throws {
        let gateway = StreamGateway()
        let huge = String(repeating: "x", count: 1_000_000)
        try await gateway.open(streamID: "done", upstream: ScriptedUpstream(tokens: ["a"], stopReason: huge))
        struct Boom: Error, CustomStringConvertible { let description: String }
        let failing = ManualUpstream()
        try await gateway.open(streamID: "err", upstream: failing)
        failing.continuation.finish(throwing: Boom(description: huge))
        let a = await Wait.until { await gateway.snapshot(streamID: "done")?.phase == .finished }
        let b = await Wait.until { await gateway.snapshot(streamID: "err")?.phase == .finished }
        XCTAssertTrue(a && b)
        let done = await collect(try await gateway.subscribe(streamID: "done", lastEventID: nil))
        let err = await collect(try await gateway.subscribe(streamID: "err", lastEventID: nil))
        XCTAssertLessThanOrEqual(done.last?.payload.utf8.count ?? .max, StreamGateway.terminalPayloadLimit)
        XCTAssertLessThanOrEqual(err.last?.payload.utf8.count ?? .max, StreamGateway.terminalPayloadLimit)
        XCTAssertEqual(err.last?.kind, .error)
    }

    func testLeasesOutliveTheCutUntilTheSubscriptionIsDropped() async throws {
        let gateway = StreamGateway(configuration: .init(subscriberQueueLimit: 1, maxSubscribersPerStream: 1))
        let upstream = ManualUpstream()
        try await gateway.open(streamID: "s", upstream: upstream)
        var held: GatewaySubscription? = try await gateway.subscribe(streamID: "s", lastEventID: nil)
        upstream.send(.text("1")); upstream.send(.text("2")) // the second one cuts it
        _ = await newest(gateway, "s", atLeast: 2)
        let cutCount = await gateway.stats().slowConsumerCuts
        XCTAssertEqual(cutCount, 1)
        do {
            _ = try await gateway.subscribe(streamID: "s", lastEventID: nil)
            XCTFail("the cut connection still pins its buffer, so it still holds the slot")
        } catch {
            XCTAssertEqual(error, .tooManySubscribers)
        }
        XCTAssertNotNil(held)
        held = nil
        let freed = await Wait.until { (try? await gateway.subscribe(streamID: "s", lastEventID: nil)) != nil }
        XCTAssertTrue(freed, "dropping the subscription releases the slot")
    }
}
