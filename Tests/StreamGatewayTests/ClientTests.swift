import XCTest
@testable import StreamGateway

/// End-to-end: gateway → SSE bytes (split into awkward chunks) → client.
final class ClientTests: XCTestCase {
    private func run(_ client: ResumableStreamClient) async -> [ClientEvent] {
        var events: [ClientEvent] = []
        for await event in await client.start() { events.append(event) }
        return events
    }

    private func client(_ transport: any StreamTransport, id: String = "s", failures: Int = 8,
                        checkpoint: TranscriptAssembler? = nil, recorder: DelayRecorder? = nil) -> ResumableStreamClient {
        ResumableStreamClient(streamID: id, transport: transport,
                              policy: ReconnectPolicy(baseDelay: .milliseconds(100), maxDelay: .seconds(2),
                                                      maxConsecutiveFailures: failures),
                              checkpoint: checkpoint, sleeper: InstantSleeper(recorder: recorder),
                              random: { 0.5 })
    }

    private func gatewayWithScript(_ tokens: [String], _ config: GatewayConfiguration = .init()) async throws -> StreamGateway {
        let gateway = StreamGateway(configuration: config)
        try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: tokens, interval: .milliseconds(1)))
        return gateway
    }

    func testResumesAcrossDroppedConnectionsWithOneUpstreamCall() async throws {
        let tokens = words(40)
        let gateway = try await gatewayWithScript(tokens)
        let transport = InMemoryTransport(gateway: gateway, faults: .init(dropAfterFrames: [1: 7, 2: 9]))
        let c = client(transport)
        let events = await run(c)
        let checkpoint = await c.checkpoint
        XCTAssertEqual(checkpoint.text, tokens.joined())
        XCTAssertEqual(events.last, .finished(.done(stopReason: "end_turn")))
        XCTAssertEqual(transport.connections, 3)
        let resumes = events.compactMap { if case .connected(_, let from) = $0 { return from } else { return nil } }
        XCTAssertEqual(resumes.count, 2, "connections 2 and 3 sent Last-Event-ID")
        XCTAssertEqual(StreamCursor(resumes[0])?.sequence, 7)
        XCTAssertEqual(StreamCursor(resumes[1])?.sequence, 16)
        let upstreamCalls = await gateway.stats().upstreamRequests
        XCTAssertEqual(upstreamCalls, 1)
    }

    func testAtLeastOnceDeliveryIsDeduplicated() async throws {
        let tokens = words(30)
        let gateway = try await gatewayWithScript(tokens)
        let transport = InMemoryTransport(gateway: gateway,
                                          faults: .init(dropAfterFrames: [1: 10], replayStaleFrames: [2: 6]))
        let c = client(transport)
        _ = await run(c)
        let checkpoint = await c.checkpoint
        XCTAssertEqual(checkpoint.text, tokens.joined())
        XCTAssertEqual(checkpoint.duplicatesDropped, 6)
    }

    func testAMissingFrameIsAProtocolViolationNotACorruptTranscript() async throws {
        let gateway = try await gatewayWithScript(words(20))
        let transport = InMemoryTransport(gateway: gateway, faults: .init(skipFrameAt: [1: 5]))
        let c = client(transport)
        let events = await run(c)
        XCTAssertEqual(events.last, .failed(.protocolViolation("expected frame 5, received 6")))
        let text = await c.checkpoint.text
        XCTAssertEqual(text, words(4).joined(), "nothing after the hole was applied")
    }

    func testReplayGapIsReportedAsRestartRequired() async throws {
        let gateway = try await gatewayWithScript(words(30), .init(replayFrameCapacity: 8))
        let done = await Wait.until { await gateway.snapshot(streamID: "s")?.phase == .finished }
        XCTAssertTrue(done)
        let events = await run(client(InMemoryTransport(gateway: gateway)))
        XCTAssertEqual(events.last, .failed(.rejected(.replayGap)))
    }

    func testGivesUpAfterConsecutiveFailuresWithBoundedBackoff() async throws {
        struct Down: StreamTransport {
            func connect(streamID: String, lastEventID: String?) async throws -> AsyncThrowingStream<[UInt8], any Error> {
                throw InMemoryTransport.Fault.connectionLost
            }
        }
        let recorder = DelayRecorder()
        let events = await run(client(Down(), failures: 4, recorder: recorder))
        XCTAssertEqual(events.last, .failed(.retriesExhausted(attempts: 4)))
        XCTAssertEqual(events.filter { if case .reconnecting = $0 { return true } else { return false } }.count, 3,
                       "4 attempts means 3 waits between them, not a 5th attempt")
        XCTAssertEqual(recorder.delays.count, 3)
        XCTAssertTrue(recorder.delays.allSatisfy { $0 >= .milliseconds(100) && $0 <= .seconds(2) })
    }

    /// With a budget of 1, a connection that drops after every single frame
    /// must still finish, because no attempt failed to make progress. A client
    /// that counted a progressing drop as a failure (or counted total rather
    /// than consecutive failures) would give up on the first drop.
    func testProgressResetsTheFailureBudget() async throws {
        let tokens = words(12)
        let gateway = try await gatewayWithScript(tokens)
        var drops: [Int: Int] = [:]
        for n in 1...20 { drops[n] = 1 }
        let transport = InMemoryTransport(gateway: gateway, faults: .init(dropAfterFrames: drops))
        let c = client(transport, failures: 1)
        let events = await run(c)
        XCTAssertEqual(events.last, .finished(.done(stopReason: "end_turn")))
        let text = await c.checkpoint.text
        XCTAssertEqual(text, tokens.joined())
        XCTAssertEqual(transport.connections, 13)
    }

    func testPersistedCheckpointResumesInANewClient() async throws {
        let tokens = words(25)
        let gateway = try await gatewayWithScript(tokens)
        let first = client(InMemoryTransport(gateway: gateway))
        var deltas = 0
        for await event in await first.start() {
            if case .delta = event { deltas += 1 }
            if deltas == 8 { break }
        }
        await first.stop()
        let saved = try JSONEncoder().encode(await first.checkpoint)
        let restored = try JSONDecoder().decode(TranscriptAssembler.self, from: saved)
        XCTAssertGreaterThanOrEqual(restored.lastSequence, 8, "the client may have applied buffered frames past the 8th")

        let transport = InMemoryTransport(gateway: gateway)
        let second = client(transport, checkpoint: restored)
        let events = await run(second)
        XCTAssertEqual(events.first, .connected(attempt: 1, resumingFrom: restored.lastEventID))
        let text = await second.checkpoint.text
        XCTAssertEqual(text, tokens.joined())
        let duplicates = await second.checkpoint.duplicatesDropped
        XCTAssertEqual(duplicates, 0)
    }

    func testServerRetryHintRaisesTheBackoffFloor() async throws {
        struct HintThenDrop: StreamTransport {
            func connect(streamID: String, lastEventID: String?) async throws -> AsyncThrowingStream<[UInt8], any Error> {
                AsyncThrowingStream { c in
                    c.yield(SSEEncoder.retry(milliseconds: 1500))
                    c.finish(throwing: InMemoryTransport.Fault.connectionLost)
                }
            }
        }
        let recorder = DelayRecorder()
        _ = await run(client(HintThenDrop(), failures: 3, recorder: recorder))
        XCTAssertEqual(recorder.delays.count, 2)
        XCTAssertTrue(recorder.delays.allSatisfy { $0 >= .milliseconds(1500) && $0 <= .seconds(2) })
    }

    func testStartTwiceAndFinishedCheckpointNeverConnect() async throws {
        struct MustNotConnect: StreamTransport {
            func connect(streamID: String, lastEventID: String?) async throws -> AsyncThrowingStream<[UInt8], any Error> {
                XCTFail("must not connect"); throw InMemoryTransport.Fault.connectionLost
            }
        }
        var done = TranscriptAssembler()
        done.apply(cursor: StreamCursor(epoch: "e", sequence: 1), kind: .done, payload: "end_turn")
        let finished = client(MustNotConnect(), checkpoint: done)
        let events = await run(finished)
        XCTAssertEqual(events, [.finished(.done(stopReason: "end_turn"))])
        let again = await run(finished)
        XCTAssertEqual(again, [.failed(.alreadyStarted)])
    }

    func testOversizedLineFromAHostileServerFailsCleanly() async throws {
        struct Flood: StreamTransport {
            func connect(streamID: String, lastEventID: String?) async throws -> AsyncThrowingStream<[UInt8], any Error> {
                AsyncThrowingStream { c in
                    c.yield(Array(repeating: UInt8(ascii: "a"), count: 70_000))
                    c.finish()
                }
            }
        }
        let events = await run(client(Flood()))
        guard case .failed(.malformedStream)? = events.last else { return XCTFail("\(events)") }
    }

    /// After a 410 the documented recovery is a new generation under a new
    /// stream ID. Re-opening the old ID joins the retained, gapped stream and
    /// would 410 again until retention expires; this pins both halves.
    func testRestartAfterReplayGapNeedsANewStreamID() async throws {
        let gateway = try await gatewayWithScript(words(30), .init(replayFrameCapacity: 8))
        let done = await Wait.until { await gateway.snapshot(streamID: "s")?.phase == .finished }
        XCTAssertTrue(done)
        let first = await run(client(InMemoryTransport(gateway: gateway)))
        XCTAssertEqual(first.last, .failed(.rejected(.replayGap)))

        let sameID = try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: words(3)))
        guard case .joined = sameID else { return XCTFail("same ID must join, not restart") }
        let again = await run(client(InMemoryTransport(gateway: gateway)))
        XCTAssertEqual(again.last, .failed(.rejected(.replayGap)))

        let fresh = try await gateway.open(streamID: "s-2", upstream: ScriptedUpstream(tokens: words(3)))
        guard case .started = fresh else { return XCTFail("a new ID starts a new generation") }
        let restarted = client(InMemoryTransport(gateway: gateway), id: "s-2")
        let events = await run(restarted)
        XCTAssertEqual(events.last, .finished(.done(stopReason: "end_turn")))
        let text = await restarted.checkpoint.text
        XCTAssertEqual(text, words(3).joined())
    }

    /// End to end: the host stops reading (a blocked socket write), the
    /// gateway cuts the connection, and the client resumes from the log.
    func testStalledHostConnectionIsCutAndTheClientResumes() async throws {
        let tokens = words(40)
        let gateway = StreamGateway(configuration: .init(subscriberQueueLimit: 3))
        try await gateway.open(streamID: "s", upstream: ScriptedUpstream(tokens: tokens, interval: .milliseconds(2)))
        let transport = InMemoryTransport(gateway: gateway,
                                          faults: .init(stallAfterFrames: [1: .init(afterFrames: 5, pause: .milliseconds(150))]))
        let c = client(transport)
        let events = await run(c)
        XCTAssertEqual(events.last, .finished(.done(stopReason: "end_turn")))
        let text = await c.checkpoint.text
        XCTAssertEqual(text, tokens.joined())
        let stats = await gateway.stats()
        XCTAssertGreaterThanOrEqual(stats.slowConsumerCuts, 1)
        XCTAssertGreaterThanOrEqual(transport.connections, 2)
        XCTAssertEqual(stats.upstreamRequests, 1)
    }
}
