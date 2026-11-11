import Foundation
import XCTest
@testable import StreamGateway

/// Returns immediately, so reconnect backoff costs no wall time in tests.
struct InstantSleeper: Sleeper {
    let recorder: DelayRecorder?
    init(recorder: DelayRecorder? = nil) { self.recorder = recorder }
    func sleep(for duration: Duration) async throws {
        recorder?.record(duration)
        await Task.yield()
        try Task.checkCancellation()
    }
}

final class DelayRecorder: @unchecked Sendable {
    // @unchecked: guarded by lock.
    private let lock = NSLock()
    private var values: [Duration] = []
    func record(_ d: Duration) { lock.withLock { values.append(d) } }
    var delays: [Duration] { lock.withLock { values } }
}

/// An upstream the test drives by hand.
final class ManualUpstream: StreamUpstream, @unchecked Sendable {
    // @unchecked: `terminated` is guarded by lock; the continuation is thread-safe.
    let stream: AsyncThrowingStream<UpstreamEvent, any Error>
    let continuation: AsyncThrowingStream<UpstreamEvent, any Error>.Continuation
    private let lock = NSLock()
    private var terminatedFlag = false
    private var calls = 0

    init() {
        (stream, continuation) = AsyncThrowingStream.makeStream(of: UpstreamEvent.self, throwing: (any Error).self)
        continuation.onTermination = { [weak self] _ in self?.markTerminated() }
    }
    private func markTerminated() { lock.withLock { terminatedFlag = true } }
    var terminated: Bool { lock.withLock { terminatedFlag } }
    var eventsCalls: Int { lock.withLock { calls } }
    func events() -> AsyncThrowingStream<UpstreamEvent, any Error> {
        lock.withLock { calls += 1 }
        return stream
    }
    func send(_ event: UpstreamEvent) { continuation.yield(event) }
}

struct SeededGenerator: RandomNumberGenerator {
    var state: UInt64
    init(seed: UInt64) { state = seed == 0 ? 0x9E3779B97F4A7C15 : seed }
    mutating func next() -> UInt64 { // xorshift64*
        state ^= state >> 12; state ^= state << 25; state ^= state >> 27
        return state &* 2685821657736338717
    }
}

enum Wait {
    /// Polls `condition` until true, yielding and sleeping briefly. Fails the
    /// caller by returning false after `timeout`.
    static func until(timeout: Duration = .seconds(5), _ condition: @Sendable () async -> Bool) async -> Bool {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            if await condition() { return true }
            try? await Task.sleep(for: .milliseconds(2))
        }
        return await condition()
    }
}

/// Reads a subscription to the end. A watchdog cancels the read after
/// `timeout` and records a test failure, so a bug that leaves a stream open
/// fails the test instead of hanging the suite (or silently passing).
func collect(_ subscription: GatewaySubscription, timeout: Duration = .seconds(5),
             file: StaticString = #filePath, line: UInt = #line) async -> [GatewayFrame] {
    let reader = Task { () -> [GatewayFrame] in
        var frames: [GatewayFrame] = []
        for await frame in subscription.frames { frames.append(frame) }
        return frames
    }
    let watchdog = Task { () -> Bool in
        do { try await Task.sleep(for: timeout) } catch { return false }
        reader.cancel()
        return true
    }
    let frames = await reader.value
    watchdog.cancel()
    if await watchdog.value {
        XCTFail("subscription did not end within \(timeout); read \(frames.count) frames", file: file, line: line)
    }
    return frames
}

func words(_ n: Int) -> [String] { (1...max(1, n)).map { "w\($0) " } }
