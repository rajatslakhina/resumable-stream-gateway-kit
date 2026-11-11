import Foundation

/// A `StreamTransport` that talks to an in-process `StreamGateway` through
/// real SSE bytes, with scripted network faults.
///
/// It is the reference for what a URLSession transport + HTTP host must do:
/// encode each frame with `WireCodec`, and map rejections to errors. Each
/// frame's bytes are delivered in several chunks, split at arbitrary byte
/// offsets (mid-field, mid-line, and mid-UTF-8 scalar when the payload has
/// any), so the parser sees the boundaries a real network produces. CRLF and
/// BOM handling are covered separately by `SSEParserTests`.
public final class InMemoryTransport: StreamTransport, @unchecked Sendable {
    // @unchecked: mutable state (`attempt`) is guarded by `lock`.

    public struct Faults: Sendable, Equatable {
        /// For connection N (1-based), drop the connection after this many frames.
        public let dropAfterFrames: [Int: Int]
        /// For connection N, re-send this many already-delivered frames first
        /// (an at-least-once proxy replaying from a stale cursor).
        public let replayStaleFrames: [Int: Int]
        /// For connection N, stop reading from the gateway for this long after
        /// this many frames, as (afterFrames, pause): a host whose socket write
        /// to the phone is blocked. Note this transport forwards bytes to the
        /// client through an unbuffered-limit stream, so a slow *client* alone
        /// does not push back on the gateway here; in a real host the socket does.
        public let stallAfterFrames: [Int: StallFault]
        /// For connection N, silently skip one frame at this 1-based position
        /// (a broken proxy). The client must detect the gap.
        public let skipFrameAt: [Int: Int]
        /// Split each frame's bytes into this many chunks (1 = no splitting).
        public let chunksPerFrame: Int

        public init(dropAfterFrames: [Int: Int] = [:], replayStaleFrames: [Int: Int] = [:],
                    stallAfterFrames: [Int: StallFault] = [:], skipFrameAt: [Int: Int] = [:],
                    chunksPerFrame: Int = 3) {
            self.dropAfterFrames = dropAfterFrames
            self.replayStaleFrames = replayStaleFrames
            self.stallAfterFrames = stallAfterFrames
            self.skipFrameAt = skipFrameAt
            self.chunksPerFrame = min(max(1, chunksPerFrame), 64)
        }
    }

    public struct StallFault: Sendable, Equatable {
        public let afterFrames: Int
        public let pause: Duration
        public init(afterFrames: Int, pause: Duration) {
            self.afterFrames = afterFrames
            self.pause = pause
        }
    }

    public enum Fault: Error, Equatable, Sendable { case connectionLost }

    private let gateway: StreamGateway
    private let faults: Faults
    private let sleeper: any Sleeper
    private let lock = NSLock()
    private var attempt = 0
    private var delivered: [GatewayFrame] = []

    public init(gateway: StreamGateway, faults: Faults = .init(), sleeper: any Sleeper = TaskSleeper()) {
        self.gateway = gateway
        self.faults = faults
        self.sleeper = sleeper
    }

    public var connections: Int { lock.withLock { attempt } }

    public func connect(streamID: String, lastEventID: String?) async throws -> AsyncThrowingStream<[UInt8], any Error> {
        let number: Int = lock.withLock { attempt += 1; return attempt }
        let subscription = try await gateway.subscribe(streamID: streamID, lastEventID: lastEventID)
        let stale: [GatewayFrame] = lock.withLock {
            let count = min(max(0, faults.replayStaleFrames[number] ?? 0), delivered.count)
            return Array(delivered.suffix(count))
        }
        let dropAfter = faults.dropAfterFrames[number]
        let stall = faults.stallAfterFrames[number]
        let skipAt = faults.skipFrameAt[number]
        let chunks = faults.chunksPerFrame
        let sleeper = sleeper

        return AsyncThrowingStream { continuation in
            let task = Task {
                for frame in stale {
                    for chunk in Self.chunk(WireCodec.encode(frame), into: chunks) { continuation.yield(chunk) }
                }
                var count = 0
                for await frame in subscription.frames {
                    count += 1
                    if let skipAt, count == skipAt { continue }
                    for chunk in Self.chunk(WireCodec.encode(frame), into: chunks) { continuation.yield(chunk) }
                    self.record(frame)
                    if let stall, count == stall.afterFrames {
                        try? await sleeper.sleep(for: stall.pause)
                    }
                    if let dropAfter, count >= dropAfter {
                        continuation.finish(throwing: Fault.connectionLost)
                        return
                    }
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    private func record(_ frame: GatewayFrame) {
        lock.withLock {
            delivered.append(frame)
            if delivered.count > 64 { delivered.removeFirst(delivered.count - 64) }
        }
    }

    static func chunk(_ bytes: [UInt8], into parts: Int) -> [[UInt8]] {
        guard parts > 1, bytes.count > 1 else { return [bytes] }
        // Ceiling division written so it cannot overflow for any `parts`.
        let size = max(1, bytes.count / parts + (bytes.count % parts == 0 ? 0 : 1))
        return stride(from: 0, to: bytes.count, by: size).map { start in
            Array(bytes[start..<min(start + size, bytes.count)])
        }
    }
}
