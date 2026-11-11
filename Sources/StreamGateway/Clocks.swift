import Foundation

/// Monotonic time as a `Duration` since an arbitrary origin. Injected so the
/// retention and abandonment policies are testable without sleeping.
public protocol MonotonicClock: Sendable {
    func now() -> Duration
}

public struct SystemClock: MonotonicClock {
    private let origin = ContinuousClock.now
    public init() {}
    public func now() -> Duration { ContinuousClock.now - origin }
}

/// A clock that only moves when told to. Thread-safe.
public final class ManualClock: MonotonicClock, @unchecked Sendable {
    // @unchecked: all mutable state is guarded by `lock`.
    private let lock = NSLock()
    private var current: Duration = .zero
    public init() {}
    public func now() -> Duration { lock.withLock { current } }
    public func advance(by duration: Duration) {
        guard duration > .zero else { return }
        // Saturate instead of trapping on Duration overflow.
        let ceiling = Duration.seconds(Int64.max)
        lock.withLock { current = current > ceiling - duration ? ceiling : current + duration }
    }
}

/// Sleeping, injected so reconnect backoff is testable instantly.
public protocol Sleeper: Sendable {
    func sleep(for duration: Duration) async throws
}

public struct TaskSleeper: Sleeper {
    public init() {}
    public func sleep(for duration: Duration) async throws {
        guard duration > .zero else { try Task.checkCancellation(); return }
        try await Task.sleep(for: duration)
    }
}
