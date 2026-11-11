/// Capped, decorrelated-jitter backoff ("AWS Architecture Blog: Exponential
/// Backoff and Jitter"), with the server's SSE `retry:` hint as a floor.
///
/// Decorrelated jitter spreads a fleet of phones that all lost the same cell
/// tower, so their reconnects do not arrive at the gateway in lockstep.
public struct ReconnectPolicy: Sendable, Equatable {
    public let baseDelay: Duration
    public let maxDelay: Duration
    /// Consecutive failed attempts (attempts that delivered no new frame)
    /// before giving up. Any attempt that delivers a frame resets the count.
    public let maxConsecutiveFailures: Int

    public init(baseDelay: Duration = .milliseconds(250),
                maxDelay: Duration = .seconds(15),
                maxConsecutiveFailures: Int = 8) {
        let base = max(.milliseconds(1), baseDelay)
        self.baseDelay = base
        self.maxDelay = max(base, maxDelay)
        self.maxConsecutiveFailures = min(max(1, maxConsecutiveFailures), 1000)
    }

    /// The next delay, given the previous one, the server hint and a random
    /// unit value in [0, 1). Always within `[max(base, hint), maxDelay]`
    /// (the hint is itself capped at `maxDelay`).
    public func nextDelay(previous: Duration?, serverHint: Duration?, unit: Double) -> Duration {
        let baseMs = Saturating.milliseconds(baseDelay)
        let capMs = Saturating.milliseconds(maxDelay)
        let hintMs = min(capMs, max(0, serverHint.map(Saturating.milliseconds) ?? 0))
        let floorMs = max(baseMs, hintMs)
        let previousMs = max(floorMs, previous.map(Saturating.milliseconds) ?? floorMs)
        let upperMs = min(capMs, Saturating.multiply(previousMs, 3))
        let u = unit.isFinite ? min(max(unit, 0), 1) : 0
        let spanMs = max(0, upperMs - floorMs)
        let jitterMs = Saturating.int(Double(spanMs) * u)
        let chosen = min(capMs, max(floorMs, Saturating.add(floorMs, jitterMs)))
        return .milliseconds(chosen)
    }
}
