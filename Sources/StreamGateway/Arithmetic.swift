/// Saturating helpers. Every numeric path reachable from public configuration
/// goes through these, so no input can trap on overflow.
enum Saturating {
    static func add(_ a: Int, _ b: Int) -> Int {
        let (r, o) = a.addingReportingOverflow(b)
        return o ? (b > 0 ? Int.max : Int.min) : r
    }

    static func multiply(_ a: Int, _ b: Int) -> Int {
        let (r, o) = a.multipliedReportingOverflow(by: b)
        guard o else { return r }
        return (a < 0) == (b < 0) ? Int.max : Int.min
    }

    /// `Int(Double)` traps on NaN, infinity and out-of-range values. This does not.
    static func int(_ value: Double) -> Int {
        guard value.isFinite else { return value.isNaN ? 0 : (value > 0 ? Int.max : Int.min) }
        if value >= Double(Int.max) { return Int.max }
        if value <= Double(Int.min) { return Int.min }
        return Int(value)
    }

    /// Milliseconds in a `Duration`, clamped to the `Int` range (32-bit safe).
    static func milliseconds(_ duration: Duration) -> Int {
        let (seconds, attoseconds) = duration.components
        let secondsAsInt: Int = seconds > Int64(Int.max / 1000) ? Int.max / 1000
            : (seconds < Int64(Int.min / 1000) ? Int.min / 1000 : Int(seconds))
        return add(multiply(secondsAsInt, 1000), Int(attoseconds / 1_000_000_000_000_000))
    }
}
