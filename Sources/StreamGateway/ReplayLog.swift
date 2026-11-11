/// Result of asking a replay log for everything after a cursor.
public enum ReplayResult: Sendable, Equatable {
    /// The frames strictly after the cursor, in order (possibly empty).
    case frames([GatewayFrame])
    /// The frames after the cursor were evicted. `oldestAvailable` is the
    /// first sequence still held. Resuming would produce a transcript with a hole.
    case gap(oldestAvailable: UInt64)
    /// The cursor names a frame this log never produced.
    case cursorAhead
}

/// A bounded, per-stream replay log: a ring buffer bounded both by frame count
/// and by bytes.
///
/// Invariants (each has a test):
/// 1. Sequences are contiguous: the held frames are exactly `oldest...newest`.
/// 2. `byteCount <= byteCapacity`, except that the newest frame is always kept
///    even if it alone exceeds the budget (dropping it would lose data that no
///    subscriber has seen yet).
/// 3. `count <= frameCapacity`.
struct ReplayLog: Sendable {
    let epoch: String
    let frameCapacity: Int
    let byteCapacity: Int

    private var storage: [GatewayFrame?]
    private var head = 0
    private(set) var count = 0
    private(set) var byteCount = 0
    /// The sequence the next appended frame will get.
    private(set) var nextSequence: UInt64 = 1

    /// Fixed per-frame overhead used in the byte accounting (cursor, kind,
    /// SSE framing). Approximate on purpose: the budget bounds memory order of
    /// magnitude, not exact allocation.
    static let frameOverhead = 64

    init(epoch: String, frameCapacity: Int, byteCapacity: Int) {
        self.epoch = epoch
        // Same ranges as GatewayConfiguration, so the log is bounded even if
        // constructed directly.
        self.frameCapacity = min(max(1, frameCapacity), 1 << 20)
        self.byteCapacity = min(max(1, byteCapacity), 1 << 30)
        // Storage grows lazily up to frameCapacity, so a large configured
        // capacity does not allocate up front.
        self.storage = []
    }

    var oldestSequence: UInt64? {
        guard count > 0 else { return nil }
        return nextSequence - UInt64(count)
    }

    var newest: GatewayFrame? {
        guard count > 0 else { return nil }
        return frame(atOffset: count - 1)
    }

    var isExhausted: Bool { nextSequence == UInt64.max }

    /// Slots actually allocated (grows lazily up to `frameCapacity`).
    var allocatedSlots: Int { storage.count }

    static func cost(of payload: String) -> Int {
        Saturating.add(payload.utf8.count, frameOverhead)
    }

    /// Appends a frame and evicts from the front until the budgets hold.
    /// Returns nil only when the sequence space is exhausted.
    @discardableResult
    mutating func append(kind: FrameKind, payload: String) -> GatewayFrame? {
        guard !isExhausted else { return nil }
        let frame = GatewayFrame(cursor: StreamCursor(epoch: epoch, sequence: nextSequence),
                                 kind: kind, payload: payload)
        nextSequence += 1 // Cannot overflow: guarded by isExhausted above.
        let cost = Self.cost(of: payload)

        if count == frameCapacity { evictOldest() }
        if count == storage.count {
            // The ring is full at its current size (and below frameCapacity,
            // because a full-capacity ring just evicted). Grow it: rotate so
            // the oldest frame is at index 0, then append.
            if head != 0 {
                storage = Array(storage[head...]) + Array(storage[..<head])
                head = 0
            }
            storage.append(frame)
        } else {
            let slot = (head + count) % storage.count
            storage[slot] = frame
        }
        count += 1
        byteCount = Saturating.add(byteCount, cost)
        while count > 1, byteCount > byteCapacity { evictOldest() }
        return frame
    }

    func replay(after sequence: UInt64?) -> ReplayResult {
        let after = sequence ?? 0
        let newestSequence = nextSequence - 1
        guard after <= newestSequence else { return .cursorAhead }
        guard let oldest = oldestSequence else { return .frames([]) }
        // The next frame the subscriber needs is after + 1. It must still be held.
        if after < newestSequence, after + 1 < oldest { return .gap(oldestAvailable: oldest) }
        if after >= newestSequence { return .frames([]) }
        let startOffset = Int(after + 1 - oldest) // in 0..<count, so it fits in Int
        var result: [GatewayFrame] = []
        result.reserveCapacity(count - startOffset)
        for offset in startOffset..<count {
            if let frame = frame(atOffset: offset) { result.append(frame) }
        }
        return .frames(result)
    }

    private func frame(atOffset offset: Int) -> GatewayFrame? {
        guard offset >= 0, offset < count, !storage.isEmpty else { return nil }
        let index = (head + offset) % storage.count
        guard storage.indices.contains(index) else { return nil }
        return storage[index]
    }

    private mutating func evictOldest() {
        guard count > 0, storage.indices.contains(head), let frame = storage[head] else { return }
        storage[head] = nil
        head = (head + 1) % storage.count
        count -= 1
        byteCount = max(0, byteCount - Self.cost(of: frame.payload))
    }
}
