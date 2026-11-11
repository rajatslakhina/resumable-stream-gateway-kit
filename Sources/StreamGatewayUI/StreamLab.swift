#if canImport(SwiftUI) && canImport(Observation)
import Foundation
import Observation
import StreamGateway
import SwiftUI

/// A network fault to replay against the gateway + client pair.
public enum LabScenario: String, CaseIterable, Identifiable, Sendable {
    case dropMidStream
    case suspendAndRelaunch
    case slowConsumer
    case duplicateDelivery
    case replayGap
    case brokenProxy

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .dropMidStream: return "Connection drops twice"
        case .suspendAndRelaunch: return "App suspended, then relaunched"
        case .slowConsumer: return "Slow consumer"
        case .duplicateDelivery: return "Proxy re-sends old frames"
        case .replayGap: return "Away longer than the replay log"
        case .brokenProxy: return "Proxy drops one frame"
        }
    }

    public var expectation: String {
        switch self {
        case .dropMidStream: return "Two resumes with Last-Event-ID. One upstream call. Full transcript."
        case .suspendAndRelaunch: return "The client is torn down, its checkpoint is persisted as JSON, and a new client resumes it."
        case .slowConsumer: return "The gateway cuts the stalled connection instead of buffering for it. The client resumes from the log."
        case .duplicateDelivery: return "Already-applied frames arrive again and are dropped by sequence."
        case .replayGap: return "The frames it missed were evicted. The gateway answers 410 replayGap instead of resuming with a hole."
        case .brokenProxy: return "A frame goes missing mid-connection. The client detects the gap and refuses to build a corrupt transcript."
        }
    }
}

/// What the host app supplies. The demo app owns this, including the
/// `GatewayConfiguration` defaults each scenario starts from.
public struct LabConfiguration: Sendable {
    public var script: String
    public var tokenInterval: Duration
    public var baseGateway: GatewayConfiguration
    public var initialScenario: LabScenario
    public var autoStart: Bool

    public init(script: String, tokenInterval: Duration = .milliseconds(45),
                baseGateway: GatewayConfiguration = .init(subscriberQueueLimit: 64,
                                                          retention: .seconds(60),
                                                          abandonTimeout: .seconds(30)),
                initialScenario: LabScenario = .dropMidStream, autoStart: Bool = true) {
        self.script = script
        self.tokenInterval = tokenInterval
        self.baseGateway = baseGateway
        self.initialScenario = initialScenario
        self.autoStart = autoStart
    }
}

@MainActor
@Observable
public final class StreamLabModel {
    public struct LogLine: Identifiable, Equatable {
        public let id: Int
        public let symbol: String
        public let text: String
    }

    public enum Status: Equatable {
        case idle, streaming, suspended, finished(String), failed(String)
    }

    public private(set) var transcript = ""
    public private(set) var status: Status = .idle
    public private(set) var log: [LogLine] = []
    public private(set) var stats = GatewayStats()
    public private(set) var connections = 0
    public private(set) var duplicatesDropped = 0
    public private(set) var lastEventID: String?
    public private(set) var expectedTranscript = ""
    public var scenario: LabScenario

    public let configuration: LabConfiguration
    private var runTask: Task<Void, Never>?
    private var pollTask: Task<Void, Never>?
    private var nextLogID = 0
    private var runID = 0
    private static let logLimit = 200

    public init(configuration: LabConfiguration) {
        self.configuration = configuration
        self.scenario = configuration.initialScenario
    }

    public var transcriptMatches: Bool { !transcript.isEmpty && transcript == expectedTranscript }

    public func run() {
        runTask?.cancel()
        pollTask?.cancel()
        runID &+= 1
        let id = runID
        transcript = ""
        log = []
        stats = GatewayStats()
        connections = 0
        duplicatesDropped = 0
        lastEventID = nil
        status = .streaming
        let scenario = scenario
        let configuration = configuration
        expectedTranscript = configuration.script
        runTask = Task { [weak self] in
            await self?.execute(id, scenario, configuration)
        }
    }

    /// Only the newest run may touch published state. A cancelled run keeps
    /// executing until its next suspension point, so every write is guarded.
    private func isCurrent(_ id: Int) -> Bool { id == runID && !Task.isCancelled }

    private func gatewayConfiguration(for scenario: LabScenario) -> GatewayConfiguration {
        let base = configuration.baseGateway
        switch scenario {
        case .slowConsumer: return base.with(subscriberQueueLimit: 6)
        case .replayGap: return base.with(replayFrameCapacity: 12)
        default: return base
        }
    }

    private func faults(for scenario: LabScenario) -> InMemoryTransport.Faults {
        switch scenario {
        case .dropMidStream: return .init(dropAfterFrames: [1: 14, 2: 21])
        case .slowConsumer: return .init(stallAfterFrames: [1: .init(afterFrames: 10, pause: .milliseconds(900))])
        case .duplicateDelivery: return .init(dropAfterFrames: [1: 18], replayStaleFrames: [2: 6])
        case .brokenProxy: return .init(skipFrameAt: [1: 9])
        case .suspendAndRelaunch, .replayGap: return .init()
        }
    }

    private func execute(_ id: Int, _ scenario: LabScenario, _ configuration: LabConfiguration) async {
        let gateway = StreamGateway(configuration: gatewayConfiguration(for: scenario))
        let streamID = "demo-" + UUID().uuidString.prefix(8).lowercased()
        let upstream = ScriptedUpstream(tokens: ScriptedUpstream.tokenize(configuration.script),
                                        interval: configuration.tokenInterval)
        do {
            let outcome = try await gateway.open(streamID: streamID, upstream: upstream)
            guard isCurrent(id) else { await gateway.cancel(streamID: streamID); return }
            append("▶︎", "open \(streamID) → upstream request #1, epoch \(outcome.epoch.prefix(6))…")
        } catch {
            if isCurrent(id) { status = .failed("open rejected: \(error)") }
            return
        }
        startPolling(gateway, id: id)

        let policy = ReconnectPolicy(baseDelay: .milliseconds(120), maxDelay: .seconds(1))
        var transport = InMemoryTransport(gateway: gateway, faults: faults(for: scenario))
        var client = ResumableStreamClient(streamID: streamID, transport: transport, policy: policy)

        switch scenario {
        case .suspendAndRelaunch, .replayGap:
            // Phase 1: stream until 10 deltas, then "the OS suspends the app".
            let suspendAfter = 10
            var deltas = 0
            for await event in await client.start() {
                handle(event, id)
                if case .delta = event { deltas += 1 }
                if deltas >= suspendAfter { break }
            }
            await client.stop()
            guard isCurrent(id) else { await gateway.cancel(streamID: streamID); return }
            let saved = await client.checkpoint
            guard isCurrent(id) else { await gateway.cancel(streamID: streamID); return }
            let data = (try? JSONEncoder().encode(saved)) ?? Data()
            connections += transport.connections
            status = .suspended
            append("⏸", "suspended: checkpoint persisted (\(data.count) bytes JSON), last id \(saved.lastEventID ?? "–")")
            try? await Task.sleep(for: scenario == .replayGap ? .seconds(2.5) : .seconds(1.5))
            guard isCurrent(id) else { await gateway.cancel(streamID: streamID); return }
            let restored = (try? JSONDecoder().decode(TranscriptAssembler.self, from: data)) ?? saved
            // The relaunched process renders from its checkpoint, which may hold
            // frames applied after the 10th delta event was observed.
            transcript = restored.text
            append("⏵", "relaunched: new client restored from checkpoint")
            status = .streaming
            transport = InMemoryTransport(gateway: gateway)
            client = ResumableStreamClient(streamID: streamID, transport: transport, policy: policy,
                                           checkpoint: restored)
        default:
            break
        }

        for await event in await client.start() {
            handle(event, id)
        }
        guard isCurrent(id) else { await gateway.cancel(streamID: streamID); return }
        if case .failed = status {
            // A client that gives up tells the gateway, so the model call
            // stops instead of running until the abandonment timeout.
            let cancelled = await gateway.cancel(streamID: streamID)
            guard isCurrent(id) else { return }
            if cancelled { append("■", "cancelled the upstream call") }
        }
        let checkpoint = await client.checkpoint
        let finalStats = await gateway.stats()
        guard isCurrent(id) else { return }
        connections += transport.connections
        duplicatesDropped = checkpoint.duplicatesDropped
        lastEventID = checkpoint.lastEventID
        stats = finalStats
        pollTask?.cancel()
    }

    private func handle(_ event: ClientEvent, _ id: Int) {
        guard isCurrent(id) else { return }
        switch event {
        case .connected(let attempt, let from):
            append("⇄", from.map { "connection \(attempt): Last-Event-ID \($0)" } ?? "connection \(attempt): from the start")
        case .delta(let text):
            transcript += text
        case .reconnecting(let attempt, let delay):
            append("↻", "dropped; retry \(attempt) in \(delay.formatted(.units(allowed: [.milliseconds])))")
        case .finished(let terminal):
            switch terminal {
            case .done(let reason):
                status = .finished(reason)
                append("✓", "done (\(reason))")
            case .failed(let message):
                status = .failed(message)
                append("✕", "stream ended: \(message)")
            }
        case .failed(let failure):
            let text: String
            switch failure {
            case .rejected(let rejection):
                text = rejection.isPermanent
                    ? "HTTP \(rejection.httpStatus) \(rejection.rawValue): restart with a new stream ID"
                    : "HTTP \(rejection.httpStatus) \(rejection.rawValue)"
            case .protocolViolation(let detail): text = "protocol violation: \(detail)"
            case .retriesExhausted(let attempts): text = "gave up after \(attempts) attempts"
            case .malformedStream(let detail): text = "malformed stream: \(detail)"
            case .alreadyStarted: text = "already started"
            }
            status = .failed(text)
            append("✕", text)
        }
    }

    private func startPolling(_ gateway: StreamGateway, id: Int) {
        pollTask = Task { [weak self] in
            while !Task.isCancelled {
                let snapshot = await gateway.stats()
                guard let self, self.isCurrent(id) else { return }
                self.stats = snapshot
                try? await Task.sleep(for: .milliseconds(150))
            }
        }
    }

    private func append(_ symbol: String, _ text: String) {
        nextLogID += 1
        log.append(LogLine(id: nextLogID, symbol: symbol, text: text))
        if log.count > Self.logLimit { log.removeFirst(log.count - Self.logLimit) }
    }
}

public struct StreamLabView: View {
    @State private var model: StreamLabModel
    private let autoStart: Bool

    public init(configuration: LabConfiguration) {
        _model = State(initialValue: StreamLabModel(configuration: configuration))
        autoStart = configuration.autoStart
    }

    public var body: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    scenarioCard
                    metrics
                    transcriptCard
                    logCard
                }
                .padding(16)
            }
            .navigationTitle("Resumable Stream")
            .background(Color.secondary.opacity(0.06))
        }
        .task {
            if autoStart, model.status == .idle { model.run() }
        }
    }

    private var scenarioCard: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .center, spacing: 12) {
                Menu {
                    Picker("Scenario", selection: $model.scenario) {
                        ForEach(LabScenario.allCases) { Text($0.title).tag($0) }
                    }
                } label: {
                    HStack(spacing: 4) {
                        Text(model.scenario.title)
                            .font(.subheadline.weight(.semibold))
                            .multilineTextAlignment(.leading)
                            .fixedSize(horizontal: false, vertical: true)
                        Image(systemName: "chevron.up.chevron.down").font(.caption2)
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
                .accessibilityLabel("Scenario: \(model.scenario.title)")
                Button {
                    model.run()
                } label: {
                    Label("Run", systemImage: "play.fill")
                }
                .buttonStyle(.borderedProminent)
                .accessibilityIdentifier("run")
            }
            Text(model.scenario.expectation)
                .font(.footnote)
                .foregroundStyle(.secondary)
            statusBadge
        }
        .padding(14)
        .background(.background, in: RoundedRectangle(cornerRadius: 14))
    }

    private var statusBadge: some View {
        let (text, color): (String, Color) = {
            switch model.status {
            case .idle: return ("Idle", .gray)
            case .streaming: return ("Streaming…", .blue)
            case .suspended: return ("Suspended", .orange)
            case .finished(let reason):
                return (model.transcriptMatches ? "Complete · \(reason) · transcript matches the model output"
                                                : "Finished · \(reason)", .green)
            case .failed(let message): return (message, .red)
            }
        }()
        return Text(text)
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 10).padding(.vertical, 5)
            .foregroundStyle(color)
            .background(color.opacity(0.12), in: Capsule())
            .accessibilityIdentifier("status")
    }

    private var metrics: some View {
        LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
            tile("Upstream calls", "\(model.stats.upstreamRequests)", "bolt.horizontal")
            tile("Connections", "\(model.connections)", "point.3.connected.trianglepath.dotted")
            tile("Replayed", "\(model.stats.framesReplayed)", "arrow.counterclockwise")
            tile("Dupes dropped", "\(model.duplicatesDropped)", "square.on.square.dashed")
            tile("Slow cuts", "\(model.stats.slowConsumerCuts)", "tortoise")
            tile("Frames", "\(model.stats.framesAppended)", "list.number")
        }
    }

    private func tile(_ title: String, _ value: String, _ symbol: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Image(systemName: symbol).font(.caption).foregroundStyle(.secondary)
            Text(value).font(.title3.monospacedDigit().weight(.semibold))
            Text(title).font(.caption2).foregroundStyle(.secondary).lineLimit(1).minimumScaleFactor(0.8)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
        .background(.background, in: RoundedRectangle(cornerRadius: 12))
        .accessibilityElement(children: .combine)
    }

    private var transcriptCard: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Transcript").font(.headline)
            Text(model.transcript.isEmpty ? "Waiting for the first token…" : model.transcript)
                .font(.callout)
                .foregroundStyle(model.transcript.isEmpty ? .secondary : .primary)
                .frame(maxWidth: .infinity, alignment: .leading)
                .textSelection(.enabled)
            if let id = model.lastEventID {
                Text("Last-Event-ID \(id)").font(.caption2.monospaced()).foregroundStyle(.secondary)
            }
        }
        .padding(14)
        .background(.background, in: RoundedRectangle(cornerRadius: 14))
    }

    private var logCard: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Connection log").font(.headline)
            if model.log.isEmpty {
                Text("No events yet.").font(.caption).foregroundStyle(.secondary)
            }
            ForEach(model.log) { line in
                HStack(alignment: .firstTextBaseline, spacing: 8) {
                    Text(line.symbol).font(.caption.monospaced())
                    Text(line.text).font(.caption.monospaced()).foregroundStyle(.secondary)
                }
            }
        }
        .padding(14)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(.background, in: RoundedRectangle(cornerRadius: 14))
    }
}
#endif
