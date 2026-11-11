#if canImport(Darwin)
import Foundation

/// The production `StreamTransport` for Apple platforms: one
/// `GET {baseURL}/streams/{id}` per connection, with `Last-Event-ID`.
///
/// Rejections: the host answers a refused resume with the status from
/// `StreamRejection.httpStatus` and the case name in an
/// `X-Stream-Rejection` header. Anything else that is not a 200 is treated as
/// transient, so the client backs off and retries.
///
/// Idle detection is the session's job: `timeoutIntervalForRequest` is an
/// idle timeout between packets, and the host sends `: keepalive` comments
/// more often than that.
///
/// Not exercised against a live server in this repository (there is no HTTP
/// host here); the request and status mapping are unit-tested via `request(…)`
/// and `rejection(…)`.
public struct URLSessionStreamTransport: StreamTransport {
    public struct UnexpectedStatus: Error, Equatable, Sendable { public let status: Int }

    public let baseURL: URL
    public let session: URLSession
    /// Bytes are forwarded at every line end, or when this many are buffered.
    public let chunkSize: Int

    public init(baseURL: URL, session: URLSession = .shared, chunkSize: Int = 4096) {
        self.baseURL = baseURL
        self.session = session
        self.chunkSize = min(max(1, chunkSize), 1 << 20)
    }

    public func request(streamID: String, lastEventID: String?) -> URLRequest {
        var request = URLRequest(url: baseURL.appendingPathComponent("streams").appendingPathComponent(streamID))
        request.httpMethod = "GET"
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        request.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
        if let lastEventID, !lastEventID.isEmpty {
            request.setValue(lastEventID, forHTTPHeaderField: "Last-Event-ID")
        }
        return request
    }

    /// Maps a non-200 response to a rejection, or nil when it is transient.
    public static func rejection(status: Int, header: String?) -> StreamRejection? {
        if let header, let named = StreamRejection(rawValue: header), named.httpStatus == status { return named }
        switch status {
        case 410: return .unknownStream
        case 409: return .cursorMismatch
        case 400: return .malformedCursor
        default: return nil
        }
    }

    public func connect(streamID: String, lastEventID: String?) async throws -> AsyncThrowingStream<[UInt8], any Error> {
        let (bytes, response) = try await session.bytes(for: request(streamID: streamID, lastEventID: lastEventID))
        let status = (response as? HTTPURLResponse)?.statusCode ?? 0
        guard status == 200 else {
            bytes.task.cancel()
            let header = (response as? HTTPURLResponse)?.value(forHTTPHeaderField: "X-Stream-Rejection")
            if let rejection = Self.rejection(status: status, header: header) { throw rejection }
            throw UnexpectedStatus(status: status)
        }
        let chunkSize = chunkSize
        let dataTask = bytes.task
        return AsyncThrowingStream { continuation in
            let task = Task {
                var buffer: [UInt8] = []
                buffer.reserveCapacity(chunkSize)
                do {
                    for try await byte in bytes {
                        buffer.append(byte)
                        if byte == 0x0A || buffer.count >= chunkSize {
                            continuation.yield(buffer)
                            buffer.removeAll(keepingCapacity: true)
                        }
                    }
                    if !buffer.isEmpty { continuation.yield(buffer) }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in
                task.cancel()
                dataTask.cancel()
            }
        }
    }
}
#endif
