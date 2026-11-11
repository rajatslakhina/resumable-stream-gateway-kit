// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "StreamGateway",
    // Only platforms that CI actually builds are declared.
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "StreamGateway", targets: ["StreamGateway"]),
        .library(name: "StreamGatewayUI", targets: ["StreamGatewayUI"]),
    ],
    targets: [
        .target(name: "StreamGateway"),
        .target(name: "StreamGatewayUI", dependencies: ["StreamGateway"]),
        .testTarget(name: "StreamGatewayTests", dependencies: ["StreamGateway"]),
    ]
)
