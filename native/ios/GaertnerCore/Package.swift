// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "GaertnerCore",
    platforms: [.iOS(.v17), .macOS(.v13)],
    products: [.library(name: "GaertnerCore", targets: ["GaertnerCore"])],
    targets: [
        .target(name: "GaertnerCore"),
        .testTarget(name: "GaertnerCoreTests", dependencies: ["GaertnerCore"])
    ]
)
