// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import CompilerPluginSupport
import PackageDescription

let dependencies: [Package.Dependency] = [
    // Core Dependencies
    .package(url: "https://github.com/grpc/grpc-swift.git", from: "1.26.0"),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    .package(
        url: "https://github.com/apple/swift-argument-parser",
        from: "1.2.0"
    ),

    // Support Dependencies
    .package(
        url: "https://github.com/apple/swift-openapi-runtime",
        from: "1.8.2"
    ),
    .package(url: "https://github.com/apple/swift-http-types", from: "1.0.2"),
    .package(
        url: "https://github.com/swift-server/swift-openapi-async-http-client",
        from: "1.0.0"
    ),

    // Additional Transform Dependencies
    .package(
        url: "https://github.com/googleapis/google-auth-library-swift",
        from: "0.0.0"
    ),

    // Swift Macro Support
    .package(url: "https://github.com/apple/swift-syntax.git", from: "509.0.0"),

    // Swift Package Manager Plugins
    .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0"),
    .package(
        url: "https://github.com/nicklockwood/SwiftFormat",
        from: "0.52.3"
    ),
]

let package = Package(
    name: "ApacheBeam",
    platforms: [
        .macOS("13.0")
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "ApacheBeam",
            targets: ["ApacheBeam"]
        )
    ],
    dependencies: dependencies,
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .macro(
            name: "ApacheBeamMacros",
            dependencies: [
                .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
                .product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
            ]
        ),
        .target(
            name: "ApacheBeam",
            dependencies: [
                "ApacheBeamMacros",
                .product(name: "GRPC", package: "grpc-swift"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "OAuth2", package: "google-auth-library-swift"),
                .product(
                    name: "OpenAPIRuntime",
                    package: "swift-openapi-runtime"
                ),
                .product(
                    name: "OpenAPIAsyncHTTPClient",
                    package: "swift-openapi-async-http-client"
                ),
                .product(
                    name: "ArgumentParser",
                    package: "swift-argument-parser"
                ),
            ]
        ),
        .testTarget(
            name: "ApacheBeamTests",
            dependencies: ["ApacheBeam"]
        ),
        .testTarget(
            name: "ApacheBeamMacroTests",
            dependencies: ["ApacheBeamMacros"]
        ),
    ]
)
