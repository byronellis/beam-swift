/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 *  License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an  AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import ApacheBeam
import AsyncHTTPClient
import Foundation
import Logging

public enum PrismError: Error {
    case noRunnerPort
    case webNotAvailable
}

public struct PrismContainer {
    private let log = Logging.Logger(label: "PrismContainer")

    let container: any ContainerProvider

    init(
        runtime: any ContainerRuntimeProvider,
        baseImage: String = "golang:latest"
    ) async throws {
        self.container = try await runtime.makeContainer(
            configuration: ContainerConfiguration(
                image: baseImage,
                exposedPorts: [
                    try .dynamicPort(for: .runner),
                    try .dynamicPort(for: .http(8074)),  // Web UI
                ],
                args: [
                    "/usr/local/go/bin/go", "run",
                    "github.com/apache/beam/sdks/v2/go/cmd/prism@latest",
                    "--log_level=debug"
                ]
            )
        )
    }

    public func start(
        timeout: TimeInterval = 600,
        interval: TimeInterval = 10
    ) async throws {
        try await container.start()
        let finishAt = Date.now.addingTimeInterval(timeout)
        while finishAt > Date.now {
            let logs = try await container.logs()
            for try await line in logs {
                log.info("PRISM: \(line)")
                if line.contains("http://localhost:8074") {
                    log.info("Finished starting Prism container")
                    return
                }
            }
        }
        throw PrismError.webNotAvailable
    }

    public func stop() async throws {
        try await container.stop()
    }

    public func runner() throws -> PortableRunner {
        guard let runnerPort = container.port(for: .runner) else {
            throw PrismError.noRunnerPort
        }
        return PortableRunner(
            port: Int(runnerPort),
            loopback: .at("0.0.0.0", "host.containers.internal", 0),
            controlEndpoint: ApiServiceDescriptor(host: "localhost", port: Int(runnerPort)),
            loggingEndpoint: ApiServiceDescriptor(host: "localhost", port: Int(runnerPort)),
            dataplaneEndpoint: ApiServiceDescriptor(host: "localhost", port: Int(runnerPort))
        )
    }
}
