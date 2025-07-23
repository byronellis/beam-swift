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

/// Encapsulates a container that runs the Prism Runner for use in integration testing.
public struct PrismContainer : TestContainerProvider {
    
    
    
    
    private let log = Logging.Logger(label: "PrismContainer")
    private let baseImage: String
    
    /// Initializes a new container using the specified container runtime. This has the side effect of creating a container in the runtime.
    /// - Parameters:
    ///   - runtime: A container runtime such as Podman, Docker or the Apple native container runtime
    ///   - baseImage: A base image capable of compiling go.
    public init(baseImage: String = "golang:latest") {
        self.baseImage = baseImage
    }
            
    public func makeContainer(runtime: any ApacheBeam.ContainerRuntimeProvider) async throws -> any ApacheBeam.ContainerProvider {
        let runnerPort : ContainerPort = try .namedPort(ContainerPort.Identifier.runner.name)
        return try await runtime.makeContainer(
            configuration: ContainerConfiguration(
                image: baseImage,
                exposedPorts: [
                    runnerPort,
                    try .dynamicPort(for: .http(8074)),  // Web UI
                ],
                args: [
                    "/usr/local/go/bin/go", "run",
                    "github.com/apache/beam/sdks/v2/go/cmd/prism@latest",
                    "--log_level=debug",
                    "--job_port=\(runnerPort.port)"
                ]
            )
        )
    }
    
    
    public func started(_ container: any ApacheBeam.ContainerProvider,timeout: TimeInterval) async throws -> Bool {
        let finishAt = Date.now.addingTimeInterval(timeout)
        while finishAt > Date.now {
            let logs = try await container.logs()
            for try await line in logs {
                log.info("PRISM: \(line)")
                if line.contains("Serving JobManagement") {
                    log.info("Finished starting Prism container")
                    return true
                }
            }
        }
        throw PrismError.webNotAvailable
    }
    
    public func provide<K>(_ key: K.Type, from container: any ApacheBeam.ContainerProvider) async throws -> K? {
        guard key is PortableRunner.Type else { return nil }
        
        guard let runnerPort = container.port(for: .runner) else {
            throw PrismError.noRunnerPort
        }
        return PortableRunner(
            port: Int(runnerPort),
            loopback: .at("0.0.0.0", "host.containers.internal", 0)
        ) as? K
    }
    

    

}
