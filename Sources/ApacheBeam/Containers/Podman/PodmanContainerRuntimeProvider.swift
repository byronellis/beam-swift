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
import System
import Foundation
import OpenAPIRuntime
import OpenAPIAsyncHTTPClient
import Logging
import AsyncHTTPClient
import HTTPTypes
import NIOCore

public enum PodmanError : Error {
    case socketNotFound
    case badRequest
}


func socketPathResolver() -> URL? {
    let log : Logging.Logger = .init(label: "socketPathResolver")
    if let tmpDir = ProcessInfo.processInfo.environment["TMPDIR"] {
        log.info("Checking for socket in tmpdir \(tmpDir)")
        let socketUrl = URL(fileURLWithPath: tmpDir).appending(components: "podman","podman-machine-default-api.sock")
        if(FileManager.default.fileExists(atPath: socketUrl.path)) {
            return URL(fileURLWithPath: socketUrl.path)
        }
    }
    
    // Try to fall back to the Docker socket (assumes that Podman is running in Docker compatibility mode)
    if(FileManager.default.fileExists(atPath: "/var/run/docker.sock")) {
        return URL(fileURLWithPath: "/var/run/docker.sock")
    }
        
    return nil
}

extension HTTPField.Name {
    public static var host: Self { .init("Host")! }
}

// http+unix doesn't pass the host header by default (which makes sense) but this upsets Podman which is expecting a Host header to always be set. Since it
// doesn't seem to matter what this header actually is we just add one.
struct HostHeaderMiddleware : ClientMiddleware {
    private let log : Logging.Logger = .init(label: "HostHeaderMiddleware")
    
    func intercept(_ request: HTTPTypes.HTTPRequest, body: OpenAPIRuntime.HTTPBody?, baseURL: URL, operationID: String, next: @Sendable (HTTPTypes.HTTPRequest, OpenAPIRuntime.HTTPBody?, URL) async throws -> (HTTPTypes.HTTPResponse, OpenAPIRuntime.HTTPBody?)) async throws -> (HTTPTypes.HTTPResponse, OpenAPIRuntime.HTTPBody?) {
        var request = request
        request.headerFields[.host] = "podman"
        log.info("PODMAN: \(baseURL) \(String(describing: request.path))")
        log.info("\(String(describing: body))")
        let response = try await next(request, body, baseURL)
        log.info("PODMAN RESPONSE: \(response)")
        return response
    }
}

public struct PodmanContainer : ContainerProvider {
    
    let runtime: PodmanContainerRuntime
    let id: String
    let exposedPorts: [String:UInt16]

    public func start() async throws {
        try await runtime.startContainer(id: id)
    }
    
    
    public func stop() async throws {
        try await runtime.stopContainer(id: id)
    }
    
    public func logs() async throws -> AsyncThrowingStream<String,Error> {
        try await runtime.logs(for: id)
    }

    public func port(for id: ContainerPort.Identifier) -> UInt16? {
        exposedPorts[id.name]        
    }

}


public struct PodmanContainerRuntime  {
    public typealias Container = PodmanContainer
    
    private let log : Logging.Logger = .init(label: "PodmanContainerRuntime")
    let serverURL: URL
    let client: Client
    
    
    public init(socketPath: URL? = nil,path: String? = "v5.0.0") throws {
        guard let resolvedPath = socketPath ?? socketPathResolver() else {
            throw PodmanError.socketNotFound
        }
        let baseURL = URL(httpURLWithSocketPath: resolvedPath.path)!
        if let pathComponent = path {
            serverURL = baseURL.appendingPathComponent(pathComponent)
        } else {
            serverURL = baseURL
        }
        client = Client(serverURL: serverURL, transport: AsyncHTTPClientTransport(), middlewares: [HostHeaderMiddleware()])
        
    }
    
    func startContainer(id:String) async throws {
        _ = try await client.ContainerStart(Operations.ContainerStart.Input(path: .init(name: id)))
    }
    
    func stopContainer(id:String) async throws {
        _ = try await client.ContainerStop(Operations.ContainerStop.Input(path:.init(name: id)))
    }
    
    func logs(for container:String) async throws -> AsyncThrowingStream<String,Error> {
        let (stream,cont) = AsyncThrowingStream.makeStream(of:String.self)

        log.debug("LOGS starting read")
        var request = HTTPClientRequest(url: "\(serverURL)/containers/\(container)/logs?stdout=true&stderr=true&follow=true")
        request.headers.add(name: "Host", value: "podman")
        log.debug("LOGS \(request)")
        let response = try await HTTPClient.shared.execute(request, timeout: .seconds(30))
        log.debug("LOGS \(response)")
        if(response.status == .ok) {
            Task {
                do {
                    let newline = UInt8(ascii:"\n")
                    var data = Data()
                    for try await buffer in response.body {
                        var b = buffer
                        if let append = b.readData(length:buffer.readableBytes) {
                            log.debug("LOGS reader appending \(buffer.readableBytes)")
                            data.append(append)
                        }
                        var nextLine : Int? = data.firstIndex(of: newline)
                        while(nextLine != nil) {
                            let output = String(data: data.prefix(upTo: nextLine!),encoding: .utf8)
                            data = data.advanced(by: nextLine! + 1)
                            if let o = output {
                                cont.yield(o)
                            }
                            nextLine = data.firstIndex(of: newline)
                        }
                    }
                    log.debug("LOGS got to end of input")
                    if(!data.isEmpty) {
                        if let s = String(data: data, encoding: .utf8) {
                            cont.yield(s)
                        }
                    }
                    cont.finish()
                } catch {
                    cont.finish(throwing: error)
                }
            }
        } else {
            log.debug("LOGS \(response)")
            cont.finish(throwing:PodmanError.badRequest)
        }
        return stream
    }
}
    
extension PodmanContainerRuntime: ContainerRuntimeProvider {
    public func makeContainer(configuration: ContainerConfiguration) async throws -> PodmanContainer {
        
        
        var exposedPorts : [String:UInt16] = [:]
        var portBindings : [String:[Components.Schemas.PortBinding]] = [:]
        
        if let ports = configuration.exposedPorts {
            for port in ports {
                exposedPorts[port.identifier.name] = port.port
                portBindings["\(port.identifier.port)/tcp"] = [.init(HostIp: "0.0.0.0", HostPort: "\(port.port)")]
            }
        }

        var config = Components.Schemas.CreateContainerConfig()
        config.Name = configuration.id
        config.Image = configuration.image
        if !portBindings.isEmpty {
            var hostConfig = Components.Schemas.HostConfig()
            hostConfig.PortBindings = Components.Schemas.PortMap(additionalProperties: portBindings)
            config.HostConfig = hostConfig
        }
        
        if let args = configuration.args {
            config.Cmd = args
        }
        
        
        log.info("Attempting to create a new Podman container")
        log.info("\(config)")
        let container = try (try (try await client.ContainerCreate(.init(body: .json(config)))).created).body.json
        log.info("Container creation successful \(container.Id). Initializing the container")
        _ = try await client.ContainerInitLibpod(Operations.ContainerInitLibpod.Input(path: .init(name: container.Id)))
        log.info("Container initialized.")


        return PodmanContainer(runtime: self, id: container.Id, exposedPorts: exposedPorts)
    }

}
