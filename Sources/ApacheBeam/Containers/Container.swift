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

import Foundation

public enum ContainerPortError: Error {
    case unableFindOpenPort(ContainerPort.Identifier, ClosedRange<UInt16>)
    case unableToBindPort(ContainerPort.Identifier, UInt16)
}

func checkOpenPort(_ port: UInt16) -> Bool {
    let sfd = socket(AF_INET, SOCK_STREAM, 0)
    guard sfd != -1 else {
        return false
    }

    var addr = sockaddr_in()
    let size = MemoryLayout<sockaddr_in>.size
    addr.sin_len = __uint8_t(size)
    addr.sin_family = sa_family_t(AF_INET)
    addr.sin_port =
        Int(OSHostByteOrder()) == OSLittleEndian ? _OSSwapInt16(port) : port
    addr.sin_addr = in_addr(s_addr: inet_addr("0.0.0.0"))
    addr.sin_zero = (0, 0, 0, 0, 0, 0, 0, 0)
    var bind_addr = sockaddr()
    memcpy(&bind_addr, &addr, Int(size))
    if Darwin.bind(sfd, &bind_addr, socklen_t(size)) == -1 {
        return false
    }
    let open = listen(sfd, SOMAXCONN) != -1
    Darwin.close(sfd)
    return open
}

public struct ContainerPort {
    public struct Identifier {
        public let name: String
        public let port: UInt16
    }

    public let identifier: Identifier
    public let port: UInt16

    public static func fixedPort(for port: Identifier, at exposedPort: UInt16)
        throws -> ContainerPort
    {
        guard checkOpenPort(exposedPort) else {
            throw ContainerPortError.unableToBindPort(port, exposedPort)
        }
        return .init(identifier: port, port: exposedPort)
    }
    
    public static func namedPort(_ name: String, inRange: ClosedRange<UInt16> = 16384...65535,maxAttempts: Int = 100) throws -> ContainerPort {
        var attempt = 0
        while(attempt < maxAttempts) {
            let proposedPort = inRange.randomElement()!
            if checkOpenPort(proposedPort) {
                return .init(identifier: .init(name: name, port: proposedPort), port: proposedPort)
            }
            attempt += 1
        }
        throw ContainerPortError.unableFindOpenPort(.init(name: name, port: 0), inRange)
    }

    public static func dynamicPort(
        for port: Identifier,
        inRange: ClosedRange<UInt16> = 16384...65535,
        maxAttempts: Int = 100
    ) throws -> ContainerPort {
        var attempt = 0
        while(attempt < maxAttempts) {
            let proposedPort = inRange.randomElement()!
            if checkOpenPort(proposedPort) {
                return .init(identifier: port, port: proposedPort)
            }
            attempt += 1
        }
        
        throw ContainerPortError.unableFindOpenPort(port, inRange)
    }
    
    public static func serverPort(for port: Identifier,inRange: ClosedRange<UInt16> = 16384...65535,maxAttempts: Int = 100) throws -> ContainerPort {
        let basePort = try dynamicPort(for: port,inRange: inRange,maxAttempts: maxAttempts)
        return ContainerPort(identifier: .init(name: basePort.identifier.name, port: basePort.port), port: basePort.port)
    }
}

extension ContainerPort.Identifier {
    public static var http: Self = .init(name: "http", port: 80)
    public static func http(_ port: Int) -> Self {
        .init(name: "http", port: UInt16(port))
    }

    public static var https: Self = .init(name: "https", port: 443)
    public static func https(_ port: Int) -> Self {
        .init(name: "https", port: UInt16(port))
    }

    public static var runner: Self = .init(name: "runner", port: 8073)

    public static func genericPort(_ port: Int) -> Self {
        .init(name: "\(port)", port: UInt16(port))
    }
    
    public static func named(_ name: String,port: Int = 0) -> Self {
        .init(name: name, port: UInt16(port))
    }

}

public struct ContainerConfiguration {
    let id: String?
    let image: String
    let exposedPorts: [ContainerPort]?
    let args: [String]?

    public init(id: String? = nil, image: String,exposedPorts: [ContainerPort]? = nil,args: [String]? = nil) {
        self.id = id
        self.image = image
        self.exposedPorts = exposedPorts
        self.args = args
    }
}

public protocol ContainerProvider {
    
    func start() async throws
    func stop() async throws
    func logs() async throws -> AsyncThrowingStream<String,Error>

    func port(for: ContainerPort.Identifier) -> UInt16?
}

public protocol ContainerRuntimeProvider {
    associatedtype Container: ContainerProvider

    func makeContainer(configuration: ContainerConfiguration) async throws
        -> Container
}

public struct Container {
    let wrapping: any ContainerProvider

}
