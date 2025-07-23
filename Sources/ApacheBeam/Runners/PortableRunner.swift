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

import GRPC
import Logging
import NIOCore
import Foundation

public enum Loopback {
    case none
    case localhost
    case at(String,String,Int)
}



public struct PortableRunner: PipelineRunner {
    let loopback: Loopback
    let log: Logging.Logger
    let host: String
    let port: Int
    

    public init(host: String = "localhost", port: Int = 8073, loopback: Loopback = .none) {
        self.loopback = loopback
        log = .init(label: "PortableRunner")
        self.host = host
        self.port = port
    }

    @discardableResult
    public func run(_ context: PipelineContext) async throws -> PipelineCompletionState {
        var proto = context.proto
        
        func worker(host: String, port: Int) throws -> WorkerServer {
            try WorkerServer(context.collections, context.pardoFns, host: host, port: port)
        }
        
        switch loopback {
            case .none: break
        case .localhost:
            let worker = try worker(host: "localhost", port: 0)
            log.info("Running in LOOPBACK mode with a worker server at \(worker.endpoint).")
            proto.components.environments[context.defaultEnvironmentId] = try .with {
                try Environment.external(worker.endpoint).populate(&$0)
            }

        case .at(let host, let exposedHost, let port):
            let worker = try worker(host: host,port: port)
            let advertisedEndpoint = ApiServiceDescriptor(host: exposedHost, port: worker.endpoint.port!)
            log.info("Running in LOOPBACK mode with a worker server at \(advertisedEndpoint)")
            proto.components.environments[context.defaultEnvironmentId] = try .with {
                try Environment.external(advertisedEndpoint).populate(&$0)
            }
        }
        
        
        log.info("\(proto)")
        log.info("Connecting to Portable Runner at \(host):\(port).")
        let group = PlatformSupport.makeEventLoopGroup(loopCount: 1)
        let channel = try GRPCChannelPool.with(target: .host(host, port: port),
                                               transportSecurity: .plaintext, eventLoopGroup: group)
        let client = Org_Apache_Beam_Model_JobManagement_V1_JobServiceAsyncClient(channel: channel)
        let prepared = try await client.prepare(.with {
            $0.pipeline = proto
        })
        let job = try await client.run(.with {
            $0.preparationID = prepared.preparationID
        })
        log.info("Submitted job \(job.jobID)")
        var done = false
        var state: PipelineCompletionState = .failed
        while !done {
            let status = try await client.getState(.with {
                $0.jobID = job.jobID
            })
            log.info("Job \(job.jobID) status: \(status.state)")
            switch status.state {
            case .done:
                state = .done
                done = true
            case .stopped:
                state = .stopped
                try await Task.sleep(for: .seconds(5))
            case .failed:
                state = .failed
                done = true
            case .cancelled:
                state = .cancelled
                done = true
            default:
                try await Task.sleep(for: .seconds(5))
            }
        }
        log.info("Job completed.")
        return state
    }
}
