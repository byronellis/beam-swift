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

typealias InstructionResponder = AsyncStream<Org_Apache_Beam_Model_FnExecution_V1_InstructionResponse>.Continuation



actor Worker {
    private let id: String
    private let collections: [String: AnyPCollection]
    private let fns: [String: SerializableFn]
    private let control: ApiServiceDescriptor
    private let remoteLog: ApiServiceDescriptor

    private let log: Logging.Logger
    private let registry: MetricsRegistry

    public init(id: String, control: ApiServiceDescriptor, log: ApiServiceDescriptor, collections: [String: AnyPCollection], functions: [String: SerializableFn]) {
        self.id = id
        self.collections = collections
        fns = functions
        self.control = control
        remoteLog = log

        self.log = Logging.Logger(label: "Worker(\(id))")
        self.registry = MetricsRegistry()
    }

    public func start() throws {
        let group = PlatformSupport.makeEventLoopGroup(loopCount: 1)
        let client = try Org_Apache_Beam_Model_FnExecution_V1_BeamFnControlAsyncClient(channel: GRPCChannelPool.with(endpoint: control, eventLoopGroup: group),defaultCallOptions: CallOptions(customMetadata: ["worker_id": id]))
        let (responses, responder) = AsyncStream.makeStream(of: Org_Apache_Beam_Model_FnExecution_V1_InstructionResponse.self)
        let control = client.makeControlCall()

        // Start the response task. This will continue until a yield call is sent from responder
        Task {
            for await r in responses {
//                log.info("Sending response \(r)")
                try await control.requestStream.send(r)
            }
        }
        
        // Start the actual work task
        Task {
            log.info("Waiting for control plane instructions.")
            var processors: [String: BundleProcessor] = [:]
            var metrics: [String: MetricAccumulator] = [:]

            func processor(for bundle: String) async throws -> BundleProcessor {
                if let processor = processors[bundle] {
                    return processor
                }
                let descriptor = try await client.getProcessBundleDescriptor(.with { $0.processBundleDescriptorID = bundle })
                let processor = try await BundleProcessor(id: id, descriptor: descriptor, collections: collections, fns: fns, registry: registry)
                processors[bundle] = processor
                return processor
            }

            // This looks a little bit reversed from the usual because response don't need an initiating call
            for try await instruction in control.responseStream {
//                log.info("\(instruction)")
                switch instruction.request {
                case let .processBundle(pbr):
                    do {
                        let p = try await processor(for: pbr.processBundleDescriptorID)
                        let accumulator = MetricAccumulator(instruction: instruction.instructionID, registry: registry)
                        await accumulator.start()
                        metrics[instruction.instructionID] = accumulator
                        Task {
                            await p.process(instruction: instruction.instructionID,accumulator: accumulator, responder: responder)
                        }
                    } catch {
                        log.error("Unable to process bundle \(pbr.processBundleDescriptorID): \(error)")
                    }
                case let .processBundleProgress(pbpr):
//                    log.info("Requesting bundle progress of \(pbpr.instructionID)")
                    if let accumulator = metrics[pbpr.instructionID] {
                        await accumulator.reporter.yield(.report({ metricInfo, metricData in
                            responder.yield(.with {
                                $0.instructionID = instruction.instructionID
                                $0.processBundleProgress = .with {
                                    $0.monitoringData.merge(metricData, uniquingKeysWith: { $1 })
                                    $0.monitoringInfos.append(contentsOf: metricInfo)
                                }
                            })
                        }))
                    }
                case let .processBundleSplit(pbsr):
                    log.info("Requesting bundle split for \(pbsr.instructionID)")
                    responder.yield(.with {
                        $0.instructionID = instruction.instructionID
                        $0.processBundleSplit = .with { _ in }
                    })
                case let .finalizeBundle(fbr):
                    log.info("Finializing bundle \(fbr.instructionID)")
                    metrics.removeValue(forKey: fbr.instructionID)
                    responder.yield(.with {
                        $0.instructionID = fbr.instructionID
                        $0.finalizeBundle = .with { _ in }
                    })
                case let .monitoringInfos(mimr):
                    log.info("Requesting monitoring information")
                    var tmp: [String:Org_Apache_Beam_Model_Pipeline_V1_MonitoringInfo] = [:]
                    for id in mimr.monitoringInfoID {
                        tmp[id] = await registry.monitoringInfo(id)
                    }
                    log.info("\(tmp)")
                    responder.yield(.with {
                        $0.instructionID = instruction.instructionID
                        $0.monitoringInfos = .with {
                            $0.monitoringInfo.merge(tmp, uniquingKeysWith: { $1 })
                        }
                    })

                default:
                    log.warning("Ignoring instruction \(instruction.instructionID). Not yet implemented.")
                    log.warning("\(instruction)")
                    responder.yield(.with {
                        $0.instructionID = instruction.instructionID
                    })
                }
            }
            log.info("Control plane connection has closed.")
        }
    }
}
