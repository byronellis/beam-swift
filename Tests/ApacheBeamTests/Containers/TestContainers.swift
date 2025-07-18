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
import ApacheBeam
import Logging

public protocol TestContainerProvider {    
    func makeContainer(runtime: any ContainerRuntimeProvider) async throws -> any ContainerProvider
    func started(_ container: any ContainerProvider,timeout: TimeInterval) async throws -> Bool
    func provide<K>(_ key: K.Type, from: any ContainerProvider) async throws -> K?
}


public enum TestContainerError : Error {
    case noRunnerContainerFound
    case notFound(Any.Type)
}

final class TestContainers {
    let containers: [(TestContainerProvider,ContainerProvider)]
    
    init<R : ContainerRuntimeProvider,each C : TestContainerProvider>(runtime: R, _ providers: repeat each C) async throws {

        // Register all the test container providers and make sure there is only a single container of a given
        // type. Also determine whether or not there is a runner provider
        var containers: [(TestContainerProvider,ContainerProvider)] = []

        for provider in repeat each providers {
            let container = try await provider.makeContainer(runtime: runtime)
            containers.append((provider,container))
        }
        self.containers = containers
        
        // Start all of the containers concurrently
        _ = await withThrowingTaskGroup(returning:[Bool].self) { taskGroup in
            var results : [Bool] = []
            for (provider,container) in containers {
                taskGroup.addTask {
                    try await container.start()
                    results.append(try await provider.started(container, timeout: 600))
                }
            }
            return results
        }
        
        
    }

    deinit {
        for (provider,container) in containers {
            Task.detached {
                let logger = Logging.Logger(label:"\(String(describing: provider))")
                logger.info("Shutting down container")
                do {
                    try await container.stop()
                } catch {
                    logger.error("\(error)")
                }
            }
        }
    }
    
    func find<T>(_ type: T.Type) async throws -> T? {
        for (provider,container) in containers {
            if let t = try await provider.provide(type, from:container) {
                return t
            }
        }
        return nil
    }
    
    
}
