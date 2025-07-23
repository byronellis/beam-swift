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
import Foundation
import Testing

public struct BasicTransformTests {
  /*
    let containers: TestContainers
        
    init() async throws {
        containers = try await TestContainers(runtime: PodmanContainerRuntime(),PrismContainer())
    }

    @Test("Multiple GroupByKey")
    func testMultipleGroupByKey() async throws {
        let expectedResult: [String: Int] = [
            "a": 3,"b": 2,"c": 2,"d": 2,"e": 2,"f": 1,"g": 1,
            "h": 1,"i": 1,"j": 1,"k": 1,"l": 1,"m": 2,"n": 2,
            "o": 2,"p": 2,"q": 2,"r": 2,"s": 2
        ]
        let result = try await Pipeline { pipeline in
            pipeline.create([
                "A a B b C c D d E e",
                "A f G h I j K l M m",
                "N n O o P p Q q R r S s",
            ]).flatMap({ $0.components(separatedBy: .whitespaces) })
                .groupBy({ ($0, 1) })
                .sum()
                .groupBy({ ($0.key.lowercased(), $0.values.reduce(0,+))})
                .sum()
                .pstream { input in
                    for try await (kv,_,_) in input {
                        #expect(expectedResult[kv.key] == kv.value)
                    }
                }
        }.run(containers.find(PortableRunner.self)!)
        
        #expect(result == .done)
    }
*/
}
