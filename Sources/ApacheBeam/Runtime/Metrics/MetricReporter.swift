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


public typealias Counter = (Int) -> Void
public typealias Distribution = (Int) -> Void

public struct MetricReporter {
    
    let registry: MetricsRegistry
    let reporter: MetricStreamReporter
    let transform : String
    let pcollection: String
    
    init(registry: MetricsRegistry,reporter: MetricStreamReporter, transform: String = "",pcollection: String = "") {
        self.registry = registry
        self.reporter = reporter
        self.transform = transform
        self.pcollection = pcollection
    }
    
    init(accumulator: MetricAccumulator, transform: String = "", pcollection: String = "") async {
        self.init(registry: await accumulator.registry, reporter: await accumulator.reporter,transform: transform, pcollection: pcollection)
    }
    
    public func register(_ name: String,namespace:String = "",transform:String = "", pcollection:String = "",initialValue: ReportableMetric) async -> String {
        return await registry.register(name,urn:initialValue.urn,type:initialValue.type,namespace:namespace,transform:transform,pcollection:pcollection)
    }
    
    public func counter(name: String, namespace: String = "", pcollection: String? = nil) async -> Counter {
        let value:ReportableMetric = .counter(0)
        let metricId = await register(name,namespace: namespace,transform: transform, pcollection: pcollection ?? self.pcollection, initialValue: value)
        reporter.yield(.update(metricId, value))
        return { update in
            reporter.yield(.update(metricId, .counter(update)))
        }
    }
    public func elementCount(name:String, namespace: String = "",transform:String? = nil,pcollection: String? = nil) async -> Counter {
        let value: ReportableMetric = .counter(0)
        let metricId = await registry.register(name,urn:"beam:metric:element_count:v1",type:"beam:metrics:sum_int64:v1",
                                               transform: transform ?? self.transform,pcollection: pcollection ?? self.pcollection)
        reporter.yield(.update(metricId, value))
        return { update in
            reporter.yield(.update(metricId, .counter(update)))
        }
    }
    
    public func distribution(name : String, namespace: String = "", pcollection: String? = nil) async -> Distribution {
        let value:ReportableMetric = .distribution(0,0,Int.max,Int.min)
        let metricId = await register(name,namespace: namespace,transform: transform, pcollection: pcollection ?? self.pcollection, initialValue: value)
        reporter.yield(.update(metricId, value))
        return { update in
            reporter.yield(.update(metricId, .distribution(1,update,update,update)))
        }
    }
}


