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

import Metrics
import CoreMetrics

typealias MetricValueStream = AsyncStream<MetricValue>

class MetricValueCounter : CounterHandler {
    let label: String
    let stream: MetricValueStream.Continuation
    
    init(_ label: String,_ stream: MetricValueStream.Continuation) {
        self.label = label
        self.stream = stream
    }

    func increment(by: Int64) {
        stream.yield(.incrementBy(label, by))
    }
    
    func reset() {
        stream.yield(.reset(label))
    }
    

}

class MetricValueRecorder : RecorderHandler {
    let label: String
    let stream: MetricValueStream.Continuation

    init(_ label: String,_ stream: MetricValueStream.Continuation) {
        self.label = label
        self.stream = stream
    }

    func record(_ value: Int64) {
        stream.yield(.recordInt(label, value))
    }
    
    func record(_ value: Double) {
        stream.yield(.recordFloat(label, value))
    }
    

}

class MetricValueTimer : TimerHandler {
    
    let label: String
    let stream: MetricValueStream.Continuation
    init(_ label: String,_ stream: MetricValueStream.Continuation) {
        self.label = label
        self.stream = stream
    }
    
    func recordNanoseconds(_ duration: Int64) {
        stream.yield(.nanos(label, duration))
    }


}


/// Background actor that handles reporting metrics to the backend. Note that metrics values are not reported until an attach.
struct MetricValueHandler : MetricsFactory {
    
    
    let stream: MetricValueStream
    let cont: MetricValueStream.Continuation
    
    init() {
        (stream,cont) = MetricValueStream.makeStream()
    }
    
    
    func makeCounter(label: String, dimensions: [(String, String)]) -> CounterHandler {
        MetricValueCounter(label,cont)
    }
    
    func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> RecorderHandler {
        MetricValueRecorder(label,cont)
    }
    
    func makeTimer(label: String, dimensions: [(String, String)]) -> TimerHandler {
        MetricValueTimer(label,cont)
    }
    
    func destroyCounter(_ handler: CounterHandler) {
    }
    
    func destroyRecorder(_ handler: RecorderHandler) {
    }
    
    func destroyTimer(_ handler: TimerHandler) {
    }

}
