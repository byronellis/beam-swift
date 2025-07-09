//
//  File.swift
//  
//
//  Created by Byron Ellis on 9/2/24.
//

import Foundation
import Logging

typealias MetricDataFn = ([Org_Apache_Beam_Model_Pipeline_V1_MonitoringInfo],Dictionary<String,Data>) -> Void

enum MetricCommand {
    case update(String,ReportableMetric)
    case report(MetricDataFn)
    case finish(MetricDataFn)
}

typealias MetricStream = AsyncStream<MetricCommand>
typealias MetricStreamReporter = MetricStream.Continuation

public actor MetricAccumulator {
    let log: Logging.Logger

    var registry: MetricsRegistry
    var values: [String:ReportableMetric] = [:]
    var stream:MetricStream
    var _reporter:MetricStreamReporter
    
    var reporter: MetricStreamReporter { get {
        return _reporter
    }}
    
    public init(instruction: String, registry: MetricsRegistry) {
        self.registry = registry
        (stream,_reporter) = AsyncStream.makeStream(of:MetricCommand.self)
        self.log = Logging.Logger(label: "Accumulator(\(instruction))")
    }
    
    func reportMetrics(_ to:MetricDataFn) async {
        var info: [Org_Apache_Beam_Model_Pipeline_V1_MonitoringInfo] = []
        var outputs: [String:Data] = [:]
        for (mId,value) in values {
            do {
                let payload = try value.encode()
                info.append(await registry.monitoringInfo(mId, payload: payload))
                outputs[mId] = payload
            } catch {
                log.error("Unable to write metric \(mId): \(error)")
            }
        }
        to(info,outputs)
    }
    
    public func start() {
        Task {
            log.info("Starting metric command processing.")
            for await cmd in stream {
                switch cmd {
                case let .update(mId, value):
                    if let o = values[mId] {
                        values.updateValue(o.merge(other: value), forKey: mId)
                    } else {
                        values[mId] = value
                    }
                case let .report(to):
                    await reportMetrics(to)
                case let .finish(to):
                    await reportMetrics(to)
                    values.removeAll()
                    break
                }
            }
            log.info("Shutting down metric command processing.")
        }
    }
    
}
