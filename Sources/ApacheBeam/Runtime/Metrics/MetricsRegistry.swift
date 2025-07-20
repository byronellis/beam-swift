//
//  File.swift
//  
//
//  Created by Byron Ellis on 9/3/24.
//

import Foundation

public struct MetricDefinition {
    let id: String
    let name: String
    let labels: [String:String]
    let urn: String
    let type: String
}


public actor MetricsRegistry {
    
    var definitions: [String:MetricDefinition] = [:]
    var names: [String:String] = [:]
    var id: Int = 1

    public func register(_ name: String,urn:String,type:String,namespace:String = "",transform:String = "",pcollection:String = "") -> String {
        let fullName = "\(transform)-\(namespace)-\(name)"
        if let metricId = names[fullName] {
            return metricId
        } else {
            let metricId = "m\(id)"
            id = id + 1
            var l:[String:String] = [:]
            l["NAME"] = name
            l["NAMESPACE"] = namespace
            
            if transform != "" {
                l["PTRANSFORM"] = transform
            }
            if pcollection != "" {
                l["PCOLLECTION"] = pcollection
            }
            definitions[metricId] = MetricDefinition(id: metricId, name: fullName, labels: l, urn: urn, type: type)
            return metricId
        }
    }
    
    

}

extension MetricsRegistry {
    func monitoringInfo(_ id: String,payload: Data? = nil) -> Org_Apache_Beam_Model_Pipeline_V1_MonitoringInfo {
        return .with {
            if let def = definitions[id] {
                $0.type = def.type
                $0.urn = def.urn
                $0.labels.merge(def.labels, uniquingKeysWith: { $1 })
            }
            if let p = payload {
                $0.payload = p
            }
        }
    }
}
