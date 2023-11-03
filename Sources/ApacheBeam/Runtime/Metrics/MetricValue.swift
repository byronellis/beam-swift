//
//  File.swift
//  
//
//  Created by Byron Ellis on 11/2/23.
//

import Foundation

enum MetricValue {
    case incrementBy(String,Int64)
    case reset(String)
    case setInt(String,Int64)
    case setFloat(String,Double)
    case incrementInt(String,Double)
    case decrementInt(String,Double)
    case recordInt(String,Int64)
    case recordFloat(String,Double)
    case nanos(String,Int64)
}



