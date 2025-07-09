import Foundation

public enum ReportableMetric {
    case counter(Int)
    case distribution(Int,Int,Int,Int)
    
    public func merge(other:ReportableMetric) -> ReportableMetric {
        switch (self,other) {
        case let (.counter(val),.counter(other_val)):
            return .counter(val+other_val)
        case let (.counter(val),.distribution(count, sum, min, max)):
            return .counter(val + sum)
        case let (.distribution(count,sum,min,max),.distribution(other_count, other_sum, other_min, other_max)):
            return .distribution(count+other_count, sum+other_sum, min < other_min ? min : other_min, max > other_max ? max : other_max)
        case let (.distribution(count,sum,min,max),.counter(val)):
            return .distribution(count+1, sum+val, min < val ? min : val, max > val ? max : val)
        }
    }
    public var type: String { get {
        switch self {
        case .counter(_):
            return "beam:metrics:sum_int64:v1"
        case .distribution(_, _, _, _):
            return "beam:metrics:distribution_int64:v1"
        }
    }}

    public var urn: String { get {
        switch self {
        case .counter(_):
            return "beam:metric:user:sum_int64:v1"
        case .distribution(_, _, _, _):
            return "beam:metric:user:distribution_int64:v1"
        }
    }}
    
    public func update(val:Int) -> ReportableMetric {
        switch self {
        case let .counter(current):
            return .counter(current+val)
        case let .distribution(count, sum, min, max):
            return .distribution(count+1, sum+val, min < val ? min : val, max > val ? max : val)
        }
    }
    
    
    public func encode() throws -> Data {
        switch self {
        case let .counter(c):
            return try Coder.varint.encode(c)
        case let .distribution(count, sum, min, max):
            return try Coder.iterable(Coder.varint).encode([count,sum,min,max])
        }
    }
}

