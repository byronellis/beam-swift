//
//  File.swift
//  
//
//  Created by Byron Ellis on 10/19/23.
//

import Foundation
import Logging
import SwiftProtobuf
import GRPC
import NIOCore
import NIOPosix

enum Log {
    case entry(Org_Apache_Beam_Model_FnExecution_V1_LogEntry)
    case flush
}

typealias LogStream = AsyncStream<Log>


struct LogEntryHandler : LogHandler {

    public var metadata: Logging.Logger.Metadata
    public var logLevel: Logging.Logger.Level
    
    let label: String
    let stream: LogStream
    let cont: LogStream.Continuation
    let bufferSize: Int
    let maxTimeInterval: TimeInterval
    let local: Bool
    
    let localLog = Logger(label:"Log",factory:StreamLogHandler.standardError(label:))

    public init(label: String,metadata: Logger.Metadata = Logger.Metadata(),
                logLevel: Logger.Level = .info,bufferSize: Int = 50,
                maxTimeInterval: TimeInterval = 1.0,local: Bool = true) {
        self.metadata = metadata
        self.logLevel = logLevel
        self.bufferSize = bufferSize
        self.maxTimeInterval = maxTimeInterval
        self.label = label
        self.local = local
        (stream,cont) = AsyncStream.makeStream(of:Log.self)
    }

    public subscript(metadataKey key: String) -> Logging.Logger.Metadata.Value? {
        get {
            metadata[key]
        }
        set(newValue) {
            metadata[key] = newValue
        }
    }
    
    func log(level: Logger.Level, message: Logger.Message, metadata: Logger.Metadata?, source: String, file: String, function: String, line: UInt) {
        if(local) {
            localLog.log(level: level,message,metadata: metadata,source: source,file: file,function:function,line:line)
        }
        cont.yield(.entry(.with {
            $0.message = "\(message)"
            
            var fields = $0.customData.fields
            fields["label"] = Google_Protobuf_Value(stringValue: label)
            fields["source"] = Google_Protobuf_Value(stringValue: source)
            fields["line"] = Google_Protobuf_Value(integerLiteral: Int64(line))
            fields["file"] = Google_Protobuf_Value(stringValue: file)
            fields["function"] = Google_Protobuf_Value(stringValue: function)
            if let m = metadata {
                for (k,v) in m {
                    fields[k] = Google_Protobuf_Value(stringValue: "\(v)")
                }
            }
            
            switch level {
                
            case .trace:
                $0.severity = .trace
            case .debug:
                $0.severity = .debug
            case .info:
                $0.severity = .info
            case .notice:
                $0.severity = .notice
            case .warning:
                $0.severity = .warn
            case .error:
                $0.severity = .error
            case .critical:
                $0.severity = .critical
            }
            
        }))
    }
    
    
    public func attach(to endpoint: ApiServiceDescriptor) throws {
        let client = Org_Apache_Beam_Model_FnExecution_V1_BeamFnLoggingAsyncClient(channel:try GRPCChannelPool.with(endpoint: endpoint, eventLoopGroup: PlatformSupport.makeEventLoopGroup(loopCount: 1)))
        let logging = client.makeLoggingCall()
        
        //If we have a flush time interval larger than some small value send occasional flush messages
        let flushTask = Task {
            if(maxTimeInterval > 1e-4) {
                while(true) {
                    try await Task.sleep(
                        nanoseconds: UInt64(1_000 * 1_000 * 1_000 * maxTimeInterval)
                    )
                    cont.yield(.flush)
                }
            }
        }
        
        
        //Start a background task to consume this stream
        Task {
            var buffer: [Org_Apache_Beam_Model_FnExecution_V1_LogEntry] = []
            var lastTime : Date = Date.now
            
            for try await e in stream {
                var shouldFlush: Bool = false

                if case .entry(let entry) = e {
                    buffer.append(entry)
                    shouldFlush = switch entry.severity {
                    case .error,.critical:
                        true
                    default:
                        buffer.count >= bufferSize || lastTime.timeIntervalSinceNow >= maxTimeInterval
                    }
                } else  {
                    shouldFlush = true
                }

                //Try to bundle up entries, but don't wait too long. If it's an error then immediately flush the buffer
                if shouldFlush && buffer.count > 0 {
                    do {
                        try await logging.requestStream.send(.with {
                            $0.logEntries.append(contentsOf: buffer)
                        })
                        buffer.removeAll(keepingCapacity: true)
                        lastTime = Date.now
                    } catch {
                        localLog.error("Unable to send messages to remote logging endpoint at \(endpoint). Keeping \(buffer.count) messages.")
                    }
                }
            }
            //When the stream is closed stop sending flush messages
            flushTask.cancel()
        }
    }
    
}
