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

extension Data {
    @inlinable
    /// A safer version of `advanced(by:)`. On non-macOS Foundation implementations ``advanced(by:)`` segfaults on ``Data`` elements with size 0.
    /// - Parameter by: Number of elements to advance
    /// - Returns: A possibly different ``Data`` element.
    func safeAdvance(by: Int) -> Data {
        #if os(macOS)
            return advanced(by: by)
        #else
            if by == 0 {
                return self
            } else if by >= count {
                return Data()
            } else {
                return advanced(by: by)
            }
        #endif
    }

    /// Read a variable length integer from the current data
    /// - Returns: An integer of up to 64 bits.
    mutating func varint() throws -> Int {
        var advance = 0
        let result = try withUnsafeBytes {
            try $0.baseAddress!.withMemoryRebound(to: UInt8.self, capacity: 4) {
                var p = $0
                if p.pointee & 0x80 == 0 {
                    advance += 1
                    return Int(UInt64(p.pointee))
                }
                var value = UInt64(p.pointee & 0x7F)
                var shift = UInt64(7)
                var count = 1
                p = p.successor()
                while true {
                    if shift > 63 {
                        throw ApacheBeamError.runtimeError("Malformed Varint. Too large.")
                    }
                    count += 1
                    value |= UInt64(p.pointee & 0x7F) << shift
                    if p.pointee & 0x80 == 0 {
                        advance += count
                        return Int(value)
                    }
                    p = p.successor()
                    shift += 7
                }
            }
        }
        self = safeAdvance(by: advance)
        return result
    }

    
    /// Decodes a Beam time value, which is a Java Instant that has been encoded to sort properly
    /// - Returns: A Date value
    mutating func instant() throws -> Date {
        let millis = try next(Int64.self) &+ Int64(-9_223_372_036_854_775_808)
        return Date(millisecondsSince1970: millis)
    }

    
    /// Implements the Beam length-prefixed byte blob encoding
    /// - Returns: A ``Data`` with a size defined by the length encoding
    mutating func subdata() throws -> Data {
        let length = try varint()
        let result = subdata(in: 0 ..< length)
        self = safeAdvance(by: length)
        return result
    }
    
    /// Read a fixed length integer of the specified type. In Beam the wire encoding is always a Java-style bigendian value
    /// - Parameter _: The integer type of read
    /// - Returns: The integer value read.
    mutating func next<T: FixedWidthInteger>(_: T.Type) throws -> T {
        let size = MemoryLayout<T>.size
        let bigEndian = withUnsafeBytes {
            $0.load(as: T.self)
        }
        self = safeAdvance(by: size)
        return T(bigEndian: bigEndian)
    }

    
    /// Read a fixed length floating point value of the specified type.
    /// - Parameter _: The floating point type to read
    /// - Returns: The floating point value
    mutating func next<T: FloatingPoint>(_: T.Type) throws -> T {
        let result = withUnsafeBytes {
            $0.load(as: T.self)
        }
        self = safeAdvance(by: MemoryLayout<T>.size)
        return result
    }
}
