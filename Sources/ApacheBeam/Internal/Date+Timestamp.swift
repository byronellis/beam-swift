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

extension Date {
    /// Convenience property to extract Java-style milliseconds since the UNIX epoch
    var millisecondsSince1970: Int64 {
        // Multiply doubles by 1000 here gives us the wrong rounging. Make sure
        // we always do the right thing.
        let (intPart,fracPart) = modf(timeIntervalSince1970)
        return Int64(intPart)*1000 + Int64((fracPart*1000.0).rounded(.towardZero))
    }
    
    /// Create a ``Date`` from UNIX epoch milliseconds
    /// - Parameter millisecondsSince1970: Milliseconds to convert
    init(millisecondsSince1970: Int64) {
        self = Date(timeIntervalSince1970: Double(millisecondsSince1970) / 1000.0)
    }
    
    public static var min = Date(millisecondsSince1970: -9223372036854775)
    public static var max = Date(millisecondsSince1970: 9223372036854775)
    public static var endOfGlobalWindow = Date(millisecondsSince1970: 9223371950454775)
    
    
}
