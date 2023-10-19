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

/// Defines a protocol for transforms that know how to read from external File-like systems given an input
/// collection of paths.
public protocol FileIOSource {
    /// A static function definition that takes `(path,matched_filename)` pairs output by ``readFiles(matching:)`` or some other source
    /// and reads them as a single data output.
    ///
    /// - Parameter matching: An input ``PCollection`` of pairs representing a `(path,matched_filename)`
    /// where `path` and `matched_filename` are dependent on the specific source.
    /// - Returns: A ``PCollection`` of binary `Data` elements. Note that the original path and filename is not carried with the data.
    static func readFiles(matching: PCollection<KV<String, String>>) -> PCollection<Data>
   
    /// Static function definition for reading a list of files matching some pattern at a given path. The definition of "path" largely depends on the
    /// source type. For example, for remote object stores that might be a bucket or a bucket prefix while for a local filesystem it would be a
    /// standard file path. It is best to check the specific transform's documentation to verify
    ///
    ///
    /// - Parameter matching: A pair representing the path to check and pattern to check with.
    ///
    /// - Returns: A list of pairs of the form `(path,matched_filename)`
    static func listFiles(matching: PCollection<KV<String, String>>) -> PCollection<KV<String, String>>
}

public extension PCollection<KV<String, String>> {
    /// A transform for reading files from a concrete ``FileIOSource``.
    /// - Parameter `_`: The type of ``FileIOSource`` to use. For example ``LocalStorage.self``
    ///
    /// - Returns: The data elements from the ``readFiles(matching:)`` implementation for this ``FileIOSource``
    func readFiles<Source: FileIOSource>(in _: Source.Type) -> PCollection<Data> {
        Source.readFiles(matching: self)
    }

    /// Takes a KV pair of (bucket,prefix) and returns a list of (bucket,filename)
    func listFiles<Source: FileIOSource>(in _: Source.Type) -> PCollection<KV<String, String>> {
        Source.listFiles(matching: self)
    }
}
