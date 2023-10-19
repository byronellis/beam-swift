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

// Utility Transforms
public extension PCollection {
    /// For each item from the input ``PCollection`` emit all of the items in a static list of values.
    ///
    /// This transform is most commonly used with ``impulse()`` to create an intial set of inputs for the pipeline. The values passed to
    /// this transform are encoded in the pipeline submission making it suitable for passing information from input parameters, such as a set
    /// of files to process.
    ///
    /// Note that because these values are encoded in the pipeline itself that they are subject to the maximum size of a protocol buffer (2GB)
    /// and large inputs can affect submitting the pipeline to the runner.
    ///
    /// - Parameters:
    ///     - values: An array of ``Codable`` values to be emitted for each input record
    ///     - name: An optional name for this transform. By default it will use the source file and line number for easy debugging
    ///
    /// - Returns: A ``PCollection`` containing contaings items of type `Value`
    func create<Value: Codable>(_ values: [Value], name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Value> {
        pstream(name: name ?? "\(_file):\(_line)", type: .bounded, values) { values, input, output in
            for try await (_, ts, w) in input {
                for v in values {
                    output.emit(v, timestamp: ts, window: w)
                }
            }
        }
    }
}

// Logging Transforms
public extension PCollection {
    @discardableResult
    func log(prefix: String, _ name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Of> where Of == String {
        pstream(name: name ?? "\(_file):\(_line)", prefix) { prefix, input, output in
            for await element in input {
                print("\(prefix): \(element)")
                output.emit(element)
            }
        }
    }

    @discardableResult
    func log<K, V>(prefix: String, _ name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<KV<K, V>> where Of == KV<K, V> {
        pstream(name: name ?? "\(_file):\(_line)", prefix) { prefix, input, output in
            for await element in input {
                let kv = element.0
                for v in kv.values {
                    print("\(prefix): \(kv.key),\(v)")
                }
                output.emit(element)
            }
        }
    }
}

// Mapping Operations
public extension PCollection {
    /// Perform a scalar transformation of input values from a ``PCollection`` to another type without modifying the window or the timestamp.
    ///
    ///  For example, the following code would return a ``PCollection<String>`` if applied to an input ``PCollection`` whose value has
    ///  a ``String`` property named `name`
    ///  ```swift
    ///    input.map { $0.name }
    ///  ```
    /// Note: The return type of the mapping function is treated as a scalar even if it is an iterable type. To return multiple values from a single input
    /// use ``flatMap(name:_file:_line:_:)`` instead.
    ///
    /// - Parameters:
    ///     - name: An optional name for this transform. By default it will use the file name and line number for easy debugging.
    ///     - fn: A trailing closure specifying the mapping function
    ///
    /// - Returns: A ``PCollection<Out>`` of scalar transformations.
    func map<Out>(name: String? = nil, _file: String = #fileID, _line: Int = #line, _ fn: @Sendable @escaping (Of) -> Out) -> PCollection<Out> {
        pardo(name: name ?? "\(_file):\(_line)") { input, output in
            output.emit(fn(input.value))
        }
    }

    /// Map a scalar input into a tuple representing a key-value pair. This is most commonly used in conjunction with ``groupByKey()`` transformations but can
    /// be used as a scalar transform to Beam's native key-value coding type.
    ///
    /// - Parameters:
    ///   - name: An optional name for this transform. By default it will use the file name and line number for easy debugging.
    ///   - fn: A trailing closure specifying the mapping function
    ///
    /// - Returns: A ``PCollection<KV<K,V>>`` which is encoded using Beam's native key value coding.
    func map<K, V>(name: String? = nil, _file: String = #fileID, _line: Int = #line, _ fn: @Sendable @escaping (Of) -> (K, V)) -> PCollection<KV<K, V>> {
        pardo(name: name ?? "\(_file):\(_line)") { input, output in
            let (key, value) = fn(input.value)
            output.emit(KV(key, value))
        }
    }

    /// Map a ``PCollection`` to zero or more outputs by returning a ``Sequence``
    ///
    /// - Parameters:
    ///   - name: An optional name for this transform. By default it will use the source filename and line number for easy debugging.
    ///   - fn: A trailing closure that returns a ``Sequence``.
    ///
    /// - Returns: A ``PCollection`` of values with the type ``Sequence.Element``
    ///
    func flatMap<S:Sequence>(name: String? = nil,_file: String = #fileID,_line: Int = #line,_ fn: @Sendable @escaping (Of) -> S) -> PCollection<S.Element> {
        pardo(name: name ?? "\(_file):\(_line)") { input,output in
            for i in fn(input.value) {
                output.emit(i)
            }
        }
    }
}

// Timestamp Operations
public extension PCollection {
    /// Modifies the timestamps of values in the input ``PCollection`` according to user specified logic
    /// - Parameters:
    ///   - name: An optional name for this transform. By default it will use the source filename and line number for easy debugging.
    ///   - fn: A trailing closure that returns the new timestamp for the input value.
    ///
    /// - Returns: A ``PCollection`` with modified timestamps.
    ///
    func timestamp(name: String? = nil, _file: String = #fileID, _line: Int = #line, _ fn: @Sendable @escaping (Of) -> Date) -> PCollection<Of> {
        pstream(name: name ?? "\(_file):\(_line)") { input, output in
            for try await (value, _, w) in input {
                output.emit(value, timestamp: fn(value), window: w)
            }
        }
    }
}

public extension PCollection<Never> {
    /// A convience implementation of ``create(_:name:_file:_line:)-38du`` that prepends an ``impulse()`` transform to a pipeline root.
    /// - Parameters:
    ///   - values: The values to emit when the impulse item is received
    ///   - name: An optional name for this transform. By default it will use the source filename and line number for easy debugging.
    ///
    /// - Returns: A ``PCollection`` of values from the static list
    func create<Value: Codable>(_ values: [Value], name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Value> {
        impulse().create(values, name: name, _file: _file, _line: _line)
    }
}

public func create<Value: Codable>(_ values: [Value], name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Value> {
    let root = PCollection<Never>(type: .bounded)
    return root.create(values, name: name, _file: _file, _line: _line)
}
