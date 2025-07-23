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
import Logging

public protocol FilesystemProvider {
    
    static func filesystem(for url: URL) throws -> Self?
}

public typealias FilesystemProviderFactory =  ((URL) throws -> (any FilesystemProvider)?)

public protocol DiscoverableFilesystem {
    static func factory() -> FilesystemProviderFactory
}

public enum FilesystemError : Error {
    case noFilesystemForURL(URL)
}

public struct Filesystems {
    
    let log = Logger(label: "Filesystems")
    let factories: [FilesystemProviderFactory]
    
    init() {
        var factories: [FilesystemProviderFactory] = []
        for aClass in Discovery.allClasses() {
            if(aClass is DiscoverableFilesystem.Type) {
                let discoverable = aClass as! DiscoverableFilesystem.Type
                let factory = discoverable.factory()
                factories.append(factory)
            }
        }
        self.factories = factories
    }
    
    func resolve(for url: URL) throws -> (any FilesystemProvider)? {
        for factory in factories {
            if let provider = try factory(url) {
                return provider
            }            
        }
        return nil
    }
    
    private static var shared: Filesystems = Filesystems()
    public static func filesystem(for url: URL) throws -> (any FilesystemProvider)? {
        return try shared.resolve(for: url)
    }
}

@attached(extension,conformances: DiscoverableFilesystem, names: arbitrary)
public macro Filesystem() = #externalMacro(module: "ApacheBeamMacros", type: "FilesystemDeclarationMacro")
