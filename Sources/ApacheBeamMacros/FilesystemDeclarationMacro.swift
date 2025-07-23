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

public import SwiftSyntax
import SwiftSyntaxBuilder
public import SwiftSyntaxMacros

public enum FilesystemMacroError : Error {
    case filesytemOnlySupportedOnClasses
}

public struct FilesystemDeclarationMacro: ExtensionMacro, Sendable {
    public static func expansion(of node: SwiftSyntax.AttributeSyntax, attachedTo declaration: some SwiftSyntax.DeclGroupSyntax, providingExtensionsOf type: some SwiftSyntax.TypeSyntaxProtocol, conformingTo protocols: [SwiftSyntax.TypeSyntax], in context: some SwiftSyntaxMacros.MacroExpansionContext) throws -> [SwiftSyntax.ExtensionDeclSyntax] {
        guard declaration.is(ClassDeclSyntax.self) else {
            throw FilesystemMacroError.filesytemOnlySupportedOnClasses
        }
        let ext: DeclSyntax = """
            extension \(type.trimmed) : DiscoverableFilesystem {
                public static func factory() -> FilesystemProviderFactory {
                    return { try \(type.trimmed).filesystem(for: $0) }
                }
            }
            """
        guard let decl = ext.as(ExtensionDeclSyntax.self) else {
            return []
        }
        return [decl]
    }

}

