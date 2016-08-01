/**
 Copyright IBM Corporation 2016
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import Foundation
import Socket

public enum ResultKind {
    case void
    case rows(metadata: Metadata, columnTypes: [(name: String, type: DataType)], rows: [[Data]])
    case schema(type: String, target: String, options: String)
    case keyspace(name: String)
    case prepared(id: UInt16, metadata: Metadata?, resMetadata: Metadata?)
    
    public var description: String {
        switch self {
        case .void                           : return "Void"
        case .rows(let m, let c, let r)      : return prettyPrint(metadata: m, columnTypes: c, rows: r)
        case .schema(let t, let ta, let o)   : return "Scheme type: \(t), target: \(ta), options: \(o)"
        case .keyspace(let name)             : return "KeySpace: \(name)"
        case .prepared                       : return "Prepared"
        }
    }
    public init(body: Data) {
        var body = body
        
        let type = body.decodeInt
        
        switch type {
        case 2 : self = parseRows(body: body)
        case 3 : self = ResultKind.keyspace(name: body.decodeString)
        case 4 : self = parsePrepared(body: body)
        case 5 : self = ResultKind.schema(type: body.decodeString, target: body.decodeString, options: body.decodeString)
        default: self = ResultKind.void
        }
    }
}
public func prettyPrint(metadata: Metadata, columnTypes: [(name: String, type: DataType)], rows: [[Data]]) -> String {
    
    var str = ""
    
    if !metadata.isRowHeaderPresent {
        str += "Keyspace: \(metadata.keyspace!) ---- Table: \(metadata.table!)\n"
    }
    
    for i in 0..<columnTypes.count {
        if i == columnTypes.count - 1 {
            str += "\(columnTypes[i].name)  |\t\n"
        } else {
            str += "\(columnTypes[i].name)  |\t"
        }
        
    }
    for i in 0..<columnTypes.count {
        if i == columnTypes.count - 1 {
            str += String(repeating: "=".characters.first!, count: 12)
            str += "\n"
        } else {
            str += String(repeating: "=".characters.first!, count: 12)
        }
    }
    for row in rows {
        for i in 0..<columnTypes.count {
            var val = row[i]
            switch columnTypes[i].type {
            case .int: str += "\(val.decodeInt)  | \t"
            case .text: str += "\(val.decodeSDataString)  |\t"
            case .varChar: str += "\(val.decodeSDataString)  |\t"
            default: str += "unknown  |\t"
            }
        }
        str += "\n"
    }
    return str
}
