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

public enum Kind {
    case void
    case rows(metadata: Metadata, rows: [Row])
    case schema(type: String, target: String, options: String)
    case keyspace(name: String)
    case prepared(id: UInt16, metadata: Metadata?, resMetadata: Metadata?)
    
    public var description: String {
        switch self {
        case .void                           : return "Void"
        case .rows(_,_)                      : return "Rows"
        case .schema(let t, let ta, let o)   : return "Scheme type: \(t), target: \(ta), options: \(o)"
        case .keyspace(let name)             : return "KeySpace: \(name)"
        case .prepared                       : return "Prepared"
        }
    }
    public init(body: Data) {
        var body = body
        
        let type = body.decodeInt
        
        switch type {
        case 2 : self = body.decodeRows
        case 3 : self = .keyspace(name: body.decodeSString)
        case 4 : self = body.decodePreparedResponse
        case 5 : self = .schema(type: body.decodeSString, target: body.decodeSString, options: body.decodeSString)
        default: self = .void
        }
    }
}

public struct HeaderKey: Hashable {
    let field: String
    let type: DataType?

    public var hashValue: Int {
        return field.hashValue
    }
}
public func ==(lhs: HeaderKey, rhs: HeaderKey) -> Bool {
    return lhs.hashValue == rhs.hashValue
}

public struct Row: CustomStringConvertible {
    
    let dict: [HeaderKey : Any]
    
    public var description: String {
        return dict.map{key, val in "\(key.field):\(String(describing: val))" }.joined(separator: ", ")
    }

    public init(header: [HeaderKey], fields: [Any]){
        dict = Dictionary(keys: header, values: fields)
    }
    
    public subscript(_ field: String) -> Any {
        return dict[HeaderKey(field: field, type: nil)] ?? "NULL"
    }
}
