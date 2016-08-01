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

public enum Kind {
    case void
    case rows(metadata: Metadata, columnTypes: [(name: String, type: DataType)], rows: [[Data]])
    case schema(type: String, target: String, options: String)
    case keyspace(name: String)
    case prepared(id: UInt16, metadata: Metadata?, resMetadata: Metadata?)
    
    public var description: String {
        switch self {
        case .void                           : return "Void"
        case .rows(let m, let c, let r)      : return "Rows"
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
        case 3 : self = Kind.keyspace(name: body.decodeString)
        case 4 : self = parsePrepared(body: body)
        case 5 : self = Kind.schema(type: body.decodeString, target: body.decodeString, options: body.decodeString)
        default: self = Kind.void
        }
    }
}
// Result Kind Parsing Functions
public func parseMetadata(data: Data) -> (data: Data, meta: Metadata) {
    var data = data
    
    let flags = data.decodeInt
    let columnCount = data.decodeInt
    var globalKeySpace: String? = nil
    var globalTableName: String? = nil
    var pagingState = Data()
    
    if flags & 0x0001 == 0x0001 {
        globalKeySpace = data.decodeString
        globalTableName = data.decodeString
    } else {
        globalKeySpace = nil
        globalTableName = nil
    }
    
    if flags & 0x0002 == 0x0002 {
        // paging state [bytes] type
        let length = data.decodeInt
        pagingState = data.subdata(in: Range(0..<length))
        data = data.subdata(in: Range(length..<data.count))
    }
    if flags & 0x0004 == 0x0004 {
        return (data: data, meta: Metadata(flags: flags))
    }
    return (data: data,
            meta: Metadata(flags: flags, count: columnCount, keyspace: globalKeySpace, table: globalTableName, rowMetadata: nil))
    
    
}

func parsePrepared(body: Data) -> Kind {
    var body = body
    
    let id = body.decodeUInt16
    
    let (data, meta) = parseMetadata(data: body)
    let metadata = meta
    
    let (_, resMeta) = parseMetadata(data: data)
    
    return Kind.prepared(id: id, metadata: metadata, resMetadata: resMeta)
}

func parseRows(body: Data) -> Kind {
    var (data, metadata) = parseMetadata(data: body)
    var columnHeaders = [(name: String, type: DataType)]()
    var rows = [[Data]]()
    
    for _ in 0..<metadata.columnCount {
        if metadata.isRowHeaderPresent {
            let _ = data.decodeString //ksname
            let _ = data.decodeString //tablename
        }
        let name = data.decodeString
        let id = data.decodeUInt16
        columnHeaders.append((name, DataType(rawValue: Int(id))!))
        
    }
    
    // Parse Row Content
    for _ in 0..<data.decodeInt {
        var cols = [Data]()
        for _ in 0..<metadata.columnCount {
            
            let length = data.decodeInt
            let value = data.subdata(in: Range(0..<length))
            
            //NOTE: Convert value to appropriate type here or leave as data?
            
            data = data.subdata(in: Range(length..<data.count))
            cols.append(value)
        }
        rows.append(cols)
    }
    return Kind.rows(metadata: metadata, columnTypes: columnHeaders, rows: rows)
}
