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
    case rows(metadata: Metadata, rows: [Row])
    case schema(type: String, target: String, options: String)
    case keyspace(name: String)
    case prepared(id: UInt16, metadata: Metadata?, resMetadata: Metadata?)
    
    public var description: String {
        switch self {
        case .void                           : return "Void"
        case .rows(let m, let r)             : return "Rows"
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
        case 3 : self = .keyspace(name: body.decodeString)
        case 4 : self = parsePrepared(body: body)
        case 5 : self = .schema(type: body.decodeString, target: body.decodeString, options: body.decodeString)
        default: self = .void
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
    }
    
    if flags & 0x0002 == 0x0002 {
        // paging state [bytes] type
        let length = data.decodeInt
        pagingState = data.subdata(in: Range(0..<length))
        data = data.subdata(in: Range(length..<data.count))
    }

    return flags & 0x0004 == 0x0004 ?
        (data: data, meta: Metadata(flags: flags)) :
        (data: data,
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

    var headers = [HeaderKey]()
    var rowVals = [[Any]]()

    for _ in 0..<metadata.columnCount {
        if metadata.isRowHeaderPresent {
            let _ = data.decodeString //ksname
            let _ = data.decodeString //tablename
        }
        headers.append(HeaderKey(field: data.decodeString, type: DataType(rawValue: Int(data.decodeUInt16))!))
    }
    
    // Parse Row Content
    for _ in 0..<data.decodeInt {

        var values = [Any]()

        for i in 0..<metadata.columnCount {
            
            let length = Int(data.decodeInt32)

            if length < 0 {
                values.append("NULL") // null
                continue
            }

            var value = data.subdata(in: Range(0..<length))

            //NOTE: Convert value to appropriate type here or leave as data?
            switch headers[i].type! {
            case .custom     : values.append(value.decodeInt)
            case .ASCII      : values.append(value.decodeInt)
            case .bitInt     : values.append(value.decodeInt)
            case .blob       : values.append(value.decodeInt)
            case .boolean    : values.append(value.decodeBool)
            case .counter    : values.append(value.decodeInt)
            case .decimal    : values.append(value.decodeInt)
            case .double     : values.append(value.decodeDouble)
            case .float      : values.append(value.decodeFloat)
            case .int        : values.append(value.decodeInt)
            case .text       : values.append(value.decodeSDataString)
            case .timestamp  : values.append(value.decodeInt)
            case .uuid       : values.append(value.decodeInt)
            case .varChar    : values.append(value.decodeSDataString)
            case .varInt     : values.append(value.decodeInt)
            case .timeUuid   : values.append(value.decodeInt)
            case .inet       : values.append(value.decodeInt)
            case .list       : values.append(value.decodeInt)
            case .map        : values.append(value.decodeInt)
            case .set        : values.append(value.decodeInt)
            case .UDT        : values.append(value.decodeInt)
            case .tuple      : values.append(value.decodeInt)
            }

            data = data.subdata(in: Range(length..<data.count))
        }
        rowVals.append(values)
    }
    
    return .rows(metadata: metadata, rows: rowVals.map { Row(header: headers, fields: $0) })
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

public struct Row {
    
    let dict: [HeaderKey : Any]
    
    init(header: [HeaderKey], fields: [Any]){
        dict = Dictionary(keys: header, values: fields)
    }
    
    subscript(_ field: String) -> Any {
        return dict[HeaderKey(field: field, type: nil)] ?? "NULL"
    }
}
