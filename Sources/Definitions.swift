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

public enum Result {
    case error(ErrorType)
    case kind(Kind)
    case generic([String: Any])
    case void
    
    public var success: Bool {
        switch self {
        case .error : return false
        default     : return true
        }
    }

    public var asError: Error? {
        switch self {
        case .error(let err) : return err
        default              : return nil
        }
    }

    public var asRows: [Row]? {
        switch self {
        case .kind(let kind) :
            switch kind {
            case .rows(_, let rows) : return rows
            default                 : return nil
            }
        default: return nil
        }
    }

    public var asPrepared: [Byte]? {
        switch self {
        case .kind(let kind) :
            switch kind {
            case .prepared(let id, _, _) : return id
            default                      : return nil
            }
        default: return nil
        }
    }

    public var asKeyspace: String? {
        switch self {
        case .kind(let kind) :
            switch kind {
            case .keyspace(let name)     : return name
            default                      : return nil
            }
        default: return nil
        }
    }
    
    public var asSchema: (type: String, target: String, options: String)? {
        switch self {
        case .kind(let kind) :
            switch kind {
            case .schema(let t, let s, let o)  : return (type: t, target: s, options: o)
            default                            : return nil
            }
        default: return nil
        }
    }
}


public enum ResponseOpcodes: UInt8 {
    case error          = 0x00
    case ready          = 0x02
    case authenticate   = 0x03
    case supported      = 0x06
    case result         = 0x08
    case authSuccess    = 0x10
    case event          = 0x0C
    case authChallenge  = 0x0E
}

public enum Consistency: UInt16 {
    case any = 0x00
    case one = 0x01
    case two = 0x02
    case three = 0x03
    case quorum = 0x04
    case all = 0x05
    case local_quorum = 0x06
    case each_quorum = 0x07
    case serial = 0x0008
    case local_serial = 0x0009
    case local_one = 0x000A
    case unknown
}

public indirect enum DataType {
    public var opcode: UInt16 {
        switch self {
        case .custom    : return 0x0000
        case .ASCII     : return 0x0001
        case .bigInt    : return 0x0002
        case .blob      : return 0x0003
        case .boolean   : return 0x0004
        case .counter   : return 0x0005
        case .decimal   : return 0x0006
        case .double    : return 0x0007
        case .float     : return 0x0008
        case .int       : return 0x0009
        case .text      : return 0x000A
        case .timestamp : return 0x000B
        case .uuid      : return 0x000C
        case .varChar   : return 0x000D
        case .varInt    : return 0x000E
        case .timeUUID  : return 0x000F
        case .inet      : return 0x0010
        case .list      : return 0x0020
        case .map       : return 0x0021
        case .set       : return 0x0022
        case .UDT       : return 0x0030
        case .tuple     : return 0x0031
        }
    }
    
    case custom
    case ASCII
    case bigInt
    case blob
    case boolean
    case counter
    case decimal
    case double
    case float
    case int
    case text
    case timestamp
    case uuid
    case varChar
    case varInt
    case timeUUID
    case inet
    case list(type: DataType)
    case map(keyType: DataType, valueType: DataType)
    case set(type: DataType)
    case UDT(keyspace: String, name: String, headers: [HeaderKey])
    case tuple(types: [DataType])
}


/// Return Code Errore
public enum ErrorType: Error {
    case ReadError
    case WriteError
    case SerializeError
    case ConnectionError
    case NoDataError
    case GenericError(String)
    case IOError
    case CassandraError(Int, String)
}

public struct CqlColMetadata {
    public var keyspace: String
    public var table: String
    public var col_name: String
    public var col_type: String
    public var col_type_aux1: String
    public var col_type_aux2: String
}

public struct Metadata {
    let flags: Int
    let columnCount: Int
    let keyspace: String?
    let table: String?
    let rowMetadata: [CqlColMetadata]?
    
    var isRowHeaderPresent: Bool {
        return flags & 0x0001 == 0x0001 ? false : true
    }
    
    var hasPagination: Bool {
        return flags & 0x0002 == 0x0002 ? false : true
    }
    
    init(flags: Int, count: Int = 0, keyspace: String? = nil, table: String? = nil, rowMetadata: [CqlColMetadata]? = nil){
        self.flags = flags
        self.columnCount = count
        self.keyspace = keyspace
        self.table = table
        self.rowMetadata = rowMetadata
    }
}

public struct TableObj: CustomStringConvertible, Sequence {
    
    var rows: [Row]
    
    public var description: String {
        var body = ""
        var len = 0
        for row in rows {
            body += row.description + "\n"
            if row.description.characters.count > len { len = row.description.characters.count }
        }
        body += String(repeating: "-", count: len)
        var rStr = String(repeating: "-", count: len)
        rStr += body
        return rStr
    }

    init(rows: [Row]){
        self.rows = rows
    }
    
    public func makeIterator() -> Generator<Row> {
        return Generator<Row>(array: rows)
    }
}

public struct Generator<T> : IteratorProtocol {
    var array: Array<T>
    
    mutating public func next() -> T? {
        if array.isEmpty { return .none }
        let element = array[0]
        array = Array(array[1..<array.count])
        return element
    }
}
