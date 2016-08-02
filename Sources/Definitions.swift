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

public struct Query {
    var query: String //Long string - Int -> utf8
    var consistency: Consistency
    var flags: UInt8
    
    var values: UInt8 { // 0x01 [short] <n> followed by <n> [bytes]
        return (flags & 0x01)
    }
    var skip_metadata: UInt8 { // 0x02
        return (flags & 0x02)
    }
    var pageSize: UInt8 { // 0x04
        return (flags & 0x04)
    }
    var withPagingState: UInt8 { //0x08
        return (flags & 0x08)
    }
    var withSerialConsistency: UInt8 { // 0x10
        return (flags & 0x10)
    }
    var withDefaultTimestamp: UInt8 { // 0x20
        return (flags & 0x20)
    }
    var withValueNames: UInt8 { // 0x40
        return (flags & 0x40)
    }
    
    init(_ query: String) {
        self.query = query
        consistency = .one
        flags = 0x00
    }
    
    func pack() -> Data {
        var data = Data()
        
        data.append(query.sData)
        data.append(consistency.rawValue.data)
        data.append(flags.data)
        
        return data
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

public enum DataType: Int {
    case custom     = 0x0000
    case ASCII      = 0x0001
    case bitInt     = 0x0002
    case blob       = 0x0003
    case boolean    = 0x0004
    case counter    = 0x0005
    case decimal    = 0x0006
    case double     = 0x0007
    case float      = 0x0008
    case int        = 0x0009
    case text       = 0x000A
    case timestamp  = 0x000B
    case uuid       = 0x000C
    case varChar    = 0x000D
    case varInt     = 0x000E
    case timeUuid   = 0x000F
    case inet       = 0x0010
    case list       = 0x0020
    case map        = 0x0021
    case set        = 0x0022
    case UDT        = 0x0030
    case tuple      = 0x0031
}

/// Return Code Errore
public enum RCErrorType: Error {
    case ReadError
    case WriteError
    case SerializeError
    case ConnectionError
    case NoDataError
    case GenericError(String)
    case IOError
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

public struct TableObj {
    
    var rows: [Row]
    
    init(rows: [Row]){
        self.rows = rows
    }
    
    subscript(_ index: String) -> String {
        return ""
    }
}
