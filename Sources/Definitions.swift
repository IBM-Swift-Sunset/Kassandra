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

/// Return Code Error
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

func packType(_ item: Any) -> String {
    switch item {
    case let val as [Any]           : return String(describing: val.map { packType($0) } )
    case let val as [String: Any]   : return "{ \(val.map { key, value in "\(packType(key)) : \(packType(value))" }.joined(separator: ", ")) }"
    case let val as String          : return "'\(val)'"
    case let val as Date            : return String(describing: UInt64(val.timeIntervalSince1970) * 1000)
    case let val as NSUUID            : return String(describing: val.uuidString)
    default                         : return String(describing: item)
    }
}
func packColumnData(key: String, mirror: Mirror) -> String {
    
    var str = ""
    for child in mirror.children {
        switch child.value {
        case is UInt8        : str += child.label! + " int "
        case is UInt16       : str += child.label! + " int "
        case is UInt32       : str += child.label! + " int "
        case is UInt64       : str += child.label! + " bigInt "
        case is Int          : str += child.label! + " int "
        case is String       : str += child.label! + " text "
        case is Float        : str += child.label! + " float "
        case is Double       : str += child.label! + " double "
        case is Decimal      : str += child.label! + " decimal "
        case is Bool         : str += child.label! + " bool "
        
        case is Date         : str += child.label! + " timestamp "
        case is NSUUID       : str += child.label! + " uuid "
        case is [Any]        : str += child.label! + " list "
        case is [String: Any]: str += child.label! + " map "
        default: break
        }
        
        child.label! == key ? (str += "PRIMARY KEY,") : (str += ",")
    }
    return str
}

func packPairs(_ pairs: [String: Any], mirror: Mirror? = nil) -> String {
    return pairs.map{key,val in  key + "=" + packType(val) }.joined(separator: ", ")
}
func packKeys(_ dict: [String: Any]) -> String {
    return dict.map {key, value in key }.joined(separator: ", ")
}
func packKeys(_ mirror: Mirror) -> String {
    return mirror.children.map { $0.label! }.joined(separator: ", ")
}
func packValues(_ dict: [String: Any]) -> String {
    return dict.map {key, value in packType(value) }.joined(separator: ", ")
}
func packValues(_ mirror: Mirror) -> String {
    return mirror.children.map{ packType($0.value) }.joined(separator: ", ")
}
