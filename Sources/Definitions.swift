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
    
    init(query: String) {
        self.query = query
        consistency = .all
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

public enum Opcode: UInt8 {
    case error          = 0x00
    case startup        = 0x01
    case ready          = 0x02
    case authenticate   = 0x03
    case options        = 0x05
    case supported      = 0x06
    case query          = 0x07
    case result         = 0x08
    case prepare        = 0x09
    case auth_success   = 0x10
    case execute        = 0x0A
    case register       = 0x0B
    case event          = 0x0C
    case batch          = 0x0D
    case auth_challenge = 0x0E
    case auth_response  = 0x0F
    case unknown        = 0xFF
}

public struct CqlFrameHeader {
    public let version: UInt8
    public let flags: UInt8
    public let stream: Int16
    public let opcode: UInt8
}

/// The first element of the body of a RESULT message is an [int] representing the
///`kind` of result.
public enum KindResult : UInt8 {
    case KindVoid = 0x01
    case KindRows = 0x02
    case KindSetKeyspace = 0x03
    case KindPrepared = 0x04
    case KindSchema = 0x05
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

enum BatchType: UInt8 {
    case Logged = 0x00
    case Unlogged = 0x01
    case Counter = 0x02
}

enum CqlValueType: UInt16 {
    case ColumnCustom = 0x0000
    case ColumnASCII = 0x0001
    case ColumnBitInt = 0x0002
    case ColumnBlob = 0x0003
    case ColumnBoolean = 0x0004
    case ColumnCounter = 0x0005
    case ColumnDecimal = 0x0006
    case ColumnDouble = 0x0007
    case ColumnFloat = 0x0008
    case ColumnInt = 0x0009
    case ColumnText = 0x000A
    case ColumnTimestamp = 0x000B
    case ColumnUuid = 0x000C
    case ColumnVarChar = 0x000D
    case ColumnVarInt = 0x000E
    case ColumnTimeUuid = 0x000F
    case ColumnInet = 0x0010
    case ColumnList = 0x0020
    case ColumnMap = 0x0021
    case ColumnSet = 0x0022
    case ColumnUnknown
//    case ColumnUDT = 0x0030
//    case ColumnTuple = 0x0031
}
    
func cql_column_type(val: UInt16) -> CqlValueType {
    switch val {
    case 0x0000:
        return CqlValueType.ColumnCustom
    case 0x0001:
        return CqlValueType.ColumnASCII
    case 0x0002:
        return CqlValueType.ColumnBitInt
    case 0x0003:
        return CqlValueType.ColumnBlob
    case 0x0004:
        return CqlValueType.ColumnBoolean
    case 0x0005:
        return CqlValueType.ColumnCounter
    case 0x0006:
        return CqlValueType.ColumnDecimal
    case 0x0007:
        return CqlValueType.ColumnDouble
    case 0x0008:
        return CqlValueType.ColumnFloat
    case 0x0009:
        return CqlValueType.ColumnInt
    case 0x000A:
        return CqlValueType.ColumnText
    case 0x000B:
        return CqlValueType.ColumnTimestamp
    case 0x000C:
        return CqlValueType.ColumnUuid
    case 0x000D:
        return CqlValueType.ColumnVarChar
    case 0x000E:
        return CqlValueType.ColumnVarInt
    case 0x000F:
        return CqlValueType.ColumnTimeUuid
    case 0x0010:
        return CqlValueType.ColumnInet
    case 0x0020:
        return CqlValueType.ColumnList
    case 0x0021:
        return CqlValueType.ColumnMap
    case 0x0022:
        return CqlValueType.ColumnSet
    default:
        return CqlValueType.ColumnUnknown
    }
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

public struct RCError {
    public let kind: RCErrorType
    public let desc: String
    
    init(msg: String, kind: RCErrorType) {
        self.desc = msg
        self.kind = kind
    }
}

public enum IPProtocolVersion {
    case Ipv4
    case Ipv6
}

public struct IPAddress {
    let protocolType: IPProtocolVersion
    let ipAddress: String
    let port: UInt16
}


public struct CqlStringMap {
    public var pairs: [CqlPair]
}

public struct CqlPair {
    public var key: String
    public var value: String
}

enum CqlBytesSize {
    case Cqli32
    case Cqli16
}

public struct CqlTableDesc {
    public let keyspace: String
    public let tablename: String
}

public struct CqlColMetadata {
    public var keyspace: String
    public var table: String
    public var col_name: String
    public var col_type: String
    public var col_type_aux1: String
    public var col_type_aux2: String
}

public struct CqlMetadata {
    public var flags: UInt32
    public var column_count: UInt32
    public var keyspace: String
    public var table: String
    public var row_metadata: [CqlColMetadata]
}
    
public struct Pair<T,V> {
    public var key: T
    public var value: V
}

public typealias CQLList = [CqlValue]
public typealias CQLMap = [Pair<CqlValue,CqlValue>]
public typealias CQLSet = [CqlValue]

public enum CqlValue {
    case CqlASCII(String)
    case CqlBigInt(Int64)
    case CqlBlob([UInt8])
    case CqlBoolean(Bool)
    case CqlCounter(Int64)
    case CqlDecimal(Int)
    case CqlDouble(Float64)
    case CqlFloat(Float32)
//    case CqlInet(IpAddr)
    case CqlInt(Int32)
    case CqlList(CQLList)
    case CqlMap(CQLMap)
    case CqlSet(CQLSet)
    case CqlText(String)
    case CqlTimestamp(UInt64)
    case CqlTimeUuid(UUID)
    case CqlVarchar(String)
    case CqlVarint(Int)
    case CqlUnknown
}

public struct CqlRow {
    public var cols: [CqlValue]
}

public struct CqlRows {
    public var metadata: CqlMetadata
    public var  rows: [CqlRow]
}

//public struct CqlRequest {
//    public var version: UInt8
//    public var flags: UInt8
//    public var stream: Int16
//    public var opcode: OpcodeRequest
//    public var body: CqlRequestBody
//}
//
//enum CqlRequestBody {
//    case RequestStartup(CqlStringMap)
//    case RequestQuery(String, Consistency, UInt8)
//    case RequestPrepare(String)
//    case RequestExec([UInt8], [CqlValue], Consistency, UInt8)
////    case RequestBatch([Query], BatchType, Consistency, UInt8)
//    case RequestOptions
//    case RequestAuthResponse([UInt8])
//}

    
    
    
    
    
    
    
    
    
    
    
    
