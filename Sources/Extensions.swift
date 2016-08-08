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

extension SocketWriter {
    func write(from data: Data) throws {
        try self.write(from: NSData(data: data))
    }
}

public extension Bool {
    
    var toUInt8: UInt8 {
        return self ? 0x01 : 0x00
    }
}

extension String {

    var data: Data {
        var array = Data()
        
        let utf = self.data(using: String.Encoding.utf8)!
        array.append(UInt16(utf.count).data)
        array.append(utf)
        
        return array
    }
    
    var sData: Data {   // long string
        var array = Data()
        
        let utf = self.data(using: String.Encoding.utf8)!
        
        // Int, n, representing number of bytes in string
        array.append(utf.count.data)
        
        // n bytes of utf8 string
        array.append(utf)

        return array
    }
}

extension Int {

    init(data: Data) {
        let u = Int(data[0]) << 24
        let um = Int(data[1]) << 16
        let ul = Int(data[2]) << 8
        let l = Int(data[3])
        
        self = u | um | ul | l
    }
    var toUInt8s: [UInt8] {
        var encLength = [UInt8]()
        var length = self
        
        repeat {
            var digit = UInt8(length % 128)
            length /= 128
            if length > 0 {
                digit |= 0x80
            }
            encLength.append(digit)
            
        } while length != 0
        
        return encLength
    }
    var data: Data {
        var data = Data(capacity: 4)
        data.append(UInt8(self >> 24).data)
        data.append(UInt8(self >> 16).data)
        data.append(UInt8(self >> 8).data)
        data.append(UInt8(self).data)
        return data
    }
}

extension UInt8 {

    var data: Data {
        return Data(bytes: [self])
    }
    
    var bool: Bool {
        return self == 0x01 ? true : false
    }
    
    var int: Int {
        return Int(self)
    }
}

extension UInt16 {

    init(random: Bool) {
        var r: UInt16 = 0
        arc4random_buf(&r, MemoryLayout<UInt16>.size)
        self = r
    }
    
    init(msb: UInt8, lsb: UInt8) {
        self = (UInt16(msb) << 8) | UInt16(lsb)
    }
    
    var data: Data {
        var data = Data()
        var bytes: [UInt8] = [0x00, 0x00]
        bytes[0] = UInt8(self >> 8)
        bytes[1] = UInt8(self & 0x00ff)
        data.append(Data(bytes: bytes, count: 2))
        return data
    }
    
    var UInt8s: [UInt8] {
        var UInt8s: [UInt8] = [0x00, 0x00]
        UInt8s[0] = UInt8(self >> 8)
        UInt8s[1] = UInt8(self & 0x00ff)
        return UInt8s
    }
}

extension Data {
    
    var decodeBool: Bool {
        mutating get {
            return self.decodeUInt8 == 0x0001 ? true : false
        }
    }
    var decodeUInt8: UInt8 {
        mutating get {
            let uint = UInt8(self[0])
            self = self.subdata(in: Range(1..<self.count))
            return uint
        }
    }
    var decodeUInt16: UInt16 {
        mutating get {
            let uint = UInt16(msb: self[0], lsb: self[1])
            self = self.subdata(in: Range(2..<self.count))
            return uint
        }
    }
    var decodeInt: Int {
        mutating get {
            let u = Int(self.decodeUInt16) << 16
            let l = Int(self.decodeUInt16)
            
            return u | l
        }
    }
    var decodeInt32: Int32 {
        mutating get {
            let u = Int32(self.decodeUInt16) << 16
            let l = Int32(self.decodeUInt16)
            
            return u | l
        }
    }
    var decodeBigInt: Int64 {
        mutating get {
            let byte1 = Int64(self.decodeInt) << 32
            let byte2 = Int64(self.decodeInt)
            return byte1 | byte2
        }
    }
    var decodeDouble: Double {
        mutating get {
            let byte1 = Int64(self.decodeInt) << 32
            let byte2 = Int64(self.decodeInt)
            return Double(byte1 | byte2)
        }
    }
    var decodeFloat: Float {
        mutating get {
            let byte1 = self.decodeInt
            return Float(byte1)
        }
    }
    var decodeVarInt: Int {
        mutating get {
            return 1
        }
    }
    var decodeInet: (String, Int) {
        mutating get {
            let size = Int(self.decodeUInt8)
            var host = self.subdata(in: Range(0..<size))
            var port = self.subdata(in: Range(size..<self.count))
            return (host.decodeString, port.decodeInt)
        }
        
    }
    var decodeString: String {
        mutating get {
            let length = UInt16(msb: self[0], lsb: self[1])
            let str = self.subdata(in: Range(2..<2 + Int(length)))
            self = self.subdata(in: Range(2 + Int(length)..<self.count))
            return String(data: str, encoding: String.Encoding.utf8)!
        }
    }
    var decodeSDataString: String {
        return String(data: self, encoding: String.Encoding.utf8) ?? "NULL"
    }
    var decodeStringMap: [String: [String]] {
        mutating get {
            var map = [String: [String]]()
            
            for _ in 0..<Int(self.decodeUInt16) {
                let key = self.decodeString

                var strList = [String]()
                for _ in 0..<Int(self.decodeUInt16) {
                    strList.append(self.decodeString)
                }
                
                map[key] = strList
            }
            return map
        }
    }
    var decodeEventResponse: Response {
        mutating get {
            switch self.decodeString {
            case "TOPOLOGY_CHANGE":
                let changeType = self.decodeString
                let inet       = self.decodeInet
                return .event(of: .topologyChange(type: changeType, inet: inet))
            case "STATUS_CHANGE":
                let changeType = self.decodeString
                let inet       = self.decodeInet
                return .event(of: .statusChange(type: changeType, inet: inet))
            case "SCHEMA_CHANGE":
                let changeType = self.decodeString
                let target     = self.decodeString
                
                if target == "KeySpace" {
                    let options  = self.decodeString
                    return .event(of: .schemaChange(type: changeType, target: target, changes: .options(with: options)))
                } else {
                    let keyspace = self.decodeString
                    let objName  = self.decodeString
                    return .event(of: .schemaChange(type: changeType, target: target, changes: .keyspace(to: keyspace, withObjName: objName)))
                }
            default: return .event(of: .error)
            }
        }
    }
    var decodePreparedResponse: Kind {
        mutating get {
            
            let id = self.decodeUInt16
            
            let meta = self.decodeMetadata
            
            let resMeta = self.decodeMetadata
            
            return Kind.prepared(id: id, metadata: meta, resMetadata: resMeta)
        }
    }
    var decodeMetadata: Metadata {
        mutating get {
            let flags = self.decodeInt
            let columnCount = self.decodeInt
            var globalKeySpace: String? = nil
            var globalTableName: String? = nil
            var pagingState = Data()
            
            if flags & 0x0001 == 0x0001 {
                globalKeySpace = self.decodeString
                globalTableName = self.decodeString
            }
            
            if flags & 0x0002 == 0x0002 {
                // paging state [bytes] type
                let length = self.decodeInt
                pagingState = self.subdata(in: Range(0..<length))
                self = self.subdata(in: Range(length..<self.count))
            }
            
            return flags & 0x0004 == 0x0004 ? Metadata(flags: flags) :
                Metadata(flags: flags, count: columnCount, keyspace: globalKeySpace, table: globalTableName, rowMetadata: nil)
        }
    }
    var decodeRows: Kind {
        mutating get {
            let metadata = self.decodeMetadata
            
            var headers = [HeaderKey]()
            var rowVals = [[Any]]()
            
            for _ in 0..<metadata.columnCount {
                if metadata.isRowHeaderPresent {
                    let _ = self.decodeString //ksname
                    let _ = self.decodeString //tablename
                }
                headers.append(HeaderKey(field: self.decodeString, type: DataType(rawValue: Int(self.decodeUInt16))!))
            }
            
            // Parse Row Content
            for _ in 0..<self.decodeInt {
                
                var values = [Any]()
                
                for i in 0..<metadata.columnCount {
                    
                    let length = Int(self.decodeInt32)
                    
                    if length < 0 {
                        values.append("NULL") // null
                        continue
                    }
                    //String.Encoding.ascii
                    var value = self.subdata(in: Range(0..<length))
                    
                    //NOTE: Convert value to appropriate type here or leave as data?
                    switch headers[i].type! {
                    case .custom     : values.append(value.decodeInt)
                    case .ASCII      : values.append(value.decodeInt)
                    case .bigInt     : values.append(value.decodeBigInt)
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
                    
                    self = self.subdata(in: Range(length..<self.count))
                }
                rowVals.append(values)
            }
            
            return .rows(metadata: metadata, rows: rowVals.map { Row(header: headers, fields: $0) })
        }
    }
}
extension Dictionary {

    public init(keys: [Key], values: [Value]) {
        precondition(keys.count == values.count)
        
        self.init()
        
        for (index, key) in keys.enumerated() {
            self[key] = values[index]
        }
    }
}
public func changeDictType<T>(dict: [T: Any]) -> [String: Any] {
    var cond = [String: Any]()
    
    for (key, value) in dict {
        cond[String(describing: key)] = value
    }
    
    return cond
}
