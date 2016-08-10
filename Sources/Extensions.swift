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

extension Bool: Convertible {
    
    var toUInt8: UInt8 {
        return self ? 0x01 : 0x00
    }
}

extension String: Convertible {

    var shortStringData: Data {
        var array = Data()
        
        let utf = self.data(using: String.Encoding.utf8)!
        array.append(UInt16(utf.count).data)
        array.append(utf)
        
        return array
    }
    
    var longStringData: Data {   // long string
        var array = Data()
        
        let utf = self.data(using: String.Encoding.utf8)!
        
        // Int, n, representing number of bytes in string
        array.append(utf.count.data)
        
        // n bytes of utf8 string
        array.append(utf)

        return array
    }
}

extension Int: Convertible {

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
        return Data(bytes: [UInt8(self >> 24), UInt8(self >> 16), UInt8(self >> 8), UInt8(self)], count: 4)
    }
}

extension UInt8: Convertible {

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

extension UInt16: Convertible {
    
    static var random: UInt16 {
        #if os(Linux)
            return UInt16(rand() % Int32(UInt16.max))
        #else
            return UInt16(arc4random_uniform(UInt32(UInt16(max))))
        #endif
    }
    
    init(msb: UInt8, lsb: UInt8) {
        self = (UInt16(msb) << 8) | UInt16(lsb)
    }
    
    var data: Data {
        return Data(bytes: self.UInt8s, count: 2)
    }
    
    var UInt8s: [UInt8] {
        return [UInt8(self >> 8), UInt8(self & 0x00ff)]
    }
}

extension Double: Convertible {}
extension Float: Convertible {}
extension UInt32 {
    var data: Data {
        return Data(bytes: [UInt8(self >> 24), UInt8(self >> 16),UInt8(self >> 8),UInt8(self)], count: 4)
    }
}
extension UInt64 {
    var data: Data {
        var data = Data()
        data.append(UInt32(self).data)
        data.append(UInt32(self >> 32).data)
        return data
    }
}
extension Date {
    static var timestamp: Data {
        let stamp = UInt64(Date().timeIntervalSince1970)
        return stamp.data
    }
}
extension Data {

    var decodeBlob: [UInt8] {
        mutating get {
            return self.filter{ _ in return true }
        }
    }
    
    var decodeShortBytes: [UInt8] {
        mutating get {
            let blob = self.subdata(in: Range(2..<Int(self.decodeUInt16)))
            self = self.subdata(in: Range(2 + blob.count..<self.count))
            return blob.filter{ _ in return true }
        }
    }
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
            let u = Int64(self.decodeInt) << 32
            let l = Int64(self.decodeInt)
            return u | l
        }
    }

    var decodeDouble: Double {
        mutating get {
            let u = Int64(self.decodeInt) << 32
            let l = Int64(self.decodeInt)
            return Double(u | l)
        }
    }

    var decodeFloat: Float {
        mutating get {
            let u = self.decodeInt
            return Float(u)
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
            return (host.decodeSString, port.decodeInt)
        }
        
    }

    // Decode Cassandra <String>
    var decodeSString: String {
        mutating get {
            let length = UInt16(msb: self[0], lsb: self[1])
            let str = self.subdata(in: Range(2..<2 + Int(length)))
            self = self.subdata(in: Range(2 + Int(length)..<self.count))
            return str.decodeHeaderlessString
        }
    }
    
    // Decode Cassandra <Long String>
    var decodeLString: String {
        mutating get {
            let length = self.decodeInt
            let str = self.subdata(in: Range(2..<2 + length))
            self = self.subdata(in: Range(2 + length..<self.count))
            return str.decodeHeaderlessString
        }
    }

    var decodeHeaderlessString: String {
        return String(data: self, encoding: String.Encoding.utf8) ?? "NULL"
    }

    var decodeAsciiString: String {
        return String(data: self, encoding: String.Encoding.ascii) ?? "NULL"
    }
    
    var decodeStringMap: [String: [String]] {
        mutating get {
            var map = [String: [String]]()
            
            for _ in 0..<Int(self.decodeUInt16) {
                let key = self.decodeSString

                var strList = [String]()
                for _ in 0..<Int(self.decodeUInt16) {
                    strList.append(self.decodeSString)
                }
                
                map[key] = strList
            }
            return map
        }
    }
    
    var decodeTimeStamp: Date {
        let _: Date = Date()
        return Date()
    }
    
    var decodeUUID: UUID {
        return UUID(uuidString: self.decodeHeaderlessString)!
    }

    var decodeEventResponse: Response {
        mutating get {
            switch self.decodeSString {
            case "TOPOLOGY_CHANGE":
                let changeType = self.decodeSString
                let inet       = self.decodeInet
                return .event(of: .topologyChange(type: changeType, inet: inet))
            case "STATUS_CHANGE":
                let changeType = self.decodeSString
                let inet       = self.decodeInet
                return .event(of: .statusChange(type: changeType, inet: inet))
            case "SCHEMA_CHANGE":
                let changeType = self.decodeSString
                let target     = self.decodeSString
                
                if target == "KeySpace" {
                    let options  = self.decodeSString
                    return .event(of: .schemaChange(type: changeType, target: target, changes: .options(with: options)))
                } else {
                    let keyspace = self.decodeSString
                    let objName  = self.decodeSString
                    return .event(of: .schemaChange(type: changeType, target: target, changes: .keyspace(to: keyspace, withObjName: objName)))
                }
            default: return .event(of: .error)
            }
        }
    }

    var decodePreparedResponse: Kind {
        mutating get {
            let id = self.decodeShortBytes

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
                globalKeySpace = self.decodeSString
                globalTableName = self.decodeSString
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
                    let _ = self.decodeSString //ksname
                    let _ = self.decodeSString //tablename
                }
                headers.append(HeaderKey(field: self.decodeSString, type: DataType(rawValue: Int(self.decodeUInt16))!))
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
                    case .custom     : values.append(value.decodeHeaderlessString)
                    case .ASCII      : values.append(value.decodeAsciiString)
                    case .bigInt     : values.append(value.decodeBigInt)
                    case .blob       : values.append(value.decodeBlob)
                    case .boolean    : values.append(value.decodeBool)
                    case .counter    : values.append(value.decodeInt)
                    case .decimal    : values.append(value.decodeInt)
                    case .double     : values.append(value.decodeDouble)
                    case .float      : values.append(value.decodeFloat)
                    case .int        : values.append(value.decodeInt)
                    case .text       : values.append(value.decodeHeaderlessString)
                    case .timestamp  : values.append(value.decodeTimeStamp)
                    case .uuid       : values.append(value.decodeUUID)
                    case .varChar    : values.append(value.decodeHeaderlessString)
                    case .varInt     : values.append(value.decodeInt)
                    case .timeUUID   : values.append(value.decodeUUID)
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
