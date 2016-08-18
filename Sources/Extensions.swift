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
        let lm = Int(data[2]) << 8
        let l = Int(data[3])
        
        self = u | um | lm | l
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
        return Data(bytes: [UInt8((self & 0xFF000000) >> 24),UInt8((self & 0x00FF0000) >> 16),UInt8((self & 0x0000FF00) >> 8),UInt8(self & 0x000000FF)], count: 4)
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
        return [UInt8((self & 0xFF00) >> 8), UInt8(self & 0x00ff)]
    }
}

extension Double: Convertible {}
extension Float: Convertible {}
extension UInt32 {
    var data: Data {
        return Data(bytes: [UInt8((self & 0xFF000000) >> 24),UInt8((self & 0x00FF0000) >> 16),UInt8((self & 0x0000FF00) >> 8),UInt8(self & 0x000000FF)], count: 4)
    }
}
extension UInt64 {
    var data: Data {
        var data = Data()
        data.append(UInt32((self & 0xFFFFFFFF00000000) >> 32).data)
        data.append(UInt32((self & 0x00000000FFFFFFFF)).data)
        return data
    }
}
extension Date {
    static var data: Data {
        let stamp = Date().timeIntervalSince1970
        return stamp.bitPattern.data
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
            let length = Int(self.decodeUInt16)
            let blob = self.subdata(in: Range(0..<length))
            self = self.subdata(in: Range(blob.count..<self.count))
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
    
    var decodeUInt32: UInt32 {
        mutating get {
            let u = UInt32(self.decodeUInt16) << 16
            let l = UInt32(self.decodeUInt16)
            return u | l
        }
    }
    
    var decodeUInt64: UInt64 {
        mutating get {
        let u = UInt64(self.decodeUInt32) << 32
        let l = UInt64(self.decodeUInt32)
        
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
            let u = UInt64(self.decodeInt) << 32
            let l = UInt64(self.decodeInt)
            
            return Double(bitPattern: u | l)
        }
    }

    var decodeFloat: Float {
        mutating get {
            let u = UInt32(self.decodeInt)

            return Float(bitPattern: u)
        }
    }
    
    var decodeDecimal: Decimal {
        mutating get {
            return Decimal()
        }
    }
    
    var decodeVarInt: String {
        let buf = UnsafePointer<UInt8>(self.filter { _ in true })
        let charA = UInt8(UnicodeScalar("a").value)
        let char0 = UInt8(UnicodeScalar("0").value)
        
        func intToHex(value: UInt8) -> UInt8 {
            return (value > 9) ? (charA + value - 10) : (char0 + value)
        }
        
        let ptr = UnsafeMutablePointer<UInt8>.allocate(capacity: count * 2)
        
        for i in 0 ..< count {
            ptr[i*2] = intToHex(value: (buf[i] >> 4) & 0xF)
            ptr[i*2+1] = intToHex(value: buf[i] & 0xF)
        }
        
        return "0x" + String(bytesNoCopy: ptr, length: count*2, encoding: String.Encoding.utf8, freeWhenDone: true)!
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
        var data = Date.data
        let decodedata: UInt64 = data.decodeUInt64
        let timeInterval = TimeInterval(bitPattern: decodedata)
        let date = Date(timeIntervalSince1970: timeInterval)
        return date
    }
    
    var decodeUUID: NSUUID {
        mutating get {
            let blob = self.subdata(in: Range(0..<16))
            self = self.subdata(in: Range(16..<self.count))
            let data = UnsafeMutablePointer<UInt8>.allocate(capacity: 16)
            blob.copyBytes(to: data, count: 16)
            return NSUUID(uuidBytes: data)
        }
    }

    var decodeTimeUUID: NSUUID {
        mutating get {
            return self.decodeUUID
        }
    }
    
    var decodeList: [Any] {
        mutating get {
            let len = self.decodeInt
            var lst = [Data]()
            for _ in 0..<len {
                let len = self.decodeInt
                lst.append(self.subdata(in: Range(0..<len)))
                self = self.subdata(in: Range(len..<self.count))
            }
            return lst
        }
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
    
    var decodeType: DataType? {
        mutating get {
            //id
            let type = Int(self.decodeUInt16)
            //value
            switch type {
            case 0x0000: return DataType.custom
            case 0x0001: return DataType.ASCII
            case 0x0002: return DataType.bigInt
            case 0x0003: return DataType.blob
            case 0x0004: return DataType.boolean
            case 0x0005: return DataType.counter
            case 0x0006: return DataType.decimal
            case 0x0007: return DataType.double
            case 0x0008: return DataType.float
            case 0x0009: return DataType.int
            case 0x000A: return DataType.text
            case 0x000B: return DataType.timestamp
            case 0x000C: return DataType.uuid
            case 0x000D: return DataType.varChar
            case 0x000E: return DataType.varInt
            case 0x000F: return DataType.timeUUID
            case 0x0010: return DataType.inet
            case 0x0020: return DataType.list(type: self.decodeType!)
            case 0x0021: return DataType.map(keytype: self.decodeType!, valuetype: self.decodeType!)
            case 0x0022: return DataType.set(type: self.decodeType!)
            case 0x0030: return DataType.UDT(type: 1)
            case 0x0031: return DataType.tuple(type: 1)
            default    : return nil
            }
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
                
                headers.append(HeaderKey(field: self.decodeSString, type: self.decodeType!))
            }
            
            // Parse Row Content
            for _ in 0..<self.decodeInt {
                
                var values = [Any]()
                
                for i in 0..<metadata.columnCount {
                    
                    guard case let length = Int(self.decodeInt32), length > 0 else {
                        values.append("NULL")
                        continue
                    }
                    
                    //String.Encoding.ascii
                    let value = self.subdata(in: Range(0..<length))
                    let decodedValue = option(type: headers[i].type!, data: value)
                    values.append(decodedValue)
                    
                    self = self.subdata(in: Range(length..<self.count))
                }
                rowVals.append(values)
            }
            
            return .rows(metadata: metadata, rows: rowVals.map { Row(header: headers, fields: $0) })
        }
    }
}

public struct AnyKey: Hashable {
    public let underlying: Any
    public let hashValueFunc: () -> Int
    public let equalityFunc: (Any) -> Bool
    
    init<T: Hashable>(_ key: T) {
        underlying = key
        
        hashValueFunc = { key.hashValue }
        
        equalityFunc = {
            if let other = $0 as? T {
                return key == other
            }
            return false
        }
    }
    
    public var hashValue: Int { return hashValueFunc() }
}

public func ==(x: AnyKey, y: AnyKey) -> Bool {
    return x.equalityFunc(y.underlying)
}



private func option(type: DataType, data: Data) -> Any {
    var data = data
    
    switch type {
    case .custom     : return data.decodeHeaderlessString
    case .ASCII      : return data.decodeAsciiString
    case .bigInt     : return data.decodeBigInt
    case .blob       : return data.decodeBlob
    case .boolean    : return data.decodeBool
    case .counter    : return data.decodeInt
    case .decimal    : return data.decodeInt
    case .double     : return data.decodeDouble
    case .float      : return data.decodeFloat
    case .int        : return data.decodeInt
    case .text       : return data.decodeHeaderlessString
    case .timestamp  : return data.decodeTimeStamp
    case .uuid       : return data.decodeUUID
    case .varChar    : return data.decodeHeaderlessString
    case .varInt     : return data.decodeVarInt
    case .timeUUID   : return data.decodeUUID
    case .inet       : return data.decodeInt
    case .list(let t): return decodeList(type: t, data: data)
    case .map(let k, let v) : return decodeMap(keyType: k, valueType: v, data: data)
    case .set(let t) : return decodeSet(type: t, data: data)
    case .UDT        : return data.decodeInt
    case .tuple      : return data.decodeInt
    }
}

private func decodeList(type: DataType, data: Data) -> [Any] {
    var data = data
    let len = data.decodeInt
    var lst = [Any]()
    for _ in 0..<len {
        let len = data.decodeInt
        let val = option(type: type, data: data.subdata(in: Range(0..<len)))
        lst.append(val)
        data = data.subdata(in: Range(len..<data.count))
    }
    return lst
}

private func decodeSet(type: DataType, data: Data) -> Set<AnyKey> {
    var data = data
    let len = data.decodeInt
    var lst = Set<AnyKey>()
    for _ in 0..<len {
        let len = data.decodeInt
        let val = option(type: type, data: data.subdata(in: Range(0..<len)))
        switch val {
            case let k as String: lst.insert(AnyKey(k))
            case let k as Double: lst.insert(AnyKey(k))
            case let k as Float: lst.insert(AnyKey(k))
            case let k as Bool: lst.insert(AnyKey(k))
            default: break
        }
        data = data.subdata(in: Range(len..<data.count))
    }
    return lst
    
}

private func decodeMap(keyType: DataType,valueType: DataType, data: Data) -> [AnyKey : Any] {
    var data = data
    var map = [AnyKey : Any]()
    
    for _ in 0..<data.decodeInt {
        let len1 = data.decodeInt
        let key = option(type: keyType, data: data.subdata(in: Range(0..<len1)))
        data = data.subdata(in: Range(len1..<data.count))
        
        let len2 = data.decodeInt
        let value = option(type: valueType, data: data.subdata(in: Range(0..<len2)))
        data = data.subdata(in: Range(len2..<data.count))
        
        switch key {
        case let k as String: map[AnyKey(k)] = value
        default: break
        }
    }
    return map
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
