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

extension Bool {
    
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
    
    func matches(pattern: String!) -> Bool {
        
        do {
            let regex = try NSRegularExpression(pattern: pattern, options: [])
            let nsString = self as NSString
            let results = regex.matches(in: self, range: NSMakeRange(0, nsString.length))
            if nsString.substring(with: results[0].range) == self {
                return true
            }
        } catch {
            NSLog("Malformed Expression")
        }
        return false
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
    
    subscript(index: Int) -> UInt8 { //Returns a UInt8 with only the index bit set if applicable
        return 0
    }
}

extension UInt16 {
    
    init(random: Bool) {
        var r: UInt16 = 0
        arc4random_buf(&r, sizeof(UInt16.self))
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
            let u = Int(self.decodeUInt8) << 24
            let um = Int(self.decodeUInt8) << 16
            let ul = Int(self.decodeUInt8) << 8
            let l = Int(self.decodeUInt8)
            
            return u | um | ul | l
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
        return String(data: self, encoding: String.Encoding.utf8)!
    }
}
