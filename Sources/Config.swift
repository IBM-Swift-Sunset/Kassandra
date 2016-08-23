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

import SSLService

public typealias Byte = UInt8

public var config = Config.sharedInstance

public enum CompressionType {
    
}

public struct Config {
    
    public var host: String = "localhost"
    public var port: Int32 = 9042

    public let CQL_MAX_SUPPORTED_VERSION: UInt8 = 0x03
    public let version: Byte = 0x03
    public var connection: Kassandra? = nil
    
    public var flags: Byte = 0x00
    
    public var compressFlag: Bool {
        return (flags & 0x01) == 0x01 ? true : false
    }
    
    public var tracingFlag: Bool {
        return (flags & 0x02) == 0x02 ? true : false
    }
    
    public var compression: CompressionType? = nil
    
    
    public var SSLConfig: SSLService.Configuration? = nil

    public static var sharedInstance = Config()


    public mutating func setHostAndPort(host: String, port: Int32){
        self.host = host
        self.port = port
    }

    public mutating func setCompression(_ type: CompressionType) {
        compression = type
        flags = flags | 0x01
    }

    public mutating func setTracing() {
        flags = flags | 0x02

    }
    
    private init(){}
}
