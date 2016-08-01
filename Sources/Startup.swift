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

public struct Startup: Request {

    public let flags: Byte
    public let identifier: UInt16
    public let options: [String: String]

    public var description: String {
        return "Startup"
    }

    init(id: UInt16 = UInt16(random: true), flags: Byte = 0x00,options: [String: String] = ["CQL_VERSION":"3.0.0"]){
        identifier = id
        self.flags = flags
        self.options = options
        
    }

    public func write(writer: SocketWriter) throws {
        var header = Data()
        var body = Data()

        header.append(config.version)
        header.append(flags)
        header.append(identifier.bigEndian.data)
        header.append(Opcode.startup.rawValue.data)

        body.append(UInt16(options.count).data)
        
        for (key, value) in options {
            body.append(key.data)
            body.append(value.data)
        }
        
        header.append(body.count.data)

        header.append(body)

        try writer.write(from: header)

    }

}
