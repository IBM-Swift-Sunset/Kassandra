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

public typealias Byte = UInt8

public protocol Request {
    var description: String { get }
    mutating func write(writer: Socket) throws
}

public class Frame {

    let version: Byte
    
    let flags: Byte
    
    let streamID: UInt16
    
    let opcode: Opcode
    
    var length: Int
    
    var header: Data

    var body: Data
    
    init(flags: Byte = 0x00, opcode: Opcode) {
        self.version = 0x03
        self.flags = flags
        self.streamID = UInt16(random: true)
        self.opcode = opcode
        self.length = 0

        header = Data()
        body = Data()
    }
}
