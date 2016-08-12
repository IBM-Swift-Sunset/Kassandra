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

import Socket
import Foundation

public enum Request {
    
    var opcode: Byte {
        switch self {
        case .startup        : return 0x01
        case .options        : return 0x05
        case .query          : return 0x07
        case .prepare        : return 0x09
        case .execute        : return 0x0A
        case .register       : return 0x0B
        case .batch          : return 0x0D
        case .authResponse   : return 0x0F
        }
    }
    
    func write(id: UInt16, writer: SocketWriter) throws {
        var body = Data()
        
        switch self {
        case .options                        : break
        case .query(let query)               : body.append(query.pack())
        case .prepare(let query)             : body.append(query.packQuery())
        case .authResponse(let token)        : body.append(token.data)
        case .execute(let qid, let query)   :
            
            body.append(UInt16(qid.count).data)
            body.append(Data(bytes: qid, count: qid.count))
            body.append(query.packParameters())

        case .startup(var options)           :
            options["CQL_VERSION"] = "3.0.0"
            
            body.append(UInt16(options.count).data)
            
            for (key, value) in options {
                body.append(key.shortStringData)
                body.append(value.shortStringData)
            }
            
        case .register(let events)  :
            
            body.append(events.count.data)
            
            for event in events {
                body.append(event.shortStringData)
            }
            
        case .batch(let queries, let type, let flags, let consistency):

            switch type {
            case .logged    : body.append(0x00.data)
            case .unlogged  : body.append(0x01.data)
            case .counter   : body.append(0x02.data)
            }
            
            body.append(UInt16(queries.count).data)

            for query in queries {
                if let id = query.preparedID {
                    body.append(0x01.data)
                    body.append(UInt16(id.count).data)
                    body.append(Data(bytes: id, count: id.count))
                } else {
                    body.append(0x00.data)
                    body.append(query.packParameters())
                }
                
                body.append(UInt16(0).data)
            }

            body.append(consistency.rawValue.data)
            body.append(flags.data)
            
            if flags & 0x10 == 0x10 {
                body.append(Consistency.serial.rawValue.data)
            }

            if flags & 0x20 == 0x20 {
                body.append(Date.timestamp)
            }
        }

        var header = Data()
        header.append(config.version.data)
        header.append(config.flags.data)
        header.append(id.bigEndian.data)
        header.append(opcode.data)

        header.append(body.count.data)
        header.append(body)

        try writer.write(from: header)
        
    }
    
    case startup(options: [String: String])
    
    case options
    
    case query(using: Query)
    
    case prepare(query: Query)
    
    case execute(id: [Byte], parameters: Query)
    
    case register(events: [String])
    
    case batch(queries: [Query], type: BatchType, flags: Byte, consistency: Consistency)
    
    case authResponse(token: Int)
}
