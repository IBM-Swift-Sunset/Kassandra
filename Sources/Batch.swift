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

public class Batch: Frame {
    /*The body of the message must be:
     <id><query_parameters>
     where <id> is the prepared query ID. It's the [short bytes] returned as a
     response to a PREPARE message. As for <query_parameters>, it has the exact
     same definition than in QUERY*/
    let type: BatchType
    let consistency: Consistency
    let Batchflags: UInt8
    
    var withNames: Bool {
        return Batchflags & 0x04 == 0x04 ? true : false
    }

    let queries: [Query]
    
    init(queries: [Query]){
        self.queries = queries
        self.type = .Unlogged
        self.consistency = .any
        self.Batchflags = 0x00
        super.init(opcode: .batch)
        
    }
    
    func write(writer: SocketWriter) throws {
        
        header.append(version)
        header.append(flags)
        header.append(streamID.bigEndian.data)
        header.append(opcode.rawValue)
        
        // set up body
        body.append(type.rawValue.data)
        body.append(queries.count.data)
        
        for query in queries {
            if withNames {
                
            }
            body.append(query.pack())
        }
        
        body.append(consistency.rawValue.data)
        
        if Batchflags & 0x10 == 0x10 {
            body.append(Consistency.serial.rawValue.data)
        }
        if Batchflags & 0x20 == 0x20 {
            //body.append() // optional timestamp
        }

        header.append(body.count.data)
        header.append(body)
        
        do {
            try writer.write(from: header)
            
        } catch {
            throw error
            
        }
    }
}
