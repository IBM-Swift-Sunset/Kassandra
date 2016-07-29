//
//  Execute.swift
//  Kassandra
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

public class Execute: Frame {
    /*The body of the message must be:
     <id><query_parameters>
     where <id> is the prepared query ID. It's the [short bytes] returned as a
     response to a PREPARE message. As for <query_parameters>, it has the exact
     same definition than in QUERY*/
    let query: Query
    
    init(query: Query){
        self.query = query
        super.init(opcode: .execute)
        
    }
    
    func write(writer: SocketWriter) throws {
        
        header.append(version)
        header.append(flags)
        header.append(streamID.bigEndian.data)
        header.append(opcode.rawValue)
        
        // set up body
        header.append(body.count.data)
        header.append(body)
        
        do {
            try writer.write(from: header)
            
        } catch {
            throw error
            
        }
    }
}
