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

public class QueryRequest: Frame {
    /*
     Performs a CQL query. The body of the message must be:
     <query><query_parameters>
     where <query> is a [long string] representing the query and
     <query_parameters> must be
     <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
    */
    let query: Query

    init(query: Query){
        self.query = query
        super.init(opcode: .query)
        
    }
    
    func write(writer: SocketWriter) throws {
        
        header.append(version)
        header.append(flags)
        header.append(streamID.bigEndian.data)
        header.append(opcode.rawValue)
        
        body.append(query.pack())

        header.append(body.count.data)
        
        header.append(body)
        
        do {
            try writer.write(from: header)
            
        } catch {
            throw error
            
        }
    }
    
}

