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

public class Register: Frame {
    
    let events: [String]
    
    init(events: [String]){
        self.events = events
        super.init(opcode: .register)
        
    }
    
    func write(writer: SocketWriter) throws {
        
        header.append(version)
        header.append(flags)
        header.append(streamID.bigEndian.data)
        header.append(opcode.rawValue)
        
        body.append(events.count.data)
        for event in events {
            body.append(event.data)
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
