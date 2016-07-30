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

public class Event: Frame {
    let type: String
    let changeType: String?
    
    var inet: (String, Int)? = nil
    
    var target: String? = nil
    var options: String? = nil
    var keyspace: String? = nil
    var objName: String? = nil

    init(body: Data){
        var body = body
        
        type = body.decodeString
    
        switch type {
            case "TOPOLOGY_CHANGE":
                changeType = body.decodeString //"NEW_NODE", "REMOVED_NODE", or "MOVED_NODE"
                inet       = body.decodeInet
            case "STATUS_CHANGE":
                changeType = body.decodeString //"UP", "DOWN"
                inet       = body.decodeInet
            case "SCHEMA_CHANGE":
                changeType = body.decodeString //"CREATED", "UPDATED" or "DROPPED"
                target     = body.decodeString //"KEYSPACE", "TABLE" or "TYPE"

                if target == "KeySpace" {
                    options  = body.decodeString
                } else {
                    keyspace = body.decodeString
                    objName  = body.decodeString
                }
            default: changeType = nil
        }
        super.init(opcode: .event)
        
    }
}
