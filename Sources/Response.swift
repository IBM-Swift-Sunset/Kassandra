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

public enum Response: CustomStringConvertible {
    
    var opcode: Byte {
        switch self {
        case .error          : return 0x00
        case .ready          : return 0x02
        case .authenticate   : return 0x03
        case .supported      : return 0x06
        case .result         : return 0x08
        case .authSuccess    : return 0x10
        case .event          : return 0x0C
        case .authChallenge  : return 0x0E
        }
    }
    
    public var description: String {
        switch self {
        case .error (let code, let message) : return ("Error: \(code) || \(message)")
        case .ready                         : return "Ready"
        case .authenticate(let authType)    : return "Authenticate with \(authType)"
        case .supported(let map)            : return "\(map)"
        case .result(let message)           : return message.description
        case .authSuccess                   : return "Authentication Success"
        case .event(let type)               : return type.description
        case .authChallenge(let token)      : return "Authentication Challenge with token: \(token)"
        }
    }
    
    public init(opcode: UInt8, body: Data) {
        var body = body
        
        let opcode = ResponseOpcodes(rawValue: opcode)!
        
        switch opcode {
        case .ready         : self = .ready
        case .authSuccess   : self = .authSuccess
        case .supported     : self = .supported(by: body.decodeStringMap)
        case .result        : self = .result(of: Kind(body: body))
        case .authChallenge : self = .authChallenge(with: body.decodeInt)
        case .authenticate  : self = .authenticate(with: body.decodeSString)
        case .error         : self = .error(code: body.decodeInt, message: body.decodeSString)
        case .event         : self = body.decodeEventResponse
        }
    }
    
    case error(code: Int, message: String)
    
    case ready
    
    case authenticate(with: String)
    
    case supported(by: [String: [String]])
    
    case result(of: Kind)
    
    case authSuccess
    
    case event(of: EventType)
    
    case authChallenge(with: Int)
}
//Event Type
public enum EventType: CustomStringConvertible {
    case topologyChange(type: String, inet: (String, Int))
    case statusChange(type: String, inet: (String, Int))
    case schemaChange(type: String, target: String, changes: schemaChangeType)
    case error
    
    public var description: String {
        switch self {
        case .topologyChange(let type, let inet) :
            return "Topology Change with Type: \(type) and Inet \(inet)"
        case .statusChange(let type, let inet)   :
            return "Status Change with Type: \(type) and Inet \(inet)"
        case .schemaChange(let type, let target, let changes):
            return "Schema Change: Type - \(type), Target - \(target): Changes: \(changes)"
        case .error: return ""
        }
    }
}
public enum schemaChangeType: CustomStringConvertible {
    case options(with: String)
    case keyspace(to: String, withObjName: String)
    
    public var description: String {
        switch self{
        case .options(let options): return "\(options)"
        case .keyspace(let name, let objName): return "\(name) \(objName)"
            
        }
    }
}
