//
//  Config.swift
//  Kassandra
//
//  Created by Aaron Liberatore on 7/29/16.
//
//

import Foundation
import Socket

public typealias Byte = UInt8

public var config = Config.sharedInstance

public struct Config {
    
    let CQL_MAX_SUPPORTED_VERSION: UInt8 = 0x03
    let version: Byte = 0x03
    var connection: Kassandra? = nil

    var map = [UInt16: (TableObj?, Error?) -> Void]()

    static var sharedInstance = Config()
    
    private init(){}
}

public protocol Packet {
    var description: String { get }
    var flags: Byte { get }
    var identifier: UInt16 { get }
}
public protocol Request: Packet {
    func write(writer: SocketWriter) throws
}
public protocol Response {
    var description: String { get }
    init(body: Data)
}

public enum RequestPacket {

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
        var flags: Byte = 0x00
        
        switch self {
        case .options                        : break
        case .execute                        : break
        case .query(let query)               : body.append(query.pack())
        case .prepare(let query)             : body.append(query.pack())
        case .authResponse(let token)        : body.append(token.data)
        case .startup(var options)           :
            options["CQL_VERSION"] = "3.0.0"
            
            body.append(UInt16(options.count).data)
            
            for (key, value) in options {
                body.append(key.data)
                body.append(value.data)
            }

        case .register(let events)  :

            body.append(events.count.data)

            for event in events {
                body.append(event.data)
            }

        case .batch(let queries, let Sflags, let consistency):
            
            for query in queries {
                //if withNames {}
                body.append(query.pack())
            }
            
            body.append(consistency.rawValue.data)
            
            if Sflags & 0x10 == 0x10 {
                body.append(Consistency.serial.rawValue.data)
            }
            if Sflags & 0x20 == 0x20 {
                //body.append() // optional timestamp
            }
            
            flags = Sflags
        }
        
        // Setup the Header

        var header = Data()
        header.append(config.version)
        header.append(flags)
        header.append(id.bigEndian.data)
        header.append(opcode)
        
        header.append(body.count.data)
        header.append(body)

        try writer.write(from: header)

    }
    
    case startup(options: [String: String])

    case options

    case query(query: Query)

    case prepare(query: Query)
    
    case execute(parameters: String)
    
    case register(events: [String])
    
    case batch(queries: [Query], flags: Byte, consistency: Consistency)
    
    case authResponse(token: Int)
}

public enum ResponsePacket: CustomStringConvertible {
    
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
        case .supported     : self = .supported(by: parseMap(body))
        case .result        : self = .result(of: ResultKind(body: body))
        case .authChallenge : self = .authChallenge(with: body.decodeInt)
        case .authenticate  : self = .authenticate(with: body.decodeString)
        case .error         : self = .error(code: body.decodeInt, message: body.decodeString)
        case .event         : self = parseEvent(body)
        }
    }

    case error(code: Int, message: String)

    case ready

    case authenticate(with: String)

    case supported(by: [String: [String]])

    case result(of: ResultKind)

    case authSuccess

    case event(of: EventType)

    case authChallenge(with: Int)
}

private func parseEvent(_ body: Data) -> ResponsePacket {
    var body = body

    switch body.decodeString {
    case "TOPOLOGY_CHANGE":
        let changeType = body.decodeString
        let inet       = body.decodeInet
        return .event(of: .topologyChange(type: changeType, inet: inet))
    case "STATUS_CHANGE":
        let changeType = body.decodeString
        let inet       = body.decodeInet
        return .event(of: .statusChange(type: changeType, inet: inet))
    case "SCHEMA_CHANGE":
        let changeType = body.decodeString
        let target     = body.decodeString
        
        if target == "KeySpace" {
            let options  = body.decodeString
            return .event(of: .schemaChange(type: changeType, target: target, changes: .options(with: options)))
        } else {
            let keyspace = body.decodeString
            let objName  = body.decodeString
            return .event(of: .schemaChange(type: changeType, target: target, changes: .keyspace(to: keyspace, withObjName: objName)))
        }
    default: return .event(of: .error)
    }
}

private func parseMap(_ body: Data) -> [String: [String]]{
    var body = body
    var map = [String: [String]]()
    
    for _ in 0..<Int(body.decodeUInt16) {
        let key = body.decodeString
        var strList = [String]()
        let strListLen = Int(body.decodeUInt16)
        
        for _ in 0..<strListLen {
            strList.append(body.decodeString)
        }
        
        map[key] = strList
    }
    return map
}
