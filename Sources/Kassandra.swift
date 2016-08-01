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

public class Kassandra {
    
    private var socket: Socket?
    
    public var host: String = "localhost"
    public var port: Int32 = 9042
    
    private var readQueue: DispatchQueue
    private var writeQueue: DispatchQueue
    
    private var buffer: Data

    public init(host: String = "localhost", port: Int32 = 9042) {
        self.host = host
        self.port = port

        socket = nil
        
        buffer = Data()

        readQueue = DispatchQueue(label: "read queue", attributes: .concurrent)
        writeQueue = DispatchQueue(label: "write queue", attributes: .concurrent)
    }
    
    public func connect(oncompletion: (Error?) -> Void) throws {
        
        if socket == nil {
            socket = try! Socket.create(family: .inet6, type: .stream, proto: .tcp)
        }

        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")

        }

        do {
            try sock.connect(to: host, port: port)

            //try RequestPacket.startup(identifier: 10, options: [:]).write(writer: sock)
            try Startup().write(writer: sock)
        } catch {
            oncompletion(RCErrorType.ConnectionError)
            return
        }
        
        read()
        
        oncompletion(nil)
    }

    private func authResponse(token: Int) {

        guard let sock = socket else {
            print(RCErrorType.GenericError("Could not create a socket"))
            return
        }

        writeQueue.async {
            do {
                try AuthResponse(token: token).write(writer: sock)
                
            } catch {
                print("error")
            }
        }
    }

    public func options(oncompletion: (Error?) -> Void) throws {

        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")
            
        }

        writeQueue.async {
            do {
                try OptionsRequest().write(writer: sock)
                
            } catch {
                oncompletion(RCErrorType.ConnectionError)
            }
        }
    }

    public func query(query: String, oncompletion: (Error?) -> Void) throws {

        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")
            
        }
        writeQueue.async {
            do {
                try QueryRequest(query: Query(query)).write(writer: sock)
                
            } catch {
                oncompletion(RCErrorType.ConnectionError)
            }
        }
    }

    public func prepare(query: String, oncompletion: (Error?) -> Void) throws {
        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")
            
        }
        writeQueue.async {
            do {
                try Prepare(query: Query(query)).write(writer: sock)
                
            } catch {
                oncompletion(RCErrorType.ConnectionError)
                
            }
        }
    }

    public func execute(id: UInt16, parameters: String, oncompletion: (Error?) -> Void) throws {
        
        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")
            
        }
        
        writeQueue.async {
            do {
                try Execute(id: id, parameters: parameters).write(writer: sock)
                
            } catch {
                oncompletion(RCErrorType.ConnectionError)
            }
        }
    }
    
    public func batch(query: [String], oncompletion: (Error?) -> Void) throws {
        
        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")
            
        }
        
        let queries = query.map {
            str in
            
            return Query(str)
        }
        
        writeQueue.async {
            do {
                try Batch(queries: queries).write(writer: sock)
                
            } catch {
                oncompletion(RCErrorType.ConnectionError)
            }
        }
    }

    public func register(events: [String], oncompletion: (Error?) -> Void) throws {

        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")
            
        }
        
        writeQueue.async {
            do {
                try Register(events: events).write(writer: sock)
                
            } catch {
                oncompletion(RCErrorType.ConnectionError)
            }
        }
    }
}

extension Kassandra {
    
    public func read() {
        
        guard let sock = socket else {
            return
        }
        
        let iochannel = DispatchIO(type: DispatchIO.StreamType.stream, fileDescriptor: sock.socketfd, queue: readQueue, cleanupHandler: {
            error in
        })
        
        iochannel.read(offset: off_t(0), length: 1, queue: readQueue) {
            done, data, error in
            
            let bytes: [Byte]? = data?.map {
                byte in
                return byte
            }
            
            if let d = bytes {
                
                self.buffer.append(d, count: d.count)

                if self.buffer.count >= 9 {
                    let _ = self.unpack()
                }
                
                self.read()
            }
        }
    }

    public func unpack() -> [Response]? {
        
        var messages = [Response]()
        while buffer.count >= 9 {
            
            //Unpack header
            let version = UInt8(buffer[0])
            let flags = UInt8(buffer[1])
            let streamID = UInt16(msb: buffer[2], lsb: buffer[3])
            let opcode = UInt8(buffer[4])
            let bodyLength = Int(data: buffer.subdata(in: Range(5...8)))
            
            // Do we have all the bytes we need for the full packet?
            let bytesNeeded = buffer.count - bodyLength - 9
            
            if bytesNeeded < 0 {
                return nil
            }

            let body = buffer.subdata(in: Range(9..<9 + bodyLength))
            
            buffer = buffer.subdata(in: Range(9 + bodyLength..<buffer.count))
            
            /*
             Struct packet version
             guard let response: Response = createResponseMessage(opcode: opcode, data: body) else {
                continue
            }
            
            response.version = version
            response.flags = flags
            response.streamID = streamID
            handle(response)
            messages.append(response)*/

            handle(ResponsePacket(opcode: opcode, body: body))
        
        }
        return messages
    }
    public func handle(_ response: ResponsePacket) {
        switch response {
        case .ready                     : break
        case .authSuccess               : break
        case .event                     : print(response)
        case .error                     : print(response)
        case .authChallenge(let token)  : authResponse(token: token)
        case .authenticate(_)           : authResponse(token: 1)
        case .supported                 : print(response)
        case .result(let resultKind)    :
            switch resultKind {
            case .void                  : break
            case .rows                  : print(response)
            case .schema                : print(response)
            case .keyspace              : print(response)
            case .prepared              : print(response)
            }
        }
    }
}
// Struct version
extension Kassandra {
    public func createResponseMessage(opcode: UInt8, data: Data) -> Response? {
        let opcode = Opcode(rawValue: opcode)!
        switch opcode {
        case .error:        return ErrorPacket(body: data)
        case .ready:        return Ready(body: data)
        case .authenticate: return Authenticate(body: data)
        case .supported:    return Supported(body: data)
        case .result:       return Result(body: data)
        case .authSuccess:  return AuthSuccess(body: data)
        case .event:        return Event(body: data)
        case .authChallenge:return AuthChallenge(body: data)
        default: return nil
        }
    }
    public func handle(_ response: Response) {
        switch response {
        case let r as ErrorPacket   : print(r.description)
        case _ as Authenticate      : authResponse(token: 1)
        case let r as Supported     : print(r.description)
        case let r as Event         : print(r.description)
        case let r as AuthChallenge : self.authResponse(token: r.token)
        case let r as Result        :
            switch r.message {
            case .void                                      : break
            case .rows(let m, let c, let r)                 : prettyPrint(metadata: m, columnTypes: c, rows: r)
            case .schema(let type, let target, let options) : print(type, target, options)
            case .keyspace(let name)                        : print(name)
            case .prepared(let id, _, _)                    : print(id)
            }
        default                     : print(response.description)
        }
    }
}













// Custom operators for database
extension Kassandra {
    subscript(_ database: String) -> Bool {
        do {
            try query(query: "USE \(database);") { _ in
                
            }
        } catch {
            return false
        }
        return true
    }
    
}



public func prettyPrint(metadata: Metadata, columnTypes: [(name: String, type: DataType)], rows: [[Data]]) -> String {
    
    var str = ""
    
    if !metadata.isRowHeaderPresent {
        str += "Keyspace: \(metadata.keyspace!) ---- Table: \(metadata.table!)\n"
    }
    
    for i in 0..<columnTypes.count {
        if i == columnTypes.count - 1 {
            str += "\(columnTypes[i].name)  |\t\n"
        } else {
            str += "\(columnTypes[i].name)  |\t"
        }
        
    }
    for i in 0..<columnTypes.count {
        if i == columnTypes.count - 1 {
            str += String(repeating: "=".characters.first!, count: 12)
            str += "\n"
        } else {
           str += String(repeating: "=".characters.first!, count: 12)
        }
    }
    for row in rows {
        for i in 0..<columnTypes.count {
            var val = row[i]
            switch columnTypes[i].type {
            case .int: str += "\(val.decodeInt)  | \t"
            case .text: str += "\(val.decodeSDataString)  |\t"
            case .varChar: str += "\(val.decodeSDataString)  |\t"
            default: str += "unknown  |\t"
            }
        }
        str += "\n"
    }
    return str
}
