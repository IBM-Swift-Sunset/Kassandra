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

let CQL_MAX_SUPPORTED_VERSION: UInt8 = 0x03
let CQL_VERSION_STRINGS: [String] = ["3.0.0", "3.0.0", "3.0.0"]

public class Kassandra {
    
    private var socket: Socket?
    private var version: UInt8
    
    public var host: String = "localhost"
    public var port: Int32 = 9042
    
    private var readQueue: DispatchQueue
    private var writeQueue: DispatchQueue
    
    private var buffer: Data

    public init() {
        socket = nil
        version = 0x03
        
        buffer = Data()
        readQueue = DispatchQueue(label: "read queue", attributes: .concurrent)
        writeQueue = DispatchQueue(label: "write queue", attributes: .concurrent)
    }
    
    public func connect(oncompletion: (Error?) -> Void) throws {
        version = CQL_MAX_SUPPORTED_VERSION
        
        if socket == nil {
            socket = try! Socket.create(family: .inet6, type: .stream, proto: .tcp)
        }

        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")

        }

        do {
            try sock.connect(to: host, port: port)

            try Startup().write(writer: sock)

        } catch {
            oncompletion(RCErrorType.ConnectionError)
            return
        }
        
        read()
        
        oncompletion(nil)
    }
    public func query(query: String, oncompletion: (Error?) -> Void) throws {
        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")
            
        }
        do {
            
            try QueryRequest(query: Query(query: query)).write(writer: sock)
            
        } catch {
            oncompletion(RCErrorType.ConnectionError)
            return
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
                    self.unpack()
                }
                
                self.read()
            }
        }
    }

    public func unpack() -> [Frame]? {
        
        var messages = [Frame]()
        while buffer.count >= 9 {
            
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

            print("header:",version, flags, streamID, opcode, bodyLength)

            let body = buffer.subdata(in: Range(9..<9 + bodyLength))
            
            buffer = buffer.subdata(in: Range(9 + bodyLength..<buffer.count))
            
            let response = createResponseMessage(opcode: opcode, data: body)
            
            switch response {
            case let r as ErrorPacket: print(r.code, r.message)
            case let r as Result: print(r.type)
            default: print(response?.opcode)
            }

            messages.append(response!)
        
        }
        return messages
    }
}

extension Kassandra {
    public func createResponseMessage(opcode: UInt8, data: Data) -> Frame? {
        let opcode = Opcode(rawValue: opcode)!
        switch opcode {
        case .error: return ErrorPacket(body: data)
        case .startup: break
        case .ready: return Ready(body: data)
        case .authenticate: break
        case .options: break
        case .supported: break
        case .query: break
        case .result: return Result(body: data)
        case .prepare: break
        case .auth_success: break
        case .execute: break
        case .register: break
        case .event: break
        case .batch: break
        case .auth_challenge: break
        case .auth_response: break
        case .unknown: break
        }
        return nil
        
    }
}
