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
    
    private var map            = [UInt16: (TableObj?, Error?) -> Void]()
    private var awaitingResult = [UInt16: (Error?) -> Void]()

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

            try Request.startup(options: [:]).write(id: 10, writer: sock)
            
            config.connection = self

        } catch {
            oncompletion(RCErrorType.ConnectionError)
            return
        }
        
        read()
        
        oncompletion(nil)
    }
    
    public func execute(_ query: String, oncompletion: (TableObj?, Error?) -> Void) throws {
        let request = Request.query(using: Raw(query: query))
        try executeHandler(request, oncompletion: oncompletion)
    }
    internal func execute(_ request: Request, oncompletion: (Error?) -> Void) throws {
        try executeHandler(request, oncompletionError: oncompletion)
    }

    internal func execute(_ request: Request, oncompletion: (TableObj?, Error?) -> Void) throws {
        try executeHandler(request, oncompletion: oncompletion)
    }
    private func executeHandler(_ request: Request, oncompletion: ((TableObj?, Error?) -> Void)? = nil,
                                                    oncompletionError: ((Error?) -> Void)? = nil) throws {
        guard let sock = socket else {
            throw RCErrorType.GenericError("Could not create a socket")
            
        }
        writeQueue.async {
            do {
                
                let id = UInt16(random: true)
                
                if let oncomp = oncompletion { self.map[id] = oncomp }
                if let oncomp = oncompletionError { self.awaitingResult[id] = oncomp }
                
                try request.write(id: id, writer: sock)
                
            } catch {
                if let oncomp = oncompletion { oncomp(nil, RCErrorType.ConnectionError) }
                if let oncomp = oncompletionError { oncomp(RCErrorType.ConnectionError) }
            }
        }
    }
}

extension Kassandra {
    
    private func read() {
        
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

    private func unpack() {
        while buffer.count >= 9 {
            
            //Unpack header
            let flags       = UInt8(buffer[1])    // flags
            let streamID    = UInt16(msb: buffer[3], lsb: buffer[2])
            let opcode      = UInt8(buffer[4])
            let bodyLength  = Int(data: buffer.subdata(in: Range(5...8)))
            
            // Do we have all the bytes we need for the full packet?
            let bytesNeeded = buffer.count - bodyLength - 9
            
            if bytesNeeded < 0 {
                return
            }

            let body = buffer.subdata(in: Range(9..<9 + bodyLength))
            
            buffer = buffer.subdata(in: Range(9 + bodyLength..<buffer.count))

            do {
                try handle(id: streamID, flags: flags, Response(opcode: opcode, body: body))
            } catch {}
        
        }
    }
    private func handle(id: UInt16, flags: Byte, _ response: Response) throws {
        switch response {
        case .ready                         : awaitingResult[id]?(nil)
        case .authSuccess                   : awaitingResult[id]?(nil)
        case .event                         : print(response)
        case .supported                     : print(response)
        case .authenticate(_)               : try Request.authResponse(token: 1).write(id: 1, writer: socket!)
        case .authChallenge(let token)      : try Request.authResponse(token: token).write(id: 1, writer: socket!)
        case .error(let code, let message)  : map[id]?(nil, RCErrorType.CassandraError(Int(code), message))
        case .result(let resultKind)        :
            switch resultKind {
            case .void                  : break
            case .schema                : print(response)
            case .keyspace              : print(response)
            case .prepared              : print(response)
            case .rows(_, let r)        : map[id]?(TableObj(rows: r),nil)
            }
        }
    }
}

// Custom operators for database
extension Kassandra {
    subscript(_ database: String) -> Bool {
        do {
            let request = Raw(query: "USE \(database);")

            let r = Request.query(using: request)
            
            try r.write(id: 0, writer: socket!)

        } catch {
            return false
        }
        return true
    }
    
}
