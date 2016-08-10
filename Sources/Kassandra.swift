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
import SSLService

public class Kassandra {
    
    internal var socket: Socket?
    
    public var host: String = "localhost"
    public var port: Int32 = 9042
    
    public var delegate: KassandraDelegate? = nil

    internal var readQueue: DispatchQueue
    internal var writeQueue: DispatchQueue
    
    internal var buffer: Data
    
    internal var map            = [UInt16: (Kind?, Error?) -> Void]()
    internal var awaitingResult = [UInt16: (Error?) -> Void]()

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
            throw ErrorType.GenericError("Could not create a socket")

        }

        do {
            try sock.connect(to: host, port: port)

            try Request.startup(options: [:]).write(id: 10, writer: sock)
            
            config.connection = self

        } catch {
            oncompletion(ErrorType.ConnectionError)
            return
        }
        
        read()
        
        oncompletion(nil)
    }
    
    public func execute(_ query: String, oncompletion: ((Kind?, Error?) -> Void)) throws {
        let request = Request.query(using: Raw(query: query))
        try executeHandler(request, oncompletion: oncompletion)
    }

    public func execute(_ request: Request, oncompletion: ((Error?) -> Void)) throws {
        try executeHandler(request, oncompletionError: oncompletion)
    }

    public func execute(_ request: Request, oncompletion: ((Kind?, Error?) -> Void)) throws {
        try executeHandler(request, oncompletion: oncompletion)
    }
    private func executeHandler(_ request: Request, oncompletion: ((Kind?, Error?) -> Void)? = nil,
                                                    oncompletionError: ((Error?) -> Void)? = nil) throws {
        guard let sock = socket else {
            throw ErrorType.GenericError("Could not create a socket")
            
        }
        writeQueue.async {
            do {
                
                let id = UInt16.random
                
                if let oncomp = oncompletion { self.map[id] = oncomp }
                if let oncomp = oncompletionError { self.awaitingResult[id] = oncomp }

                try request.write(id: id, writer: sock)
                
            } catch {
                if let oncomp = oncompletion { oncomp(nil, ErrorType.ConnectionError) }
                if let oncomp = oncompletionError { oncomp(ErrorType.ConnectionError) }
            }
        }
    }
}

extension Kassandra {
    
    internal func read() {
        
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

            do { try handle(id: streamID, flags: flags, Response(opcode: opcode, body: body)) } catch {}
        
        }
    }
    private func handle(id: UInt16, flags: Byte, _ response: Response) throws {
        switch response {
        case .ready                         : awaitingResult[id]?(nil)
        case .authSuccess                   : awaitingResult[id]?(nil)
        case .event(let event)              : delegate?.didReceiveEvent(event: event)
        case .supported                     : print(response)
        case .authenticate(_)               : try Request.authResponse(token: 1).write(id: 1, writer: socket as! SocketWriter)
        case .authChallenge(let token)      : try Request.authResponse(token: token).write(id: 1, writer: socket as! SocketWriter)
        case .error(let code, let message)  : map[id]?(nil, ErrorType.CassandraError(Int(code), message))
        case .result(let resultKind)        : map[id]?(resultKind, nil)
        }
    }
}

extension Kassandra {
    public func setSSL(certPath: String? = nil, keyPath: String? = nil) throws {
        
        let SSLConfig = SSLService.Configuration(withCACertificateDirectory: nil, usingCertificateFile: certPath, withKeyFile: keyPath)
        
        config.SSLConfig = SSLConfig

        socket?.delegate = try SSLService(usingConfiguration: SSLConfig)
    }
    
    public func setSSL(with ChainFilePath: String, usingSelfSignedCert: Bool) throws {
        
        let SSLConfig = SSLService.Configuration(withChainFilePath: ChainFilePath, usingSelfSignedCerts: usingSelfSignedCert)
        
        config.SSLConfig = SSLConfig

        socket?.delegate = try SSLService(usingConfiguration: SSLConfig)
    }
    
    public func setSSL(with CACertificatePath: String?, using CertificateFile: String?, with KeyFile: String?, selfSignedCerts: Bool) throws {
        
        let SSLConfig = SSLService.Configuration(withCACertificateFilePath: CACertificatePath,
                                                 usingCertificateFile: CertificateFile,
                                                 withKeyFile: KeyFile,
                                                 usingSelfSignedCerts: selfSignedCerts)
        config.SSLConfig = SSLConfig

        socket?.delegate = try SSLService(usingConfiguration: SSLConfig)
    }
}

// Custom operators for database
extension Kassandra {
    subscript(_ database: String) -> Bool {
        do {
            let request = Raw(query: "USE \(database);")

            let r = Request.query(using: request)
            
            try r.write(id: 0, writer: socket as! SocketWriter)

        } catch {
            return false
        }
        return true
    }
    
}
