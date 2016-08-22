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
import Dispatch

public class Kassandra {
    
    public var delegate: KassandraDelegate? = nil

    internal var socket: Socket?

    internal var readQueue: DispatchQueue
    internal var writeQueue: DispatchQueue
    
    internal var buffer: Data
    
    internal var map = [UInt16: (Result) -> Void]()


    /**
         Initializes a Kassandra Instance
         
         - Parameters
            - host: String denoting the server host - defaults to localhost
            - port: Int32 denoting the server port  - defaults to 9042
         
         Returns a QueryResult Enum through the callback
     
     */
    public init(host: String = "localhost", port: Int32 = 9042) {

        config.setHostAndPort(host: host, port: port)

        socket = nil
        
        buffer = Data()

        readQueue = DispatchQueue(label: "read queue", attributes: DispatchQueue.Attributes.concurrent)
        writeQueue = DispatchQueue(label: "write queue", attributes: DispatchQueue.Attributes.concurrent)
    }
 

    /**
         Connects to a Kassandra Server
         
         - Parameters
            - onCompletion: Callback function for on completion
         
         Returns a Optional Error through the given callback
     
     */
    public func connect(oncompletion: @escaping (Result) -> Void) throws {
        
        if socket == nil {
            socket = try! Socket.create(family: .inet6, type: .stream, proto: .tcp)
        }

        guard let sock = socket else {
            throw ErrorType.GenericError("Could not create a socket")

        }

        do {
            try sock.connect(to: config.host, port: config.port)
            
            let id = UInt16.random
            
            self.map[id] = oncompletion

            try Request.startup(options: [:]).write(id: id, writer: sock)
            
            config.connection = self

        } catch {
            oncompletion(Result.error(ErrorType.ConnectionError))
            return
        }
        
        read()

    }
 

    /**
         Executes a String Representation of a CSQL Query
         
         - Parameters
            - query:        String Representation of a CSQL Query to be executed
            - onCompletion: Callback function for on completion
         
         Returns a QueryResult Enum through the given callback
     */
    public func execute(_ query: String, oncompletion: @escaping ((Result) -> Void)) {
        let request = Request.query(using: Raw(query: query))
        executeHandler(request, oncompletion: oncompletion)
    }


    /**
         Executes a String Representation of a CSQL Query
         
         - Parameters
            - query:        String Representation of a CSQL Query to be executed
            - onCompletion: Callback function for on completion
         
         Returns a QueryResult Enum through the given callback
     */
    public func execute(_ request: Request, oncompletion: @escaping ((Result) -> Void)) {
        executeHandler(request, oncompletion: oncompletion)
    }


    /**
         Executes a String Representation of a CSQL Query
         
         - Parameters
            - query:        String Representation of a CSQL Query to be executed
            - onCompletion: Callback function for on completion
         
         Returns a QueryResult Enum through the given callback
     */
    public func execute(_ query: Query, oncompletion: @escaping ((Result) -> Void)) {
        executeHandler(.query(using: query), oncompletion: oncompletion)
    }

    private func executeHandler(_ request: Request, oncompletion: @escaping ((Result) -> Void)) {
        guard let sock = socket else {
            oncompletion(Result.error(ErrorType.GenericError("Socket Not Connected")))
            return
        }
        writeQueue.async {
            do {
                
                let id = UInt16.random
                
                self.map[id] = oncompletion

                try request.write(id: id, writer: sock)
                
            } catch {
                oncompletion(.error(ErrorType.ConnectionError))
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
            let flags       = UInt8(buffer[1])
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
        case .ready                         : map[id]?(.void)
        case .authSuccess                   : map[id]?(.void)
        case .event(let event)              : delegate?.didReceiveEvent(event: event)
        case .supported(let options)        : map[id]?(.generic(options))
        case .authenticate(_)               : try Request.authResponse(token: 1).write(id: 1, writer: socket as! SocketWriter)
        case .authChallenge(let token)      : try Request.authResponse(token: token).write(id: 1, writer: socket as! SocketWriter)
        case .error(let code, let message)  : map[id]?(.error(ErrorType.CassandraError(Int(code), message)))
        case .result(let resultKind)        : map[id]?(.kind(resultKind))
        }
    }
}

extension Kassandra {

    /**
         Initialize a configuration using a `CA Certificate` directory.

         *Note:* `caCertificateDirPath` - All certificates in the specified directory **must** be hashed using the `OpenSSL Certificate Tool`.

         - Parameters:
            - caCertificateDirPath:		Path to a directory containing CA certificates. *(see note above)*
            - certificateFilePath:		Path to the PEM formatted certificate file. If nil, `certificateFilePath` will be used.
            - keyFilePath:				Path to the PEM formatted key file (optional). If nil, `certificateFilePath` is used.
            - selfSigned:				True if certs are `self-signed`, false otherwise. Defaults to true.

     */

    public func setSSL(certPath: String? = nil, keyPath: String? = nil) throws {
        
        let SSLConfig = SSLService.Configuration(withCACertificateDirectory: nil, usingCertificateFile: certPath, withKeyFile: keyPath)
        
        config.SSLConfig = SSLConfig

        socket?.delegate = try SSLService(usingConfiguration: SSLConfig)
    }

    /**
         Initialize a configuration using a `Certificate Chain File`.

         *Note:* If using a certificate chain file, the certificates must be in PEM format and must be sorted starting with the subject's certificate (actual client or server certificate), followed by intermediate CA certificates if applicable, and ending at the highest level (root) CA.

         - Parameters:
            - chainFilePath:			Path to the certificate chain file (optional). *(see note above)*
            - selfSigned:				True if certs are `self-signed`, false otherwise. Defaults to true.

     */
    public func setSSL(with ChainFilePath: String, usingSelfSignedCert: Bool) throws {
        
        let SSLConfig = SSLService.Configuration(withChainFilePath: ChainFilePath, usingSelfSignedCerts: usingSelfSignedCert)
        
        config.SSLConfig = SSLConfig

        socket?.delegate = try SSLService(usingConfiguration: SSLConfig)
    }

    /**
         Initialize a configuration using a `CA Certificate` file.
         
         - Parameters:
            - caCertificateFilePath:	Path to the PEM formatted CA certificate file.
            - certificateFilePath:		Path to the PEM formatted certificate file.
            - keyFilePath:				Path to the PEM formatted key file. If nil, `certificateFilePath` will be used.
            - selfSigned:				True if certs are `self-signed`, false otherwise. Defaults to true.

     */
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
