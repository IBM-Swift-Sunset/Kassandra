//
//  Kassandra.swift
//  Kassandra
//
//  Created by Chia Huang on 7/26/16.
//
//

import Foundation
import Socket

public typealias Byte = UInt8
let CQL_MAX_SUPPORTED_VERSION: UInt8 = 0x03
let CQL_VERSION_STRINGS: [String] = ["3.0.0", "3.0.0", "3.0.0"]

public class Kassandra {
    
    private var socket: Socket?
    private var version: UInt8
    
    public init(socket: Socket?, version: UInt8) {
        self.socket = socket
        self.version = version
    }
    

    public func connect(host: String, port: Int32) throws {
        version = CQL_MAX_SUPPORTED_VERSION
        
        if socket == nil {
            socket = try! Socket.create(family: .inet6, type: .stream, proto: .tcp)
        }
        
        guard socket != nil else {
            throw RCErrorType.GenericError("Could not create a socket")
            return
        }
        do {
            while version >= 0x01 {
                try! socket?.connect(to: host, port: port)
                
            }
        } catch {
            print(error)
        }
    }
}
