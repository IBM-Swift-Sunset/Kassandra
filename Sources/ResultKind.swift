//
//  ResultKind.swift
//  Kassandra
//
//  Created by Aaron Liberatore on 7/29/16.
//
//

import Foundation

public protocol Kind {}

public struct Metadata {
    public var flags: Int
    
    var isRowHeaderPresent: Bool {
        return flags & 0x0001 == 0x0001 ? false : true
    }
    
    var hasPagination: Bool {
        return flags & 0x0002 == 0x0002 ? false : true
    }
    public var columnCount: Int
    public var keyspace: String?
    public var table: String?
    public var rowMetadata: [CqlColMetadata]?

    init(flags: Int, count: Int = 0, keyspace: String? = nil, table: String? = nil, rowMetadata: [CqlColMetadata]? = nil){
        self.flags = flags
        self.columnCount = count
        self.keyspace = keyspace
        self.table = table
        self.rowMetadata = rowMetadata
    }
}
public func parseMetadata(data: Data) -> (data: Data, meta: Metadata) {
    var data = data
    
    let flags = data.decodeInt
    let columnCount = data.decodeInt
    var globalKeySpace: String? = nil
    var globalTableName: String? = nil
    var pagingState = Data()
    
    if flags & 0x0001 == 0x0001 {
        globalKeySpace = data.decodeString
        globalTableName = data.decodeString
    } else {
        globalKeySpace = nil
        globalTableName = nil
    }
    
    if flags & 0x0002 == 0x0002 {
        // paging state [bytes] type
        let length = data.decodeInt
        pagingState = data.subdata(in: Range(0..<length))
        data = data.subdata(in: Range(length..<data.count))
    }
    if flags & 0x0004 == 0x0004 {
        return (data: data, meta: Metadata(flags: flags))
    }
    return (data: data, meta: Metadata(flags: flags, count: columnCount, keyspace: globalKeySpace, table: globalTableName, rowMetadata: nil))
    
    
}
public struct Rows: Kind {
    public var metadata: Metadata
    public var columnTypes = [(name: String, type: Options)]()
    public var rows = [[Data]]()
    
    init(data: Data){
        var (data, metadata) = parseMetadata(data: data)
        self.metadata = metadata
        
        // Get column names and the value type it holds | Doesn't handle custom value types
        var colHeaders = [(name: String, type: Options)]()

        for _ in 0..<metadata.columnCount {
            if metadata.isRowHeaderPresent {
                let _ = data.decodeString //ksname
                let _ = data.decodeString //tablename
            }
            let name = data.decodeString
            let id = data.decodeUInt16
            colHeaders.append((name, Options(rawValue: Int(id))!))
            
        }
        
        self.columnTypes = colHeaders
        
        // Parse Row Content
        for _ in 0..<data.decodeInt {
            var cols = [Data]()
            for col in 0..<metadata.columnCount {

                let length = data.decodeInt
                let value = data.subdata(in: Range(0..<length))
    
                //NOTE: Convert value to appropriate type here or leave as data?

                data = data.subdata(in: Range(length..<data.count))
                cols.append(value)
            }
            rows.append(cols)
        }
    }
    
    public func prettyPrint() {

        if !metadata.isRowHeaderPresent {
            print("Keyspace: \(metadata.keyspace!) ---- Table: \(metadata.table!)")
        }
        
        for i in 0..<columnTypes.count {
            if i == columnTypes.count - 1 {
                print("\(columnTypes[i].name)  |\t")
            } else {
                print("\(columnTypes[i].name)  |\t", terminator: "")
            }
            
        }
        for i in 0..<columnTypes.count {
            if i == columnTypes.count - 1 {
                print("="*12)
            } else {
                print("="*12, terminator: "")
            }
        }
        for row in rows {
            for i in 0..<columnTypes.count {
                var val = row[i]
                switch columnTypes[i].type {
                    case .int: print("\(val.decodeInt)  | \t", terminator: "")
                    case .text: print("\(val.decodeSDataString)  |\t", terminator: "")
                    case .varChar: print("\(val.decodeSDataString)  |\t", terminator: "")
                    default: print("unknown  |\t", terminator: "")
                }
            }
            print("\n")
        }
    }
}

func *(lhs: Character, rhs: Int) -> String {
    return String(repeating: lhs, count: rhs)
}
let s = " " * 5

public struct KeySpace: Kind {
    let name: String
}
public struct Prepared: Kind {
    public var id: UInt16
    public var metadata: Metadata?
    public var resultMetadata: Metadata?
    
    init(data: Data){
        var data = data

        id = data.decodeUInt16

        var (d, meta) = parseMetadata(data: data)
        metadata = meta
        
        (d, meta) = parseMetadata(data: d)
        resultMetadata = meta
        
        
        
    }
}
public struct SchemaChange: Kind {
    let change_type: String
    let target: String
    let options: String
}
