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
    public var columnCount: Int?
    public var keyspace: String?
    public var table: String?
    public var rowMetadata: [CqlColMetadata]?
    
    init(flags: Int, count: Int? = nil, keyspace: String? = nil, table: String? = nil, rowMetadata: [CqlColMetadata]? = nil){
        self.flags = flags
        self.columnCount = count
        self.keyspace = keyspace
        self.table = table
        self.rowMetadata = rowMetadata
    }
}

public struct Rows: Kind {
    public var metadata: Metadata?
    public var rows = [[AnyObject]]()
    
    public let globalKeySpace: String?
    public let globalTableName: String?
    
    init(data: Data){
        var data = data
        var isRowHeaderPresent = true
        
        let flags = data.decodeInt
        let columnCount = data.decodeInt
        
        var pagingState = Data()
        
        if flags & 0x0001 == 0x0001 {
            globalKeySpace = data.decodeString
            globalTableName = data.decodeString
            isRowHeaderPresent = false
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
            metadata = Metadata(flags: flags)
            return
        }
        
        var columnTypes = [(name: String, type: Options)]()
        
        // Get column names and the value type it holds | Doesn't handle custom value types
        for _ in 0..<columnCount {
            if isRowHeaderPresent {
                let _ = data.decodeString //ksname
                let _ = data.decodeString //tablename
            }
            let name = data.decodeString
            let id = data.decodeUInt16
            columnTypes.append((name, Options(rawValue: Int(id))!))
            
        }
        
        // Parse Row Content
        for _ in 0..<data.decodeInt {
            var cols = [AnyObject]()
            for col in 0..<columnCount {
                let x = data.decodeInt
                let value = data.subdata(in: Range(0..<x))
                data = data.subdata(in: Range(x..<data.count))
                cols.append(value)
            }
            rows.append(Row(cols: cols))
        }
    }
}
public struct KeySpace: Kind {
    let name: String
}
public struct Prepared: Kind {
    public var metadata: Metadata?
    public var rows: [[AnyObject]]
    
    init(data: Data){
        metadata = nil
        rows = []
    }
}
public struct SchemaChange: Kind {
    let change_type: String
    let target: String
    let options: String
}
