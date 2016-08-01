//
//  ResultKind.swift
//  Kassandra
//
//  Created by Aaron Liberatore on 7/29/16.
//
//

import Foundation

public struct Metadata {
    let flags: Int
    let columnCount: Int
    let keyspace: String?
    let table: String?
    let rowMetadata: [CqlColMetadata]?
    
    var isRowHeaderPresent: Bool {
        return flags & 0x0001 == 0x0001 ? false : true
    }
    
    var hasPagination: Bool {
        return flags & 0x0002 == 0x0002 ? false : true
    }

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
    return (data: data,
            meta: Metadata(flags: flags, count: columnCount, keyspace: globalKeySpace, table: globalTableName, rowMetadata: nil))
    
    
}

func parsePrepared(body: Data) -> ResultKind {
    var body = body

    let id = body.decodeUInt16
    
    let (data, meta) = parseMetadata(data: body)
    let metadata = meta
    
    let (_, resMeta) = parseMetadata(data: data)
    
    return ResultKind.prepared(id: id, metadata: metadata, resMetadata: resMeta)
}

func parseRows(body: Data) -> ResultKind {
    var (data, metadata) = parseMetadata(data: body)
    var columnHeaders = [(name: String, type: DataType)]()
    var rows = [[Data]]()
    
    for _ in 0..<metadata.columnCount {
        if metadata.isRowHeaderPresent {
            let _ = data.decodeString //ksname
            let _ = data.decodeString //tablename
        }
        let name = data.decodeString
        let id = data.decodeUInt16
        columnHeaders.append((name, DataType(rawValue: Int(id))!))
        
    }

    // Parse Row Content
    for _ in 0..<data.decodeInt {
        var cols = [Data]()
        for _ in 0..<metadata.columnCount {
            
            let length = data.decodeInt
            let value = data.subdata(in: Range(0..<length))
            
            //NOTE: Convert value to appropriate type here or leave as data?
            
            data = data.subdata(in: Range(length..<data.count))
            cols.append(value)
        }
        rows.append(cols)
    }
    return ResultKind.rows(metadata: metadata, columnTypes: columnHeaders, rows: rows)
}
