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

public protocol Table {
    associatedtype Field: Hashable
    static var tableName: String { get }
}

public extension Table {
    
    public static func select(_ fields: Field ..., oncompletion: (TableObj?, Error?) -> Void) throws {
        try execute(.query(using: .select(from: tableName,fields: fields.map{ String($0) })), oncompletion: oncompletion)
    }
    
    public static func insert(_ values: [Field: String], oncompletion: (TableObj?, Error?) -> Void) throws {
        
        var translated = [String: String]()
        
        for (key, value) in values {
            translated[String(key)] = value
        }
        
        try execute(.query(using: .insert(into: tableName, fields: translated)), oncompletion: oncompletion)
    }
    
    public static func update(_ values: [Field: String], conditions: [Field: String], oncompletion: (TableObj?, Error?) -> Void) throws {
        
        var vals = [String: String]()
        
        for (key, value) in values {
            vals[String(key)] = value
        }

        var cond = [String: String]()
        
        for (key, value) in conditions {
            cond[String(key)] = value
        }

        try execute(.query(using: .update(from: tableName, to: vals, with: cond)), oncompletion: oncompletion)
    }
    
    public static func delete(where conditions: [Field: String], oncompletion: (TableObj?, Error?) -> Void) throws {
        
        var cond = [String: String]()
        
        for (key, value) in conditions {
            cond[String(key)] = value
        }
        
        try execute(.query(using: .delete(from: tableName, conditions: cond)), oncompletion: oncompletion)
    }
    
    private static func execute(_ query: Request, oncompletion: (TableObj?, Error?) -> Void) throws {
        try config.connection?.execute(query) {
            table, error in
            
            if error != nil { oncompletion(nil, error) }
            else            { oncompletion(table, nil) }
        }
    }
}

public enum QueryTypes {

    var type: String {
        switch self {
        case .select: return "SELECT"
        case .insert: return "INSERT"
        case .update: return "UPDATE"
        case .delete: return "DELETE"
        case .create: return "CREATE"
        case .raw   : return "RAW"
        }
    }
    public func pack() -> Data {
        var data = Data()
        
        switch self {
        case .select(let table, let fields):
            
            fields.count == 0 ? data.append("SELECT * FROM \(table);".sData) :
                                data.append("SELECT \(fields.joined(separator: " ")) FROM \(table);".sData)
            
        case .insert(let table, let newValues):
            let keys = newValues.keys.map { "\($0)"}.joined(separator: ", ")
            let vals = newValues.values.map { "'\($0)'"}.joined(separator: ", ")
            data.append(("INSERT INTO \(table) ("+keys+") VALUES("+vals+");").sData)
            
        case .update(let table, let newValues, let conditions):
            let conds = conditions.map { "\($0)='\($1)'"}.joined(separator: ", ")
            let vals = newValues.map { "\($0)='\($1)'"}.joined(separator: ", ")
            data.append(("UPDATE \(table) SET " + vals + " WHERE " + conds + ";").sData)
            
        case .delete(let table, let conditions):
            let conds = conditions.map { "\($0)='\($1)'"}.joined(separator: ", ")
            data.append(("DELETE FROM \(table) WHERE " + conds + ";").sData)
    
        case .create(let table, let fields):
            data.append(("CREATE TABLE \(table)(" + fields + ");").sData)
    
        case .raw(let query):
            data.append(query.sData)
        }
        
        data.append(Consistency.one.rawValue.data)
        data.append(0x00.data)
        
        return data
    }
    private func packConditions(conds: [String]) -> String {
        
        return ""
    }
    
    case select(from: String, fields: [String])
    case insert(into: String, fields: [String: String])
    case update(from: String, to: [String: String], with: [String: String])
    case delete(from: String, conditions: [String: String])
    case create(table: String, fields: String)
    case raw(String)
}

// Struct Version -- This might be the way to go depending on the functionality needed
public protocol QueryType {
    func pack() -> Query
}
public struct Update: QueryType {
    
    let tableName: String
    
    let newValues: [String: String]
    let conditions: [String: String]
    
    init(to newValues: [String: String], in tableName: String, where condition: [String: String]) {
        self.newValues = newValues
        self.conditions = condition
        self.tableName = tableName
    }
    
    public func pack() -> Query {
        let conds = conditions.map { "\($0)='\($1)'"}.joined(separator: ", ")
        let vals = newValues.map { "\($0)='\($1)'"}.joined(separator: ", ")
        return Query("UPDATE \(tableName) SET " + vals + " WHERE " + conds + ";")
    }
}
public struct Delete: QueryType {
    //
    let tableName: String
    
    let conditions: [String: String]
    
    init(from tableName: String, where condition: [String: String]) {
        self.conditions = condition
        self.tableName = tableName
    }
    
    public func pack() -> Query {
        let conds = conditions.map { "\($0)='\($1)'"}.joined(separator: ", ")
        return Query("DELETE FROM \(tableName) WHERE " + conds + ";")
    }
}
public struct Insert: QueryType {
    
    let tableName: String
    
    let fields: [String: String]
    
    init(_ fields: [String: String], into tableName: String) {
        self.fields = fields
        self.tableName = tableName
    }
    
    public func pack() -> Query {
        let keys = fields.keys.map { "\($0)"}.joined(separator: ", ")
        let vals = fields.values.map { "'\($0)'"}.joined(separator: ", ")
        return Query("INSERT INTO \(tableName) ("+keys+") VALUES("+vals+");")
    }
}
public struct Select: QueryType {
    
    let tableName: String
    
    let fields: [String]
    
    init(_ fields: [String], from tableName: String) {
        self.fields = fields
        self.tableName = tableName
    }
    
    public func pack() -> Query {
        return fields.count == 0 ? Query("SELECT * from \(tableName);") :
            Query("SELECT \(fields.joined(separator: " ")) from \(tableName);")
    }
    func filter() {
        
    }
}
