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
    
    public static func insert(_ values: [Field: AnyObject], oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let vals = changeDictType(dict: values)
        
        //try execute(.query(using: .insert(into: Self.tableName, fields: mirror)), oncompletion: oncompletion)
    }
    
    public static func update(_ values: [Field: AnyObject], conditions: [Field: AnyObject], oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let vals = changeDictType(dict: values)

        let cond = changeDictType(dict: conditions)

        try execute(.query(using: .update(from: tableName, to: vals, with: cond)), oncompletion: oncompletion)
    }
    
    public static func delete(where conditions: [Field: AnyObject], oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let cond = changeDictType(dict: conditions)
        
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

public enum Query {

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
            
        case .insert(let table, let mirror):
            let keys = packKeys(mirror)
            let vals = packValues(mirror)
            data.append(("INSERT INTO \(table) ("+keys+") VALUES("+vals+");").sData)
            
        case .update(let table, let newValues, let conditions):
            let conds = packPairs(conditions)
            let vals  = packPairs(newValues)
            data.append(("UPDATE \(table) SET " + vals + " WHERE " + conds + ";").sData)
            
        case .delete(let table, let conditions):
            let conds = packPairs(conditions)
            data.append(("DELETE FROM \(table) WHERE " + conds + ";").sData)
    
        case .create(let table, let key, let mirror):
            data.append(("CREATE TABLE \(table)(" + packParams(key: key, mirror: mirror) + ");").sData)
    
        case .raw(let query):
            data.append(query.sData)
        }
        
        data.append(Consistency.one.rawValue.data)
        data.append(0x00.data)
        
        return data
    }
    
    case select(from: String, fields: [String])
    case insert(into: String, fields: Mirror)
    case update(from: String, to: [String: AnyObject], with: [String: AnyObject])
    case delete(from: String, conditions: [String: AnyObject])
    case create(table: String, key: String, fields: Mirror)
    case raw(String)
}
