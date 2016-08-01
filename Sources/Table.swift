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

public protocol Funcs {
    associatedtype Field: Hashable

    static func select(_ fields: Field ..., oncompletion: (TableObj?, Error?) -> Void) throws
    static func insert(_ values: [String: String], oncompletion: (TableObj?, Error?) -> Void) throws
    static func update(_ id: Field, oncompletion: (TableObj?, Error?) -> Void) throws
    static func delete(where condition: [String: String], oncompletion: (TableObj?, Error?) -> Void) throws
}

public protocol Table: Funcs {
    static var tableName: String { get }
}

public protocol Model: Funcs {
    static var tableName: String { get }
    static var primaryKey: Field { get }
}

public extension Model {
    
    public static func select(_ fields: Field ..., oncompletion: (TableObj?, Error?) -> Void) throws {
        let queryPacket = RequestPacket.query(query: Select(fields.map{ String($0) }, from: tableName).pack())
        try execute(queryPacket, oncompletion: oncompletion)
    }

    public static func insert(_ values: [String: String], oncompletion: (TableObj?, Error?) -> Void) throws {
        let queryPacket = RequestPacket.query(query: Insert(values, into: tableName).pack())
        try execute(queryPacket, oncompletion: oncompletion)
    }

    public static func update(_ values: [String: String], oncompletion: (TableObj?, Error?) -> Void) throws {
        let queryPacket = RequestPacket.query(query: Insert(values, into: tableName).pack())
        try execute(queryPacket, oncompletion: oncompletion)
    }

    public static func delete(where condition: [String: String], oncompletion: (TableObj?, Error?) -> Void) throws {
        let queryPacket = RequestPacket.query(query: Delete(from: tableName, where: condition).pack())
        try execute(queryPacket, oncompletion: oncompletion)
    }

    private static func execute(_ query: RequestPacket, oncompletion: (TableObj?, Error?) -> Void) throws {
        try config.connection?.execute(query) {
            table, error in
        
            if error != nil { oncompletion(nil, error) }
            else            { oncompletion(table, nil) }
        }
    }
}

public protocol QueryType {
    func pack() -> Query
}
public struct Update: QueryType {

    let tableName: String
    
    let newValues: [String: String]
    let condition: [String: String]
    
    init(to newValues: [String: String], in tableName: String, where condition: [String: String]) {
        self.newValues = newValues
        self.condition = condition
        self.tableName = tableName
    }
    
    public func pack() -> Query {
        //UPDATE emp SET emp_city='Delhi',emp_sal=50000 WHERE emp_id=2;
        return Query("DELETE from \(tableName) where emp_id=100;")
    }
}
public struct Delete: QueryType {
    //
    let tableName: String
    
    let condition: [String: String]
    
    init(from tableName: String, where condition: [String: String]) {
        self.condition = condition
        self.tableName = tableName
    }
    
    public func pack() -> Query {
        //DELETE emp_sal FROM emp WHERE emp_id=3;
        return Query("DELETE from \(tableName) where emp_id=100;")
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
        //Insert INTO emp (emp_id, emp_name) VALUES(100,'Aaron');
        return Query("Insert INTO emp (emp_id, emp_name) VALUES(100,'Aaron');")
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
