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

private let connection = config.connection

public protocol Table {
    associatedtype Field: Hashable
    static var tableName: String { get }

}

public extension Table {

    public typealias Document = [Field: Any]
    
    public static func count(_ fields: Field...) -> Select {
        
        let fields = fields.map{ String(describing: $0) }

        var query = Select(fields, from: Self.tableName)

        query.sqlfunction = SQLFunction.count(fields)

        return query
    }
    public static func select(_ fields: Field ...) -> Select {
        return Select(fields.map{ String(describing: $0) }, from: Self.tableName)
    }
    
    public static func insert(_ values: Document) -> Insert {
        return Insert(changeDictType(dict: values), into: Self.tableName)
    }
    
    public static func update(_ values: Document, conditions: Predicate) -> Update {
        
        let vals = changeDictType(dict: values)
        
        return Update(to: vals, in: Self.tableName, where: conditions)
    }
    
    public static func delete(where conditions: Predicate) -> Delete {
        
        return Delete(from: tableName, where: conditions)
    }
    
    public static func truncate() -> Raw {
        return Raw(query: "TRUNCATE TABLE \(Self.tableName)")
    }

    public static func drop() -> Raw {
        return Raw(query: "DROP TABLE \(Self.tableName)")
    }
    
    public static func created() -> Raw {
        return Raw(query: "CREATE TABLE \(Self.tableName)() VALUES()")
    }
}
