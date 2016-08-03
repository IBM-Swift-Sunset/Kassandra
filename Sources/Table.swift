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

    public static func select(_ fields: Field ..., oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let request: Request = .query(using: Select(fields.map{ String($0) }, from: Self.tableName))

        try connection?.execute(request, oncompletion: oncompletion)
    }

    public static func count(fields: [Field]? = nil, matching: Document? = nil, oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let request: Request = .query(using: Select(fields!.map{ String($0) }, from: Self.tableName))
        
        try connection?.execute(request, oncompletion: oncompletion)
    }

    public static func insert(_ values: Document, oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let values = changeDictType(dict: values)

        let request = Request.query(using: Insert(values, into: Self.tableName))

        try connection?.execute(request, oncompletion: oncompletion)
    }
    
    public static func update(_ values: Document, conditions: Document, oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let vals = changeDictType(dict: values)

        let cond = changeDictType(dict: conditions)
        
        let request = Request.query(using: Update(to: vals, in: Self.tableName, where: cond))

        try connection?.execute(request, oncompletion: oncompletion)
    }
    
    public static func delete(where conditions: Document, oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let cond = changeDictType(dict: conditions)
        
        let request = Request.query(using: Delete(from: tableName, where: cond))

        try connection?.execute(request, oncompletion: oncompletion)
    }
}
