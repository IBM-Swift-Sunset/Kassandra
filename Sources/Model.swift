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

public protocol Model {
    associatedtype Field: Hashable
    
    static var tableName: String { get }
    static var primaryKey: Field { get }
    
    var setPrimaryKey: Int? { get set }
    
    var serialize: [Field: AnyObject] { get }
    
    init(row: Row)
}

public extension Model {
    
    public func save() throws {
        
    }
    
    public func create(oncompletion: (TableObj?, Error?) -> Void) throws {
        
        let key = String(Self.primaryKey) + " int PRIMARY KEY, "
        let params = serialize.keys.map{ String($0) + " text" }.joined(separator: ", ")
        
        let queryPacket = Request.query(using: .create(table: Self.tableName, fields: key + params))
        
        try config.connection?.execute(queryPacket) {
            table, error in
            
            if error != nil { oncompletion(nil, error) }
            else            { oncompletion(table, nil) }
        }
    }
}
