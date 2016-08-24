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

public typealias Row = [String: Any]
/**
    Defines a Model Instance
 
    - Requirements
        - Field:            Associated Type for table Fields -> Use an Enum
        - tableName:        Name of the table being modelled
        - primaryKey:       Primary key of the table
        - key:              Getter and Setter of the table key
        - init(row: Row):   Initializer for the model given a [String, Any]
 
 */
public protocol Model: Table {
    associatedtype Field: Hashable
    
    static var tableName: String { get }
    static var primaryKey: Field { get }

    var key: UUID? { get set }

    init(row: Row)
}

public extension Model {

    
    /**
        Creates an Insert Query representing field values in the Model as a Row
         
        Returns the Insert Query
     
     */
    public func save() {
        let values: [String: Any] =  Mirror(reflecting: self).children.reduce([:]) { acc, child in
            var ret = acc
            ret[child.label!] = child.value
            return ret
        }
        
        Insert(values, into: Self.tableName).execute() {
            result in
            
            print(result)
        }
    }

    
    /**
        Creates a Delete Query to the row being modelled
     
        Returns the Delete Query
     
     */
    public func delete() {
        Delete(from: Self.tableName, where: "id" == key!)
    }

    
    /**
        Creates the table represesented by the Model
     
        - Parameters:
            - onCompeletion:    Closure for Result Callback

        Returns the result of the query through the given callback
     
     */
    public func create(ifNotExists: Bool = false, oncompletion: @escaping ((Result)->Void)) throws {

        let vals = packColumnData(key: String(describing: Self.primaryKey), mirror: Mirror(reflecting: self))

        Raw(query: "CREATE TABLE \(ifNotExists ? "IF NOT EXISTS" : "") \(Self.tableName)(\(vals));").execute(oncompletion: oncompletion)
    }

    
    /**
         Fetches the Rows of the Modelled Table
         
         - Parameters:
            - fields:           Array representing the fields to be selected
            - onCompeletion:    Closure for Result Callback
         
         Returns the result as an optional array of the model and optional error through the given callback
     */
    public static func fetch(_ fields: [Field] = [], oncompletion: @escaping (([Self]?, Error?)->Void)) {
        
        Select(fields.map{ String(describing: $0) }, from: tableName).execute() {
            result in
            
            if let err = result.asError { oncompletion(nil, err)}
            if let rows = result.asRows {
                oncompletion(rows.map { Self.init(row: $0) }, nil)
            }
        }
    }
}
