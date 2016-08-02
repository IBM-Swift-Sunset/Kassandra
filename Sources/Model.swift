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

public var mirrors = [Int: Mirror]()

public extension Model {
    
    public var hashValue: Int {
        return Self.tableName.hashValue
    }

    private var mirror: Mirror {
        if mirrors[hashValue] == nil {
            mirrors[hashValue] = Mirror(reflecting: self)
        }
        return mirrors[hashValue]!
    }

    public func save() throws {
        try config.connection?.execute(.query(using: Query.insert(into: Self.tableName, fields: mirror))){
            res,err in
            
            for row in res!.rows {
                print(row["id"], row["name"], row["city"])
            }
        }
    }
    
    public func create(oncompletion: (TableObj?, Error?) -> Void) throws {

        let queryPacket = Request.query(using: .create(table: Self.tableName, key: String(Self.primaryKey), fields: mirror))
        try config.connection?.execute(queryPacket, oncompletion: oncompletion)
    }
}

func getType(_ item: Any ) -> DataType? {

    switch item {
    case _ as Int     : return .int
    case _ as String  : return .text
    case _ as Float   : return .float
    case _ as Double  : return .double
    case _ as Decimal : return .decimal
    default: return nil
    }
}

func packType(_ item: Any) -> String? {
    switch item {
    case let val as Int     : return String(val)
    case let val as String  : return "'\(val)'"
    case let val as Float   : return String(val)
    case let val as Double  : return String(val)
    case let val as Decimal : return String(val)
    default: return nil
    }
}
func packParams(key: String, mirror: Mirror) -> String {
    
    var str = ""
    for child in mirror.children {
        switch child.value {
        case is Int     : str += child.label! + " int "
        case is String  : str += child.label! + " text "
        case is Float   : str += child.label! + " float "
        case is Double  : str += child.label! + " double "
        case is Decimal : str += child.label! + " decimal "
        default: break
        }

        child.label! == key ? (str += "PRIMARY KEY,") : (str += ",")
    }
    return str
}

func packPairs(_ pairs: [String: AnyObject], mirror: Mirror? = nil) -> String {
    return pairs.map{key,val in  key + "=" + packType(val)! }.joined(separator: ", ")
}
func packValues(_ mirror: Mirror) -> String {
    return mirror.children.map{ packType($0.value)! }.joined(separator: ", ")
}

func packKeys(_ mirror: Mirror) -> String {
    return mirror.children.map { $0.label! }.joined(separator: ", ")
}
