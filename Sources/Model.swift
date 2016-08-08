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

public protocol Model: Table {
    associatedtype Field: Hashable
    
    static var tableName: String { get }
    static var primaryKey: Field { get }

    var key: Int? { get set }

    init(row: Row)
}

internal var mirrors = [Int: Mirror]()

public extension Model {

    public var hashValue: Int {
        return key ?? -1
    }

    private var mirror: Mirror {
        if mirrors[hashValue] == nil {
            mirrors[hashValue] = Mirror(reflecting: self)
        }
        return mirrors[hashValue]!
    }

    public func save() -> Promise<Self> {
        let p = Promise<Self>.deferred()
        
        let values: [String: Any] = mirror.children.reduce([:]) { acc, child in
            var ret = acc
            ret[child.label!] = child.value
            return ret
        }
        Insert(values, into: Self.tableName).execute()
            .then {
                table in
                print(table)
                    p.resolve()( Self.init(row: table.rows.first!) )
            
            }
            .fail {
                error in
                p.reject(dueTo: error)
                //p.resolve()()
            }
        
        return p
    }
    
    public func delete() -> Promise<Self> {
        let p = Promise<Self>.deferred()

        Delete(from: Self.tableName, where: ["id": key!]).execute()
            .then {
                table in
                p.resolve()( Self.init(row: table.rows[0]) )
                
            }.fail {
                error in
                p.reject(dueTo: error)
        }
        return p
    }
    
    public func create() throws {

        let values: [String: Any] = mirror.children.reduce([:]) { acc, child in
            var ret = acc
            ret[child.label!] = child.value
            return ret
        }
 
        let vals = packColumnData(key: String(describing: Self.primaryKey), mirror: mirror)

        try Raw(query: "CREATE TABLE \(Self.tableName)(\(vals));").execute {
            (err: Error?) in
            
            if err != nil { return }
            else {
                do {
                    try Insert(values, into: Self.tableName).execute { (err: Error?) in }
                } catch {
                    
                }
            }
        }
    }
    
    public static func fetch(_ fields: [Field] = []) -> Promise<[Self]> {
        let p = Promise<[Self]>.deferred()

        Select(fields.map{ String(describing: $0) }, from: tableName).execute()
            .then {
                table in
                    p.resolve()( table.rows.map { Self.init(row: $0) } )

            }.fail {
                error in
                p.reject(dueTo: error)
                //p.resolve()([])
            }
        return p
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
    case let val as Int     : return String(describing: val)
    case let val as String  : return "'\(val)'"
    case let val as Float   : return String(describing: val)
    case let val as Double  : return String(describing: val)
    case let val as Decimal : return String(describing: val)
    case let val as Bool    : return String(describing: val)
    default: return nil
    }
}
func packColumnData(key: String, mirror: Mirror) -> String {
    
    var str = ""
    for child in mirror.children {
        switch child.value {
        case is Int     : str += child.label! + " int "
        case is String  : str += child.label! + " text "
        case is Float   : str += child.label! + " float "
        case is Double  : str += child.label! + " double "
        case is Decimal : str += child.label! + " decimal "
        case is Bool    : str += child.label! + " bool "
        default: break
        }

        child.label! == key ? (str += "PRIMARY KEY,") : (str += ",")
    }
    return str
}

func packPairs(_ pairs: [String: Any], mirror: Mirror? = nil) -> String {
    return pairs.map{key,val in  key + "=" + packType(val)! }.joined(separator: ", ")
}
func packKeys(_ dict: [String: Any]) -> String {
    return dict.map {key, value in key }.joined(separator: ", ")
}
func packKeys(_ mirror: Mirror) -> String {
    return mirror.children.map { $0.label! }.joined(separator: ", ")
}
func packValues(_ dict: [String: Any]) -> String {
    return dict.map {key, value in packType(value)! }.joined(separator: ", ")
}
func packValues(_ mirror: Mirror) -> String {
    return mirror.children.map{ packType($0.value)! }.joined(separator: ", ")
}
