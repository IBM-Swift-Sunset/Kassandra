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

public protocol Query {
    func pack() -> Data
}

//private typealias Field: String

extension Query {

    public func execute() throws -> Promise<TableObj> {
        let p = Promise<TableObj>.deferred()
    
        let request: Request = .query(using: self)
        
        try config.connection?.execute(request) {
            result, error in
            
            if let error = error { p.reject(dueTo: error) }
            if let res = result { p.resolve()(res) }
        }

        return p
    }

    public func execute(oncompletion: (Error?) -> Void) throws {
        
        let request: Request = .query(using: self)
        
        try config.connection?.execute(request, oncompletion: oncompletion)
    }
}

public enum Order: String {
    case ASC = "ASC"
    case DESC = "DESC"
}
public enum SQLFunction<T> {
    case max([T])
    case min([T])
    case avg([T])
    case sum([T])
    case count([T])
    
    func pack() -> String {
        switch self {
        case .max(let args)     : return args.count == 0 ? "MAX(*)" : "MAX(\(args.map{ String($0) }.joined(separator: ", ")))"
        case .min(let args)     : return args.count == 0 ? "MIN(*)" : "MIN(\(args.map{ String($0) }.joined(separator: ", ")))"
        case .avg(let args)     : return args.count == 0 ? "AVG(*)" : "AVG(\(args.map{ String($0) }.joined(separator: ", ")))"
        case .sum(let args)     : return args.count == 0 ? "SUM(*)" : "SUM(\(args.map{ String($0) }.joined(separator: ", ")))"
        case .count(let args)   : return args.count == 0 ? "COUNT(*)" : "COUNT(\(args.map{ String($0) }.joined(separator: ", ")))"
        }
    }
}

public struct Select: Query {
    
    let tableName: String
    
    let fields: [String]

    var order: [String: Order]? = nil
    
    var limitResultCount: Int? = nil
    
    var sqlfunction: SQLFunction<String>? = nil

    init(_ fields: [String], from tableName: String) {
        self.fields = fields
        self.tableName = tableName
    }
    
    private mutating func order(by predicate: [String: Order]) {
        order = predicate
    }

    public func ordered(by predicate: [String: Order]) -> Select {
        var new = self
        new.order(by: predicate)
        return new
    }

    private mutating func limit(to newLimit: Int) {
        limitResultCount = newLimit
    }

    public func limited(to newLimit: Int) -> Select {
        var new = self
        new.limit(to: newLimit)
        return new
    }

    public func filter(by: [String: Any]) -> Select {
        return self
    }

    public func pack() -> Data {
        var data = Data()
        
        data.append(buildQueryString.sData)
        data.append(Consistency.one.rawValue.data)
        data.append(0x00.data)
        
        return data
    }

    private var buildQueryString: String {
        var str = "SELECT "
        
        if let function = sqlfunction?.pack() {
            fields.count == 0 ? (str += "\(function) FROM \(tableName)") :
                (str += "\(function), \(fields.joined(separator: " ")) FROM \(tableName)")
        } else {
            fields.count == 0 ? (str += "* FROM \(tableName)") :
                (str += "\(fields.joined(separator: " ")) FROM \(tableName)")
        }
        
        
        if let order = order {
            str += " ORDER BY " + order.map {key, val in "\(key) \(val.rawValue)" }.joined(separator: ", ")
        }
        if let limit = limitResultCount {
            str += " LIMIT \(limit)"
        }
        
        return str + ";"
        
    }
}
public struct Update: Query {
    
    let tableName: String
    
    let newValues: [String: Any]
    var conditions: [String: Any]
    
    init(to newValues: [String: Any], in tableName: String, where predicate: [String: Any]) {
        self.newValues = newValues
        self.tableName = tableName
        self.conditions = predicate
    }
    
    public mutating func filter(by predicate: [String: Any]){
        conditions = predicate
    }

    public func pack() -> Data {
        var data = Data()

        let vals  = packPairs(newValues)
        let conds = packPairs(conditions)

        data.append(("UPDATE \(tableName) SET \(vals) WHERE \(conds);").sData)
        

        data.append(Consistency.one.rawValue.data)
        data.append(0x00.data)
        
        return data
    }
    
}
public struct Delete: Query {

    let tableName: String
    
    let conditions: [String: Any]
    
    init(from tableName: String, where condition: [String: Any]) {
        self.conditions = condition
        self.tableName = tableName
    }
    
    public func pack() -> Data {
        var data = Data()

        let conds = packPairs(conditions)
        
        data.append(("DELETE FROM \(tableName) WHERE \(conds);").sData)
        data.append(Consistency.one.rawValue.data)
        data.append(0x00.data)
        
        return data
    }
}
public struct Insert: Query {
    
    let tableName: String
    
    let fields: [String: Any]
    
    init(_ fields: [String: Any], into tableName: String) {
        self.fields = fields
        self.tableName = tableName
    }
    
    /*init(mirror: Mirror, into tableName: String) {
        
    }*/
    
    public func pack() -> Data {
        var data = Data()
    
        let keys = packKeys(fields)
        let vals = packValues(fields)
        
        data.append(("INSERT INTO \(tableName) (\(keys)) VALUES(\(vals));").sData)
        data.append(Consistency.one.rawValue.data)
        data.append(0x00.data)
        
        return data
    }
}
public struct Raw: Query {
    let query: String
    
    public func pack() -> Data {
        var data = Data()
    
        data.append(query.sData)
        data.append(Consistency.one.rawValue.data)
        data.append(0x00.data)
        
        return data
    }
}
