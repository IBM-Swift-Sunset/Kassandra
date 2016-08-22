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

extension Array where Element: Query {
    
    /**
        Convienence Function to excute batch queries from arrays
     
        Parameters:
            - type:         Batch Type of the operation
            - consis:       Consistency for the operation
            - oncompletion: Callback Closure

        Returns a Result type through the given callback
    */
    func execute(with type: BatchType, consis: Consistency, oncompletion: @escaping ((Result)->Void)) {
        let request: Request = Request.batch(queries: self, type: type, flags: 0x00, consistency: .any)
        config.connection?.execute(request, oncompletion: oncompletion)
    }
}

extension Query {

    /**
         Convienence Function to prepare queries
         
         Parameters:
            - oncompletion: Callback Closure
         
         Returns a Result type through the given callback
     */
    public func prepare(oncompletion: @escaping ((Result)->Void)) {
        config.connection?.execute(.prepare(query: self), oncompletion: oncompletion)
    }


    /**
         Convienence Function to execute non-prepared queries
         
         Parameters:
            - oncompletion: Callback Closure
         
         Returns a Result type through the given callback
     */
    public func execute(oncompletion: @escaping ((Result)->Void)) {
        config.connection?.execute(.query(using: self), oncompletion: oncompletion)
    }
}

public enum BatchType: Byte {
    case logged     = 0x00
    case unlogged   = 0x01
    case counter    = 0x02
}

public enum Flags: Byte {
    case none = 0x00
    case compression = 0x01
    case tracing = 0x02
    case all = 0x03
    
}
public enum QueryFlags {
    case values             // 0x01
    case skipMetadata       // 0x02
    case pageSize(Int)      // 0x04
    case withPagingState    // 0x08
    case withSerialConsistency // 0x10
    case withTimestamp      //0x20
    case withValueNames     //0x40
}
public enum SQLFunction<T> {
    case max([T])
    case min([T])
    case avg([T])
    case sum([T])
    case count([T])
    
    func pack() -> String {
        switch self {
        case .max(let args)     : return args.count == 0 ? "MAX(*)" : "MAX(\(args.map{ String(describing: $0) }.joined(separator: ", ")))"
        case .min(let args)     : return args.count == 0 ? "MIN(*)" : "MIN(\(args.map{ String(describing: $0) }.joined(separator: ", ")))"
        case .avg(let args)     : return args.count == 0 ? "AVG(*)" : "AVG(\(args.map{ String(describing: $0) }.joined(separator: ", ")))"
        case .sum(let args)     : return args.count == 0 ? "SUM(*)" : "SUM(\(args.map{ String(describing: $0) }.joined(separator: ", ")))"
        case .count(let args)   : return args.count == 0 ? "COUNT(*)" : "COUNT(\(args.map{ String(describing: $0) }.joined(separator: ", ")))"
        }
    }
}

public protocol Query {
    
    var preparedID: [Byte]? { get set }
    
    func packQuery() -> Data
    func packParameters() -> Data
}

public struct Select: Query {
    
    let table: String
    
    let fields: [String]
    
    var orderBy: String? = nil
    
    var conditions: Predicate? = nil

    var limitResultCount: Int? = nil

    var distinct = false

    var having : Having?

    var sqlfunction: SQLFunction<String>? = nil
    
    var consistency: Consistency
    
    var flags: Flags = .none

    public var preparedID: [Byte]? = nil

    public init(_ fields: [String], from table: String, consistency: Consistency = .one) {
        self.fields = fields
        self.table = table
        self.consistency = consistency
    }
    
    private mutating func order(by clause: [Order]) {
        orderBy = clause.map { $0.description }.joined(separator: ", ")
    }

    public func ordered(by clause: Order...) -> Select {
        var new = self
        new.order(by: clause)
        return new
    }

    private mutating func has(_ clause: Having) {
        having = clause
    }
    
    public func having(_ clause: Having) -> Select {
        var new = self
        new.has(clause)
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

    public mutating func filter(by conditions: Predicate) {
        self.conditions = conditions
    }

    public func filtered(by conditions: Predicate) -> Select {
        var new = self
        new.filter(by: conditions)
        return new
    }
    
    public mutating func set(consistency: Consistency = .any, flags: Flags = .none) {
        self.flags = flags
        self.consistency = consistency
    }

    public func with(consistency: Consistency = .any, flags: Flags = .none) -> Select {
        var new = self
        new.set(consistency: consistency, flags: flags)
        return new
    }

    public func packQuery() -> Data {
        var result = distinct ? "SELECT DISTINCT" : "SELECT "
        
        if let function = sqlfunction?.pack() {
            fields.count == 0 ? (result += "\(function) FROM \(table)") :
                (result += "\(function), \(fields.joined(separator: " ")) FROM \(table)")
        } else {
            fields.count == 0 ? (result += "* FROM \(table)") :
                (result += "\(fields.joined(separator: " ")) FROM \(table)")
        }
        
        if let cond = conditions {
            result += " WHERE " + cond.str
        }
        if let order = orderBy {
            result += " ORDER BY \(order)"
        }
        if let havingClause = having {
            result += " HAVING \(havingClause.clause)"
        }
        if let limit = limitResultCount {
            result += " LIMIT \(limit)"
        }

        return (result + ";").longStringData
    }

    public func packParameters() -> Data {
        var data = Data()

        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
    }
}

public struct Update: Query {
    
    let tableName: String
    
    let newValues: [String: Any]

    var conditions: Predicate
    
    var consistency: Consistency
    
    var flags: Flags = .none

    public var preparedID: [Byte]? = nil

    public init(to newValues: [String: Any], in tableName: String, where predicate: Predicate, consistency: Consistency = .any) {
        self.newValues = newValues
        self.tableName = tableName
        self.conditions = predicate
        self.consistency = consistency
    }
    
    public mutating func filter(by predicate: Predicate){
        conditions = predicate
    }

    public mutating func set(consistency: Consistency = .any, flags: Flags = .none) {
        self.flags = flags
        self.consistency = consistency
    }
    
    public func with(consistency: Consistency = .any, flags: Flags = .none) -> Update {
        var new = self
        new.set(consistency: consistency, flags: flags)
        return new
    }

    public func packQuery() -> Data {
        
        let vals  = packPairs(newValues)
        let conds = conditions.str

        return ("UPDATE \(tableName) SET \(vals) WHERE \(conds);").longStringData
    }

    public func packParameters() -> Data {
        var data = Data()

        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
    }
}

public struct Delete: Query {

    let tableName: String
    
    let conditions: Predicate
    
    var consistency: Consistency

    var flags: Flags = .none

    public var preparedID: [Byte]? = nil

    public init(from tableName: String, where condition: Predicate, consistency: Consistency = .any) {
        self.conditions = condition
        self.tableName = tableName
        self.consistency = consistency
    }

    public mutating func set(consistency: Consistency = .any, flags: Flags = .none) {
        self.flags = flags
        self.consistency = consistency
    }
    
    public func with(consistency: Consistency = .any, flags: Flags = .none) -> Delete {
        var new = self
        new.set(consistency: consistency, flags: flags)
        return new
    }

    public func packQuery() -> Data {
        return ("DELETE FROM \(tableName) WHERE \(conditions.str);").longStringData
    }

    public func packParameters() -> Data {
        var data = Data()
        
        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
    }
}

public struct Insert: Query {
    
    let tableName: String
    
    let fields: [String: Any]
    
    var consistency: Consistency

    var flags: Flags = .none

    public var preparedID: [Byte]? = nil

    public init(_ fields: [String: Any], into tableName: String, consistency: Consistency = .any) {
        self.fields = fields
        self.tableName = tableName
        self.consistency = consistency
    }
    
    public mutating func set(consistency: Consistency = .any, flags: Flags = .none) {
        self.flags = flags
        self.consistency = consistency
    }
    
    public func with(consistency: Consistency = .any, flags: Flags = .none) -> Insert {
        var new = self
        new.set(consistency: consistency, flags: flags)
        return new
    }

    public func packQuery() -> Data {
        let keys = packKeys(fields)
        let vals = packValues(fields)

        return ("INSERT INTO \(tableName) (\(keys)) VALUES(\(vals));").longStringData
    }

    public func packParameters() -> Data {
        var data = Data()

        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
    }
}

public struct Raw: Query {

    let query: String

    var consistency: Consistency

    var flags: Flags = .none

    public var preparedID: [Byte]? = nil

    init(query: String, consistency: Consistency = .one) {
        self.query = query
        self.consistency = consistency
    }

    public mutating func set(consistency: Consistency = .any, flags: Flags = .none) {
        self.flags = flags
        self.consistency = consistency
    }
    
    public func with(consistency: Consistency = .any, flags: Flags = .none) -> Raw {
        var new = self
        new.set(consistency: consistency, flags: flags)
        return new
    }

    public func packQuery() -> Data {
        return query.longStringData
    }

    public func packParameters() -> Data {
        var data = Data()

        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
    }
}

