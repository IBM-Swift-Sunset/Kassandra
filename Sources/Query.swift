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
    
    var preparedID: [Byte]? { get set }

    func build() -> String

    func pack() -> Data
    func packQuery() -> Data
    func packParameters() -> Data
}
public enum Status {
    case success
    case failure(Error)
}

extension Array where Element: Query {
    func execute(with type: BatchType, consis: Consistency, oncompletion: ((Result)->Void)) {
        do {
            let request: Request = Request.batch(queries: self, type: type, flags: 0x00, consistency: .any)
            try config.connection?.execute(request) {
                result in
                
                oncompletion(result)
            }
        } catch {
            oncompletion(Result.error(ErrorType.IOError))
        }
    }
}

extension Query {
    public func prepare() -> Promise<[Byte]> {
        let p = Promise<[Byte]>.deferred()
        
        let request: Request = .prepare(query: self)
        
        do {
            try config.connection?.execute(request) {
                result in
                
                switch result {
                case .error(let error): p.reject(dueTo: error)
                case .kind(let res):
                    switch res {
                    case Kind.prepared(let id, _,_) : p.resolve()(id)
                    default                         : break
                    }
                default: p.reject(dueTo: ErrorType.IOError)
                }
            }
        } catch {
            p.reject(dueTo: error)
            
        }
        
        return p
    
    }
    public func execute() -> Promise<Status> {
        let p = Promise<Status>.deferred()
        
        let request: Request = .query(using: self)
        
        do {
            try config.connection?.execute(request) {
                result in
                
                switch result {
                case .error(let error): p.reject(dueTo: error)
                case .void: p.resolve()(Status.success)
                default : p.reject(dueTo: ErrorType.IOError)
                }
                
            }
        } catch {
            p.reject(dueTo: error)
            
        }
        
        return p
    }

    public func execute() -> Promise<TableObj> {
        let p = Promise<TableObj>.deferred()
    
        let request: Request = .query(using: self)
        
        do {
            try config.connection?.execute(request) {
                result in
                
                switch result {
                case .error(let error): p.reject(dueTo: error)
                case .kind(let res):
                    switch res {
                    case Kind.rows(_, let r): p.resolve()(TableObj(rows: r))
                    default: p.resolve()(TableObj(rows: []))
                    }
                default: p.reject(dueTo: ErrorType.NoDataError)
                }
            }
        } catch {
            p.reject(dueTo: error)

        }

        return p
    }
}

public enum Order: String {
    case ASC = "ASC"
    case DESC = "DESC"
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

public struct Select: Query {
    
    let table: String
    
    let fields: [String]
    
    var order: [String: Order]? = nil
    
    var conditions: Predicate? = nil

    var limitResultCount: Int? = nil
    
    var sqlfunction: SQLFunction<String>? = nil
    
    var consistency: Consistency
    
    var flags: Flags = .none

    public var preparedID: [Byte]? = nil

    public init(_ fields: [String], from table: String, consistency: Consistency = .one) {
        self.fields = fields
        self.table = table
        self.consistency = consistency
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

    public mutating func filtered(by conditions: Predicate) {
        self.conditions = conditions
    }

    public func filter(by conditions: Predicate) -> Select {
        var new = self
        new.filtered(by: conditions)
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

    public func pack() -> Data {
        var data = Data()
        
        data.append(build().longStringData)
        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
    }

    public func packQuery() -> Data {
        return build().longStringData
    }

    public func packParameters() -> Data {
        var data = Data()

        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
    }

    public func build() -> String {
        var str = "SELECT "
        
        if let function = sqlfunction?.pack() {
            fields.count == 0 ? (str += "\(function) FROM \(table)") :
                (str += "\(function), \(fields.joined(separator: " ")) FROM \(table)")
        } else {
            fields.count == 0 ? (str += "* FROM \(table)") :
                (str += "\(fields.joined(separator: " ")) FROM \(table)")
        }
        
        if let cond = conditions {
            str += " WHERE " + cond.str
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

    public func build() -> String {
        let vals  = packPairs(newValues)
        let conds = conditions.str
        
        return "UPDATE \(tableName) SET \(vals) WHERE \(conds);"
    }

    public func pack() -> Data {
        var data = Data()

        data.append(build().longStringData)
        

        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
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

    public func build() -> String {
        return "DELETE FROM \(tableName) WHERE \(conditions.str);"
    }

    public func pack() -> Data {
        var data = Data()

        data.append(build().longStringData)
        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
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

    public func build() -> String {
        let keys = packKeys(fields)
        let vals = packValues(fields)
        
        return "INSERT INTO \(tableName) (\(keys)) VALUES(\(vals));"
    }

    public func pack() -> Data {
        var data = Data()
        
        data.append(build().longStringData)
        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
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

    public func build() -> String {
        return query
    }

    public func pack() -> Data {
        var data = Data()
    
        data.append(query.longStringData)
        data.append(consistency.rawValue.data)
        data.append(flags.rawValue.data)
        
        return data
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

public enum BatchType: Byte {
    case logged     = 0x00
    case unlogged   = 0x01
    case counter    = 0x02
}

