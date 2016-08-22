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


/**
     Convienence Function to excute batch queries from arrays
     
     Parameters:
        - type:         Batch Type of the operation
        - consis:       Consistency for the operation
        - oncompletion: Callback Closure
     
     Returns a Result type through the given callback
 */
extension Array where Element: Query {

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

public protocol Query {
    
    var preparedID: [Byte]? { get set }
    
    func build() -> String
    func packParameters() -> Data
}

// Optional Query Values
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
