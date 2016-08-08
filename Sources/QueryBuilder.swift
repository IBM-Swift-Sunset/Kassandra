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

public protocol Convertible {
    
}
extension String: Convertible{
    
}
extension Int: Convertible {
    
}
public enum WhereConvertible {
    case notEqual(String, Convertible)
    case equal(String, Convertible)
    case greaterThan(String, Convertible)
    case greaterThanOrEqual(String, Convertible)
    case lessThan(String, Convertible)
    case lessThanOrEqual(String, Convertible)
    case and(Predicate, Predicate)
    case or(Predicate, Predicate)
}
public struct Predicate: Convertible  {
    let str: String
    
    init(_ expession: WhereConvertible) {
        switch expession {
        case .notEqual(let lhr, let rhs)             : str = "\(lhr) != \(convert(rhs))"
        case .equal(let lhr, let rhs)                : str = "\(lhr) =  \(convert(rhs))"
        case .greaterThan(let lhr, let rhs)          : str = "\(lhr) >  \(convert(rhs))"
        case .greaterThanOrEqual(let lhr, let rhs)   : str = "\(lhr) >= \(convert(rhs))"
        case .lessThan(let lhr, let rhs)             : str = "\(lhr) <  \(convert(rhs))"
        case .lessThanOrEqual(let lhr, let rhs)      : str = "\(lhr) <= \(convert(rhs))"
        case .and(let lhr, let rhs)                  : str = "\(lhr.str) AND \(rhs.str)"
        case .or(let lhr, let rhs)                   : str = "\(lhr.str) OR \(rhs.str)"
        }
    }
}

public func convert(_ item: Convertible) -> String{
    switch item {
    case is String  : return "'\(item)'"
    case is Int     : return "\(item)"
    default: return ""
    }
}

public func ==(key: String, predicate: Convertible) -> Predicate {
    return Predicate(.equal(key, predicate))
}
public func !=(key: String, predicate: Convertible) -> Predicate {
    return Predicate(.notEqual(key, predicate))
}
public func >(key: String, predicate: Convertible) -> Predicate {
    return Predicate(.greaterThan(key, predicate))
}
public func >=(key: String, predicate: Convertible) -> Predicate {
    return Predicate(.greaterThanOrEqual(key, predicate))
}
public func <(key: String, predicate: Convertible) -> Predicate {
    return Predicate(.lessThan(key, predicate))
}
public func <=(key: String, predicate: Convertible) -> Predicate {
    return Predicate(.lessThanOrEqual(key, predicate))
}
public func &&(rhs: Predicate, lhs: Predicate) -> Predicate {
    return Predicate(.and(rhs, lhs))
}
public func ||(rhs: Predicate, lhs: Predicate) -> Predicate {
    return Predicate(.or(rhs, lhs))
}
