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

public enum Result {
    case error(ErrorType)
    case kind(Kind)
    case generic([String: Any])
    case void
    
    public var success: Bool {
        switch self {
        case .error : return false
        default     : return true
        }
    }
    
    public var asError: Error? {
        switch self {
        case .error(let err) : return err
        default              : return nil
        }
    }
    
    public var asRows: [Row]? {
        switch self {
        case .kind(let kind) :
            switch kind {
            case .rows(_, let rows) : return rows
            default                 : return nil
            }
        default: return nil
        }
    }
    
    public var asPrepared: [Byte]? {
        switch self {
        case .kind(let kind) :
            switch kind {
            case .prepared(let id, _, _) : return id
            default                      : return nil
            }
        default: return nil
        }
    }
    
    public var asKeyspace: String? {
        switch self {
        case .kind(let kind) :
            switch kind {
            case .keyspace(let name)     : return name
            default                      : return nil
            }
        default: return nil
        }
    }
    
    public var asSchema: (type: String, target: String, options: String)? {
        switch self {
        case .kind(let kind) :
            switch kind {
            case .schema(let t, let s, let o)  : return (type: t, target: s, options: o)
            default                            : return nil
            }
        default: return nil
        }
    }
}
