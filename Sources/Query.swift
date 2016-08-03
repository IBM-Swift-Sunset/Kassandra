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

public enum QueryOptions {
    case orderded(by: String)
    case count()
    case limit(to: Int)
    case offset(by: Int)
}

public struct Select: Query {
    
    let tableName: String
    
    let fields: [String]
    
    init(_ fields: [String], from tableName: String) {
        self.fields = fields
        self.tableName = tableName
    }
    
    public func pack() -> Data {
        var data = Data()
        
        fields.count == 0 ? data.append("SELECT * FROM \(tableName);".sData) :
                            data.append("SELECT \(fields.joined(separator: " ")) FROM \(tableName);".sData)

        data.append(Consistency.one.rawValue.data)
        data.append(0x00.data)
        
        return data
    }
}
public struct Update: Query {
    
    let tableName: String
    
    let newValues: [String: Any]
    let conditions: [String: Any]
    
    init(to newValues: [String: Any], in tableName: String, where condition: [String: Any]) {
        self.newValues = newValues
        self.conditions = condition
        self.tableName = tableName
    }
    
    public func pack() -> Data {
        var data = Data()

        let conds = packPairs(conditions)
        let vals  = packPairs(newValues)
        
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
