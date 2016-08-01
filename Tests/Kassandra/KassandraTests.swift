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


import XCTest
@testable import Kassandra

public final class Employee {
    let id: String
    let name: String
    let city: String
    
    init(id: String, name: String, city: String) {
        self.id = id
        self.name = name
        self.city = city
    }
}
extension Employee: Model {
    public enum Field: String {
        case id = "id"
        case name = "name"
        case city = "city"
    }

    public static var tableName: String = "employee"
    
    public static var primaryKey = Field.id
}

class KassandraTests: XCTestCase {

    private var client: Kassandra!
    
    weak var expectation: XCTestExpectation!
    
    var tokens = [String]()

    static var allTests: [(String, (KassandraTests) -> () throws -> Void)] {
        return [
            ("testConnect", testConnect),
            ("testQuery", testQuery),
        ]
    }
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
        client = Kassandra()
    }
	
    func testConnect() throws {
        
        try client.connect {
                error in
                
                print(error)
            }
    }
    
    func testQuery() throws {
    
        do {
            try client.connect {
                error in
                
                print(error)
            }
            
            let _ = client["test"]
            
            try Employee.insert(
                [Employee.Field.id.rawValue: "7",Employee.Field.name.rawValue: "Aaron",Employee.Field.city.rawValue: "Austin"]) {
                result, error in
                
                print(result, error )
            }
            sleep(1)
            try Employee.select() {
                result, error in
                
                print(result!.rows[0].map{ "\($0) = \($1.decodeSDataString)"})
            }
            sleep(1)
            try Employee.update([Employee.Field.city.rawValue: "Durham"], conditions: [Employee.Field.id.rawValue: "7"]){
                result, error in
                
                print(result!.rows[0].map{ "\($0) = \($1.decodeSDataString)"})
            }
            sleep(1)
            try Employee.select() {
                result, error in
                
                print(result!.rows[0].map{ "\($0) = \($1.decodeSDataString)"})
            }
            sleep(1)
            try Employee.delete(where: [Employee.Field.id.rawValue: "7"]) {
                result, error in
                
                print(result, error)
            }
            sleep(1)
            try Employee.select() {
                result, error in
                
                print(result)
            }
            
        } catch {
            throw error
        }
        sleep(5)
    }
}
