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
    let emp_id: String
    let emp_name: String
    let emp_city: String
    
    init(id: String, name: String, city: String) {
        self.emp_id = id
        self.emp_name = name
        self.emp_city = city
    }
}
extension Employee: Model {
    public enum Field: String {
        case emp_id = "emp_id"
        case emp_name = "emp_name"
        case emp_city = "emp_city"
    }

    public static var tableName: String = "emp"
    
    public static var primaryKey = Field.emp_id
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
            
            /*try Employee.insert([Employee.Field.emp_name.rawValue: "Aaron"]) {
                result, error in
                
                print(result, error )
            }*/
            try Employee.delete(where: [:]) {
                result, error in
                
                print(result, error)
            }
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
