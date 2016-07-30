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

/*class Employee {
    let id: Int
    let name: String
    let city: String
    
    init(id: Int, name: String, city: String){
        self.id = id
        self.name = name
        self.city = city
    }
}
extension Employee: Table {
    enum Fields: String {
        case id = "id"
        case name = "name"
        case city = "city"
    }
    
    var hashValue: Int {
        return Fields.id.hashValue
    }
}*/

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
        
        do {
            try client.connect {
                error in
                
                print(error)
            }

        } catch {
            throw error
        }
        sleep(10)
    }
    
    func testQuery() throws {
    
        do {
            try client.connect {
                error in
                
                print(error)
            }
            
            //let query0 = "DROP TABLE emp;"
            //let query2 = "CREATE KEYSPACE test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}; "
            //let query3 = "CREATE TABLE emp(emp_id int PRIMARY KEY, emp_name text, emp_city text, emp_sal varint, emp_phone varint);"
            //let query3 = "INSERT INTO emp (emp_id, emp_name, emp_city, emp_phone, emp_sal) VALUES(1,'ram', 'Hyderabad', 9848022338, 50000);"
            let query4 = "select * from emp;"

            let _ = client["test"]

            try client.query(query: query4) {
                error in
                
                print(error)
            }
            /*try client.query(query: query4) {
                error in
                
                print(error)
            }*/
        } catch {
            throw error
        }
        sleep(10)
    }
}
