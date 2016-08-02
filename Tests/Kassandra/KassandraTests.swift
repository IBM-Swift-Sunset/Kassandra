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

public final class Student {
    var id: Int?
    var name: String
    var school: String
    
    init(id: Int?, name: String, school: String) {
        self.id = id
        self.name = name
        self.school = school
    }
}
extension Student: Model {

    public enum Field: String {
        case id = "id"
        case name = "name"
        case school = "school"
    }
    
    public static var tableName: String = "student"
    
    public static var primaryKey: Field = .id
    
    public var setPrimaryKey: Int? {
        get {
            return id
        }
        set {
            id = newValue
        }
    }
    
    public var serialize: [Field: AnyObject] {
        return [.name: name, .school: school]
    }

    public convenience init(row: Row) {
        let id = row["id"] as? Int
        let name = row["name"] as! String
        let school = row["school"] as! String
        
        self.init(id: id, name: name, school: school)
    }
    
}
public class Employee: Table {
    public enum Field: String {
        case id = "id"
        case name = "name"
        case city = "city"
    }

    public static var tableName: String = "employee"
    
}

class KassandraTests: XCTestCase {

    private var client: Kassandra!
    
    weak var expectation: XCTestExpectation!
    
    var tokens = [String]()

    static var allTests: [(String, (KassandraTests) -> () throws -> Void)] {
        return [
            ("testConnect", testConnect),
            ("testTable", testTable),
            ("testModel", testModel),
        ]
    }
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
        client = Kassandra()
    }
	
    func testConnect() throws {
        
        // try client.connect { error in }
    }
    
    func testTable() throws {
    
       do {
            try client.connect { error in print(error) }
            
            let _ = client["test"]
            
            sleep(1)
    
            try Employee.insert([.id: "7",.name: "Aaron",.city: "Austin"]) {
                result, error in
                
                print(result, error )
            }
            sleep(1)
            try Employee.select { result, error in
                
                for row in result!.rows {
                    print(row["id"], row["name"], row["city"])
                }
                
            }
            sleep(1)
            try Employee.update([.city: "Durham"], conditions: [.id: "7"]){
                result, error in
                
                print(result, error)
            }
            sleep(1)
            try Employee.select { result, error in
                
                for row in result!.rows {
                    print(row["id"], row["name"], row["city"])
                }
            }
            sleep(1)
            try Employee.delete(where: [.id: "7"]) {
                result, error in
                
                print(result, error)
            }
            sleep(1)
            try Employee.select { result, error in
                
                for row in result!.rows {
                    print(row["id"], row["name"], row["city"])
                }
            }
            
        } catch {
            throw error
        }
        sleep(5)
    }
    func testModel() throws {
        
        print("--------+---------+----------+---------")

        do {
            try client.connect { error in print(error) }

            sleep(1)
            let _ = client["test"]

            let studentTable = Student(id: 10, name: "Dave", school: "UNC")

            try studentTable.create() {
                result, error in
                
                print(result, error)
            }
            
            try studentTable.save()

        } catch {
            throw error
        }
        sleep(5)
    }
}
