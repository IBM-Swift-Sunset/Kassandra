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
extension Student: Model, CustomStringConvertible {
    
    public enum Field : String {
        case id
        case name
        case school
    }
    
    public var description: String {
        return "id: \(id!), name: \(name), school: \(school)"
    }
    public static var tableName: String = "student"
    
    public static var primaryKey: Field = Field.id
    
    public var key: Int? {
        get { return id }
        set { id = newValue }
    }

    public convenience init(row: Row) {
        let id = row["id"] as? Int
        let name = row["name"] as! String
        let school = row["school"] as! String
        
        self.init(id: id, name: name, school: school)
    }
    
}
public class TodoItem: Table {
    public enum Field: String {
        case type = "type"
        case userID = "userID"
        case title = "title"
        case pos = "pos"
        case completed = "completed"
    }

    public static var tableName: String = "todoitem"
    
}

class KassandraTests: XCTestCase {

    private var client: Kassandra!
    
    weak var expectation: XCTestExpectation!
    
    var tokens = [String]()

    static var allTests: [(String, (KassandraTests) -> () throws -> Void)] {
        return [
            ("testConnect", testConnect),
            ("testTable", testTable),
            //("testModel", testModel),
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
        try client.connect(oncompletion: ErrorHandler)

        let _ = client["test"]

        sleep(1)
        let query: Raw = Raw(query: "INSERT INTO todoitem (userid) VALUES(176);")
    
        try client.execute(Request.prepare(query: query)) {
            prepared, error in
            var id: [Byte] = []

            if let prep = prepared {
                switch prep {
                case Kind.prepared(let sid, _, _): id = sid
                default: print("failure")
                }
            }
            print(id)
            do {
                try self.client.execute(Request.execute(id: id, parameters: query)) {
                    prepared, error in
                    print("--",prepared, error)
                }
            } catch{
                
            }
            
        }
        sleep(10)
        print("ffef")
    } catch {
        throw error
    }
        
    }

    func testModel() throws {
        
        print("--------+---------+----------+---------")

        do {
            try client.connect(oncompletion: ErrorHandler)

            sleep(1)
            let _ = client["test"]

            /*try Student.drop().execute(oncompletion: ErrorHandler)
            try Student.select().execute(oncompletion: ResultHandler)
            try Student.insert([:]).execute(oncompletion: ErrorHandler)
            try Student.delete(where: [:]).execute(oncompletion: ErrorHandler)
            try Student.update([:], conditions: [:]).execute(oncompletion: ErrorHandler)*/

            let student = Student(id: 10, name: "Dave", school: "UNC") ; sleep(1)
            try student.create() ; sleep(1)
            
            student.id = 15
            student.name = "Aaron"
            
            student.save().fail{
                error in
                    print(error)
                }
            
            Student.fetch()
                .then { rows in
                    print(rows)
                    student.delete().fail {
                        error in
                        print(error)
                    }
                    Student.fetch()
                        .then { rows in
                            print(rows)
                            
                        }.fail{ error in
                            print(error)
                    }
                }.fail{ error in
                    print(error)
                }
            
            
        } catch {
            throw error
        }
        sleep(5)
    }

    public func ErrorHandler(error: Error?) {
        if error != nil { print(error) }
    }
}
