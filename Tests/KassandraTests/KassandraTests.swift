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
import Foundation

#if os(OSX) || os(iOS)
    import Darwin
#elseif os(Linux)
    import Glibc
#endif

class KassandraTests: XCTestCase {
    
    private var connection: Kassandra!
    
    public var t: TodoItem!
    
    var tokens = [String]()
    
    public let createKeyspace: String = "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};"
    public let useKeyspace: String = "USE test;"
    
    static var allTests: [(String, (KassandraTests) -> () throws -> Void)] {
        return [
            ("testConnect", testConnect),
            ("testKeyspaceWithCreateABreadShopTable", testKeyspaceWithCreateABreadShopTable),
            ("testKeyspaceWithCreateABreadShopTableInsertAndSelect", testKeyspaceWithCreateABreadShopTableInsertAndSelect),
            ("testKeyspaceWithCreateATable", testKeyspaceWithCreateATable),
            ("testKeyspaceWithFetchCompletedTodoItems", testKeyspaceWithFetchCompletedTodoItems),
            ("testPreparedQuery", testPreparedQuery),
            ("testZBatch", testZBatch),
            ("testZDropTableAndDeleteKeyspace", testZDropTableAndDeleteKeyspace)
        ]
    }
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
        connection = Kassandra()
        t = TodoItem()
    }
    
    func testConnect() throws {
        
        try connection.connect() { result in XCTAssertTrue(result.success, "Connected to Cassandra")
        }
    }
    
    func testKeyspaceWithCreateABreadShopTable() throws {
        
        let expectation1 = expectation(description: "Create a table in the keyspace or table exist in the keyspace")
        
        try connection.connect() { result in
            XCTAssert(result.success, "Connected to Cassandra")
            
            self.connection.execute(self.createKeyspace) { result in
                self.connection.execute(self.useKeyspace) { result in
                    self.connection.execute("CREATE TABLE IF NOT EXISTS breadshop (userID uuid primary key, type text, bread map<text, int>, cost float, rate double, time timestamp);") {
                        result in
                        
                        XCTAssertEqual(result.asSchema!.type, "CREATED", "Created Table \(BreadShop.tableName)")
                        if result.success { expectation1.fulfill() }
                    }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateABreadShopTableInsertAndSelect() throws {
        
        let expectation1 = expectation(description: "Insert and select the row")
        
        let bread: [BreadShop.Field: Any] = [.userID: UUID(), .type: "Sandwich", .bread: ["Chicken Roller": 3, "Steak Roller": 7, "Spicy Chicken Roller": 9], .cost: 2.1, .rate: 9.1, .time : Date()]
        
        try connection.connect() { result in
            XCTAssertTrue(result.success, "Connected to Cassandra")
            
            self.connection.execute(self.useKeyspace) { result in
                BreadShop.insert(bread).execute() { result in
                    BreadShop.select().execute() {
                        result in
                        
                        XCTAssertEqual(result.asRows?.count, 1)
                        if result.asRows != nil { expectation1.fulfill() }
                    }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
        
    }
    
    func testKeyspaceWithCreateATable() throws {
        
        let expectation1 = expectation(description: "Create a table in the keyspace or table exist in the keyspace")
        
        try connection.connect() { result in
            XCTAssertTrue(result.success, "Connected to Cassandra")
            
            self.connection.execute(self.createKeyspace) { result in
                self.connection.execute(self.useKeyspace) { result in
                    self.connection.execute("CREATE TABLE IF NOT EXISTS todoitem(userID uuid primary key, type text, title text, pos int, completed boolean);") { result in
                        
                        XCTAssertEqual(result.asSchema!.type, "CREATED", "Created Table \(TodoItem.tableName)")
                        if result.success { expectation1.fulfill() }
                    }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    
    func testKeyspaceWithFetchCompletedTodoItems() throws {
        
        let expectation1 = expectation(description: "Select first two completed item and check their row count")
        let expectation2 = expectation(description: "Truncate the table to get 0 completed items")
        
        let userID1 = UUID()
        let god: [TodoItem.Field: Any] = [.type: "todo", .userID: userID1, .title: "God Among God", .pos: 1, .completed: true]
        let ares: [TodoItem.Field: Any] = [.type: "todo", .userID: UUID(), .title: "Ares", .pos: 2, .completed: true]
        let thor: [TodoItem.Field: Any] = [.type: "todo", .userID: UUID(), .title: "Thor", .pos: 3, .completed: true]
        let apollo: [TodoItem.Field: Any] = [.type: "todo", .userID: UUID(), .title: "Apollo", .pos: 4, .completed: true]
        let cass: [TodoItem.Field: Any] = [.type: "todo", .userID: UUID(), .title: "Cassandra", .pos: 5, .completed: true]
        let hades: [TodoItem.Field: Any] = [.type: "todo", .userID: UUID(), .title: "Hades", .pos: 6, .completed: true]
        let athena: [TodoItem.Field: Any] =  [.type: "todo", .userID: UUID(), .title: "Athena", .pos: 7, .completed: true]
        
        try connection.connect() { result in
            XCTAssertTrue(result.success, "Connected to Cassandra")
            
            self.connection.execute(self.createKeyspace) { result in
                self.connection.execute(self.useKeyspace) { result in
                    TodoItem.insert(god).execute() { result in
                        TodoItem.insert(ares).execute() { result in
                            TodoItem.insert(thor).execute() { result in
                                TodoItem.insert(apollo).execute() { result in
                                    TodoItem.insert(cass).execute() { result in
                                        TodoItem.insert(hades).execute() { result in
                                            TodoItem.insert(athena).execute() { result in
                                                TodoItem.update([.title: "Zeus"], conditions: "userID" == userID1).execute {
                                                    result in
                                                    
                                                    TodoItem.select().limit(to: 2).filter(by: "userID" == userID1).execute() {
                                                        result in
                                                        
                                                        if let rows = result.asRows {
                                                            XCTAssertEqual(rows[0]["title"] as! String, "Zeus")
                                                            if rows.count == 1 { expectation1.fulfill() }
                                                        }
                                                    }
                                                    
                                                    TodoItem.truncate().execute() { result in
                                                        
                                                        TodoItem.count(TodoItem.Field.type).execute() { result in
                                                            XCTAssertEqual(result.asRows![0]["system.count(type)"] as! Int64, 0)
                                                            expectation2.fulfill()
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    
    func testZDropTableAndDeleteKeyspace() throws {
        
        let expectation1 = expectation(description: "Drop the table and delete the keyspace")
        
        try connection.connect() { result in
            XCTAssertTrue(result.success, "Connected to Cassandra")
            
            self.connection.execute(self.useKeyspace) { result in
                TodoItem.drop().execute() { result in
                    self.connection.execute("DROP KEYSPACE test") { result in
                        
                        XCTAssertTrue(result.success)
                        expectation1.fulfill()
                    }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testPreparedQuery() throws {
        
        let expectation1 = expectation(description: "Execute a prepared query")
        
        var query: Query = Raw(query: "SELECT userID FROM todoitem WHERE completed = true allow filtering;")
        
        try connection.connect() { result in
            XCTAssertTrue(result.success, "Connected to Cassandra")
            
            self.connection.execute(self.useKeyspace) { result in
                query.prepare() { result in
                    if let id = result.asPrepared {
                        
                        query.preparedID = id
                        
                        query.execute() { result in
                            
                            XCTAssertEqual(result.asRows?.count, 0)
                            if result.success { expectation1.fulfill() }
                        }
                    }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    public func testZBatch() throws {
        let expectation1 = expectation(description: "Execute a batch query")
        
        let insert1 = TodoItem.insert([.type: "todo", .userID: NSUUID(), .title: "Water Plants", .pos: 15, .completed: false])
        let insert2 = TodoItem.insert([.type: "todo", .userID: NSUUID(),.title: "Make Dinner", .pos: 14, .completed: true])
        let insert3 = TodoItem.insert([.type: "todo", .userID: NSUUID(),.title: "Excercise", .pos: 13, .completed: true])
        let insert4 = TodoItem.insert([.type: "todo", .userID: NSUUID(),.title: "Sprint Plannning", .pos: 12, .completed: false])
        
        try connection.connect() { result in
            XCTAssertTrue(result.success, "Connected to Cassandra")
            
            self.connection.execute(self.useKeyspace) { result in
                insert1.execute() { result in
                    [insert1,insert2,insert3,insert4].execute(with: .logged, consis: .any) { result in
                        TodoItem.select().execute() { result in
                            
                            XCTAssertEqual(result.asRows?.count, 4)
                            if result.success { expectation1.fulfill() }
                        }
                    }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
}

