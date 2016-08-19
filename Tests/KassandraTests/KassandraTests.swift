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


#if os(OSX) || os(iOS)
    import Darwin
#elseif os(Linux)
    import Glibc
#endif

class KassandraTests: XCTestCase {
    
    private var connection: Kassandra!
    
    public var t: TodoItem!

    var tokens = [String]()
    
    static var allTests: [(String, (KassandraTests) -> () throws -> Void)] {
        return [
            ("testConnect", testConnect),
            ("testCreateKeyspace", testCreateKeyspace),
            ("testKeyspaceWithCreateATable", testKeyspaceWithCreateATable)
            /*("testKeyspaceWithCreateABookCollectionTable", testKeyspaceWithCreateABookCollectionTable),
            ("testKeyspaceWithCreateABookCollectionTableInsertAndSelect", testKeyspaceWithCreateABookCollectionTableInsertAndSelect),
            ("testKeyspaceWithCreateABreadShopTable", testKeyspaceWithCreateABreadShopTable),
            ("testKeyspaceWithCreateABreadShopTableInsertAndSelect", testKeyspaceWithCreateABreadShopTableInsertAndSelect),
            ("testKeyspaceWithCreateACollectRanDataTable", testKeyspaceWithCreateACollectRanDataTable),
            ("testKeyspaceWithCreateACollectRanDataTableInsertAndSelect",testKeyspaceWithCreateACollectRanDataTableInsertAndSelect),
            ("testKeyspaceWithCreateAIceCreamTable", testKeyspaceWithCreateAIceCreamTable),
            ("testKeyspaceWithCreateAIceCreamTableInsertAndSelect", testKeyspaceWithCreateAIceCreamTableInsertAndSelect),
            ("testKeyspaceWithCreateATestScoreTable", testKeyspaceWithCreateATestScoreTable),
            ("testKeyspaceWithCreateATestScoreTableInsertAndSelect",testKeyspaceWithCreateATestScoreTableInsertAndSelect),*/
            //("testKeyspaceWithFetchCompletedTodoItems", testKeyspaceWithFetchCompletedTodoItems),
            /*("testOptions",testOptions),
            ("testPreparedQuery", testPreparedQuery),
            ("testTruncateTable",testTruncateTable),
            ("testZBatch", testZBatch),
            ("testZDropTableAndDeleteKeyspace", testZDropTableAndDeleteKeyspace),*/
            //("testMaxTodoitemID", testMaxTodoitemID),
        ]
    }
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
        connection = Kassandra()
        t = TodoItem()
    }
    
    func testConnect() throws {
        
        try connection.connect() { result in XCTAssertNil(result.asError) }
    }
    
    public func resultHandler(_ result: QueryResult) {
        
    }
    func testCreateKeyspace() throws {
        
        let expectation1 = expectation(description: "Created a keyspace or Keyspace exist")
        
        try connection.connect() { result in
            
            if result.asError == nil {
                self.connection.execute(query: "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};") {
                        result in

                        if result.success { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateATable() throws {
        
        let expectation1 = expectation(description: "Create a table in the keyspace or table exist in the keyspace")
        
        try connection.connect() { result in
            
            if result.asError == nil {
                let _ = self.connection["test"]

                self.connection.execute(query: "CREATE TABLE IF NOT EXISTS todoitem(userID int primary key, type text, title text, pos int, completed boolean);") {
                    result in

                    if result.success { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    
/*    func testKeyspaceWithCreateABreadShopTable() throws {
        
        let expectation1 = expectation(description: "Create a table in the keyspace or table exist in the keyspace")
        
        try connection.connect() { result in
            
            if result.asError == nil {
                let _ = self.connection["test"]
                
                self.connection.execute(query: "CREATE TABLE IF NOT EXISTS breadshop (userID uuid primary key, type text, breadname text, cost float, rate double, time timestamp);") {
                    result in
                    
                    if result.success { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateABreadShopTableInsertAndSelect() throws {
        
        let expectation1 = expectation(description: "Insert and select the row")
        
        let bread: [Field: Any] = [BreadShop.userid: "uuid()", BreadShop.type: "Sandwich", BreadShop.breadname: "Roller", BreadShop.cost: "2.1", BreadShop.rate: "9.1", BreadShop.time : "2013-03-07 11:17:38"]
        
        try connection.connect() { result in
            XCTAssertNil(result.asError)
            
            let _ = self.connection["test"]
            
            BreadShop.insert(values: bread).execute(self.connection) { result in
                BreadShop.select().execute(self.connection) {
                    result in
                    
                    if result.asRows != nil { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
        
    }
    
    func testKeyspaceWithCreateATestScoreTable() throws {
        
        let expectation1 = expectation(description: "Create a table in the keyspace or table exist in the keyspace")
        
        try connection.connect() { result in
            
            if result.asError == nil {
                let _ = self.connection["test"]
                
                self.connection.execute(query: "CREATE TABLE IF NOT EXISTS testscore (userID ascii primary key, commit blob, score decimal, subject text, time timestamp, userip inet);") {
                    result in
                    
                    if result.success { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateATestScoreTableInsertAndSelect() throws {
        
        let expectation1 = expectation(description: "Insert and select the row")
        
        let test: [Field: Any] = [TestScore.userid: "admin", TestScore.commit: "textAsBlob('bdb14fbe076f6b94444c660e36a400151f26fc6f')", TestScore.score: 3.141, TestScore.subject: "Calculus", TestScore.time: "toTimestamp(now())", TestScore.userip : "127.0.0.1"]
        
        try connection.connect() { result in
            XCTAssertNil(result.asError)
            
            let _ = self.connection["test"]
            
            TestScore.insert(values: test).execute(self.connection) { result in
                TestScore.select().execute(self.connection) {
                    result in
                    
                    if result.asRows != nil { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
        
    }
    
    func testKeyspaceWithCreateACollectRanDataTable() throws {
        
        let expectation1 = expectation(description: "Create a table in the keyspace or table exist in the keyspace")
        
        try connection.connect() { result in
            
            if result.asError == nil {
                let _ = self.connection["test"]
                
                self.connection.execute(query: "CREATE TABLE IF NOT EXISTS collectrandata (id int primary key, numbers tuple<int, text, float>);") {
                    result in
                    
                    if result.success { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateACollectRanDataTableInsertAndSelect() throws {
        
        let expectation1 = expectation(description: "Insert and select the row")

        let random: [Field: Any] = [CollectRanData.id: 1, CollectRanData.numbers: "(3, 'bar', 2.20)"]
        
        try connection.connect() { result in
            XCTAssertNil(result.asError)
            
            let _ = self.connection["test"]
            
            CollectRanData.insert(values: random).execute(self.connection) { result in
                CollectRanData.select().execute(self.connection) {
                    result in
                    
                    if result.asRows != nil { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateABookCollectionTable() throws {
        
        let expectation1 = expectation(description: "Create a table in the keyspace or table exist in the keyspace")
        
        try connection.connect() { result in
            
            if result.asError == nil {
                let _ = self.connection["test"]
                
                self.connection.execute(query: "CREATE TABLE IF NOT EXISTS bookcollection (id uuid primary key, bookname text, price float, series map<text,text>, emails set<text>);") {
                    result in
                    
                    if result.success { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateABookCollectionTableInsertAndSelect() throws {
        
        let expectation1 = expectation(description: "Insert and select the row")

        let books: [Field: Any] = [BookCollection.id: "uuid()", BookCollection.bookname: "Harry Potter", BookCollection.price: 89.99, BookCollection.series: "{'Volume 1' : 'Harry Potter and the Philosophers Stone', 'Volume 2' : 'Harry Potter and the Chamber of Secrets', 'Volume 3':'Harry Potter and the Prisoner of Azkaban', 'Volume 4':'Harry Potter and the Goblet of Fire', 'Volume 5':'Harry Potter and the Order of the Phoenix', 'Volume 6':'Harry Potter and the Half-Blood Prince', 'Volume 7':'Harry Potter and the Deathly Hallows'}", BookCollection.emails: "{'harrypotter@gmail.com', 'hermionegranger@gmail.com', 'ronweasley@gmail.com', 'harrypotterfan@gmail.com','ronweasley@gmail.com'}"]
        
        try connection.connect() { result in
            XCTAssertNil(result.asError)
            
            let _ = self.connection["test"]
            
            BookCollection.insert(values: books).execute(self.connection) { result in
                BookCollection.select().execute(self.connection) {
                    result in
                    
                    if result.asRows != nil { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateAIceCreamTable() throws {
        
        let expectation1 = expectation(description: "Create a table in the keyspace or table exist in the keyspace")
        
        try connection.connect() { result in
            
            if result.asError == nil {
                let _ = self.connection["test"]
                
                self.connection.execute(query: "CREATE TABLE IF NOT EXISTS icecream (id uuid primary key, icecreamname text, price float, flavors list<text>, calories varint);") {
                    result in
                    
                    if result.success { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testKeyspaceWithCreateAIceCreamTableInsertAndSelect() throws {
        
        let expectation1 = expectation(description: "Insert and select the row")

        let iceCreamPack: [Field: Any] = [IceCream.id: "uuid()", IceCream.icecreamname: "Xtreme Cookie n Cream", IceCream.price: 5.99, IceCream.flavors: "['Cookies', 'Strawberry Milk', 'Chocolate']", IceCream.calories: 1080]
        
        try connection.connect() { result in
            XCTAssertNil(result.asError)
            
            let _ = self.connection["test"]
            
            IceCream.insert(values: iceCreamPack).execute(self.connection) { result in
                IceCream.select().execute(self.connection) {
                    result in
                    
                    if result.asRows != nil { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }*/
    
    /*func testKeyspaceWithFetchCompletedTodoItems() throws {
        
        let expectation1 = expectation(description: "Filter out todoitems that are done and update one of the todoitems")
        
        let god: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 1, TodoItem.title: "God Among God", TodoItem.pos: 1, TodoItem.completed: true]
        let ares: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 2, TodoItem.title: "Ares", TodoItem.pos: 2, TodoItem.completed: true]
        let thor: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 3, TodoItem.title: "Thor", TodoItem.pos: 3, TodoItem.completed: true]
        let apollo: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 4, TodoItem.title: "Apollo", TodoItem.pos: 4, TodoItem.completed: true]
        let cass: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 5, TodoItem.title: "Cassandra", TodoItem.pos: 5, TodoItem.completed: true]
        let hades: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 6, TodoItem.title: "Hades", TodoItem.pos: 6, TodoItem.completed: true]
        let athena: [Field: Any] =  [TodoItem.type: "todo", TodoItem.userID: 7, TodoItem.title: "Athena", TodoItem.pos: 7, TodoItem.completed: true]
            
        try connection.connect() { result in
            XCTAssertNil(result.asError)
        
            let _ = self.connection["test"]
        
            TodoItem.insert(values: god).execute(self.connection) { result in
                TodoItem.insert(values: ares).execute(self.connection) { result in
                    TodoItem.insert(values: thor).execute(self.connection) { result in
                        TodoItem.insert(values: apollo).execute(self.connection) { result in
                            TodoItem.insert(values: cass).execute(self.connection) { result in
                                TodoItem.insert(values: hades).execute(self.connection) { result in
                                    TodoItem.insert(values: athena).execute(self.connection) { result in
                                        TodoItem.update(values: [TodoItem.title: "Zeus"], cond: TodoItem.userID == 1).execute {
                                            result in

                                            XCTAssertNil(result.asError)

                                            TodoItem.select().limited(to: 3).execute(self.connection) {
                                                result in
                                                
                                                if result.asRows != nil { expectation1.fulfill() }
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
    
   
   func testTruncateTable() throws {
        
        let expectation1 = expectation(description: "Truncate table")
        
        try connection.connect() { result in XCTAssertNil(result.asError)
            if result.asError == nil {
                let _ = self.connection["test"]
                
                let hera: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 10, TodoItem.title: "Hera", TodoItem.pos: 10, TodoItem.completed: true]
                let aphrodite: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 11, TodoItem.title: "Aphrodite", TodoItem.pos: 11, TodoItem.completed: true]
                let poseidon: [Field: Any] = [TodoItem.type: "todo", TodoItem.userID: 12, TodoItem.title: "Poseidon", TodoItem.pos: 12, TodoItem.completed: true]

                TodoItem.insert(values: hera).execute(self.connection) {
                    result in
                    
                    TodoItem.insert(values: aphrodite).execute(self.connection) {
                        result in
                        
                        TodoItem.insert(values: poseidon).execute(self.connection) {
                            result in
                            
                            TodoItem.select(count(TodoItem.type)).execute(self.connection) {
                                result in

                                XCTAssertEqual((result.asRows![0]["system.count(type)"] as! Int64), 10)
                                
                                TodoItem.truncate().execute(self.connection) { result in
                                    
                                    TodoItem.select(count(TodoItem.type)).execute(self.connection) { result in
                                        
                                        if (result.asRows![0]["system.count(type)"] as! Int64) == 0 { expectation1.fulfill() }
                                    }
                                }
                            }
                            
                        }
                    }
                }
            }
        }

        waitForExpectations(timeout: 10, handler: { error in XCTAssertNil(error, "Timeout") })
    }
    
    func testOptions() throws {
        
        let expectation1 = expectation(description: "Showing options")

        try connection.connect() { result in
            if result.asError == nil {
                
                let _ = self.connection["test"]

                sleep(1)

                self.connection.options() { result in if result.success { expectation1.fulfill() } }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
        
    }
    
    func testZDropTableAndDeleteKeyspace() throws {
        
        let expectation1 = expectation(description: "Drop the table and delete the keyspace")
        
        try connection.connect() { result in

             XCTAssertNil(result.asError)
        
            let _ = self.connection["test"]
            
            TodoItem.truncate().execute() { result in
                self.connection.execute(query: "DROP KEYSPACE test;") { result in
                    
                    if result.success { expectation1.fulfill() }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
        
    }
    
    /*func testPreparedQuery() throws {
     
        let expectation1 = expectation(description: "Execute a prepared query")

        var query: Query = Raw(query: "SELECT userID FROM todoitem WHERE completed = true allow filtering;")

        try connection.connect() { result in
            
            XCTAssertNil(result.asError)
            
            let _ = self.connection["test"]
            
            query.prepare() { result in
                if let id = result.asPrepared {
                    
                    query.preparedID = id
                    
                    query.execute() { result in
                        
                        if result.success { expectation1.fulfill() }
                    }
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
     }*/
    
    public func testZBatch() throws {
        let expectation1 = expectation(description: "Execute a batch query")
        
        var insert1 = TodoItem.insert(values: [TodoItem.type: "todo", TodoItem.userID: 99, TodoItem.title: "Water Plants", TodoItem.pos: 15, TodoItem.completed: false])
        let insert2 = TodoItem.insert(values: [TodoItem.type: "todo", TodoItem.userID: 98,TodoItem.title: "Make Dinner", TodoItem.pos: 14, TodoItem.completed: true])
        let insert3 = TodoItem.insert(values: [TodoItem.type: "todo", TodoItem.userID: 97,TodoItem.title: "Excercise", TodoItem.pos: 13, TodoItem.completed: true])
        let insert4 = TodoItem.insert(values: [TodoItem.type: "todo", TodoItem.userID: 96,TodoItem.title: "Sprint Plannning", TodoItem.pos: 12, TodoItem.completed: false])
        
        try connection.connect() { result in
            
            XCTAssertNil(result.asError)
            
            let _ = self.connection["test"]
    
            insert1.execute(self.connection) { result in
            
                [insert1,insert2,insert3,insert4].execute(with: .logged, consis: .any) { result in

                    if result.success { expectation1.fulfill() }
                    
                }
            }
        }
        waitForExpectations(timeout: 5, handler: { error in XCTAssertNil(error, "Timeout") })
    }*/
}

