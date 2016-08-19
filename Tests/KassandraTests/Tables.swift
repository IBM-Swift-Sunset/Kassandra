//
//  Student.swift
//  Kassandra
//
//  Created by Aaron Liberatore on 8/12/16.
//
//

import Foundation
import Kassandra

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

public class BreadShop: Table {
    public enum Field: String {
        case type = "type"
        case userID = "userID"
        case time = "time"
        case name = "name"
        case cost = "cost"
        case rate = "rate"
    }
    
    public static var tableName: String = "breadshop"
    
}

public class TestScore: Table {
    public enum Field: String {
        case commit = "commit"
        case score = "score"
        case userID = "userID"
        case subject = "subject"
        case time = "time"
        case userip = "userip"
    }
    
    public static var tableName: String = "testscore"
    
}

public class IceCream: Table {
    public enum Field: String {
        case id = "id"
        case calories = "calories"
        case name = "name"
        case price = "price"
        case flavors = "flavors"
    }
    
    public static var tableName: String = "icecream"
    
}

public class BookCollection: Table {
    public enum Field: String {
        case id = "id"
        case name = "name"
        case price = "price"
        case series = "series"
        case emails = "emails"
    }
    
    public static var tableName: String = "bookcollection"
}

public class CollectRanData: Table {
    public enum Field: String {
        case id = "id"
        case numbers = "numbers"
    }
    
    public static var tableName: String = "collectrandata"
}
