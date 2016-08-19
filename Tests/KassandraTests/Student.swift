//
//  Student.swift
//  Kassandra
//
//  Created by Aaron Liberatore on 8/12/16.
//
//

import Foundation
import SwiftKuery

public class TodoItem: Table {
    static let type = Field("type")
    static let userID = Field("userID")
    static let title = Field("title")
    static let pos = Field("pos")
    static let completed = Field("completed")

    public static let name: String = "todoitem"

}


public class BreadShop: Table {
        static let type = Field("type")
        static let userID = Field("userID")
        static let time = Field("time")
        static let breadname = Field("breadname")
        static let cost = Field("cost")
        static let rate = Field("rate")
    
    public static let name: String = "breadshop"
}

public class TestScore: Table {
        static let commit = Field("commit")
        static let score = Field("score")
        static let userID = Field("userID")
        static let subject = Field("subject")
        static let time = Field("time")
        static let userip = Field("userip")
    
    public static let name: String = "testscore"
}

public class IceCream: Table {
        static let id = Field("id")
        static let calories = Field("calories")
        static let icecreamname = Field("icecreamname")
        static let price = Field("price")
        static let flavors = Field("flavors")
    
    public static let name: String = "icecream"
}

public class BookCollection: Table {
        static let id = Field("id")
        static let bookname = Field("bookname")
        static let price = Field("price")
        static let series = Field("series")
        static let emails = Field("emails")
    
    public static let name: String = "bookcollection"
}

public class CollectRanData: Table {
        static let id = Field("id")
        static let numbers = Field("numbers")
    
    public static let name: String = "collectrandata"
}
