//
//  Futures.swift
//  Kassandra
//
//  Created by Aaron Liberatore on 7/30/16.
//
//

import Foundation

protocol Finishable {
    func done(done: (() -> ())) -> ()
}

class Promise : Finishable {

    var pending: [(() -> ())] = []
    
    var done: (() -> ()) = {}
    
    var fail: (() -> ()) = {}
    
    var rejected: Bool = false
    
    class func deferred() -> Promise {
        return Promise()
    }
    
    func resolve() -> (() -> ()) {
        func resolve() -> () {
            for f in self.pending {
                if self.rejected {
                    fail()
                    return
                }
                f()
            }
            if self.rejected {
                fail()
                return
            }
            done()
        }
        return resolve
    }
    
    func reject() -> () {
        self.rejected = true
    }

    func then(callback: (() -> ())) -> Promise {
        self.pending.append(callback)
        return self
    }
    
    func then(callback: ((promise: Promise) -> ())) -> Promise {
        func thenWrapper() -> () {
            callback(promise: self)
        }
        self.pending.append(thenWrapper)
        return self
    }
    
    func fail(fail: (() -> ())) -> Finishable {
        self.fail = fail
        let finishablePromise : Finishable = self
        return finishablePromise
    }

    func done(done: (() -> ())) -> () {
        self.done = done
    }
}
