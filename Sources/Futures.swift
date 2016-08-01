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
