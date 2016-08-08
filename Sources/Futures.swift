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

public protocol Finishable {
    func done(done: (() -> ())) -> ()
}

public class Promise<T> : Finishable {

    var pending: [((T) -> ())] = []
    
    var done: (() -> ()) = { }
    
    var fail: ((Error) -> ()) = { _ in }
    
    var rejected: Bool = false
    
    var error: Error = RCErrorType.GenericError("")

    public class func deferred() -> Promise {
        return Promise<T>()
    }
    
    public func scatterMap() -> Promise {
        //[].map
        return Promise<T>()
    }
    public func resolve() -> ((T) -> ()) {
        func res(x: T) -> () {
            for f in self.pending {
                if self.rejected {
                    fail(error)
                    return
                }
                f(x)
            }
            if self.rejected {
                fail(error)
                return
            }
            done()
        }
        return res
    }

    public func reject(dueTo error: Error) -> () {
        self.error = error
        self.rejected = true
        fail(error)
        return
    }

    public func then(callback: ((T) -> ())) -> Promise {
        self.pending.append(callback)
        return self
    }
    @discardableResult
    public func fail(fail: ((Error) -> ())) -> Finishable {
        self.fail = fail
        let finishablePromise : Finishable = self
        return finishablePromise
    }

    public func done(done: (() -> ())) -> () {
        self.done = done
    }
}
