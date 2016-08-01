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
import Socket

public struct Supported: Response {
    
    var map = [String: [String]]()
    
    public var description: String {
        return "Supports: \(map)"
    }

    public init(body: Data) {
        var body = body

        for _ in 0..<Int(body.decodeUInt16) {
            let key = body.decodeString
            var strList = [String]()
            let strListLen = Int(body.decodeUInt16)

            for _ in 0..<strListLen {
                strList.append(body.decodeString)
            }

            map[key] = strList
        }
    }
}
