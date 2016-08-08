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

import SSLService

public typealias Byte = UInt8

public var config = Config.sharedInstance

public struct Config {
    
    let CQL_MAX_SUPPORTED_VERSION: UInt8 = 0x03
    let version: Byte = 0x03
    var connection: Kassandra? = nil

    var map = [UInt16: (TableObj?, Error?) -> Void]()

    var SSLConfig: SSLService.Configuration? = nil

    static var sharedInstance = Config()
    
    private init(){}
}
