// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


import Foundation

struct NativeFailure: Error {
    let kind: String
    let message: String
    var posix: Error {
        let code: POSIXErrorCode
        switch kind {
        case "NotFound": code = .ENOENT
        case "AlreadyExists": code = .EEXIST
        case "NotDirectory": code = .ENOTDIR
        case "IsDirectory": code = .EISDIR
        case "NotEmpty": code = .ENOTEMPTY
        case "ReadOnly": code = .EROFS
        case "PermissionDenied": code = .EACCES
        case "Busy", "Frozen": code = .EBUSY
        case "Conflict", "Retryable": code = .EAGAIN
        case "Invalid", "InvalidName": code = .EINVAL
        case "Unsupported": code = .ENOTSUP
        case "NoSpace": code = .ENOSPC
        case "TooLarge": code = .EFBIG
        default: code = .EIO
        }
        return POSIXError(code)
    }
}
struct NativeItem: Decodable {
    let node: String
    let parent: String?
    let name: String
    let directory: Bool
    var size: UInt64
    let executable: Bool
    let generation: UInt64
    let revision: String
    init(_ value: [String: Any]) throws {
        self = try JSONDecoder().decode(Self.self, from: JSONSerialization.data(withJSONObject: value))
    }
}
final class NativeBridge {
    private let session: UInt64
    static func invoke(_ value: [String: Any]) throws -> [String: Any] {
        let input = try JSONSerialization.data(withJSONObject: value)
        let output = input.withUnsafeBytes {
            yy_native_call($0.baseAddress?.assumingMemoryBound(to: UInt8.self), $0.count)
        }
        guard let output else { throw POSIXError(.ENOMEM) }
        defer { yy_native_free(output) }
        let result = try JSONSerialization.jsonObject(with: Data(String(cString: output).utf8)) as? [String: Any]
        if let error = result?["error"] as? [String: Any] {
            throw NativeFailure(kind: error["kind"] as? String ?? "Io",
                                message: error["message"] as? String ?? "Native operation failed")
        }
        guard let ok = result?["ok"] as? [String: Any] else { throw POSIXError(.EIO) }
        return ok
    }
    init(config: [String: Any], mode: String) throws {
        let result = try Self.invoke(["action": "connect", "config": config, "mode": mode])
        guard let id = result["session"] as? NSNumber else { throw POSIXError(.EIO) }
        session = id.uint64Value
    }
    deinit { _ = try? Self.invoke(["action": "close", "session": session]) }
    @discardableResult
    func call(_ request: [String: Any]) throws -> [String: Any] {
        try Self.invoke(["action": "call", "session": session, "request": request])
    }
    func item(_ request: [String: Any]) throws -> NativeItem { try NativeItem(call(request)) }
    func read(node: String, offset: UInt64, length: Int, revision: String? = nil) throws -> Data {
        var output = Data()
        while output.count < length {
            let count = min(length - output.count, 1024 * 1024)
            var request: [String: Any] = ["op": revision == nil ? "read" : "download",
                "node": node, "offset": offset + UInt64(output.count), "length": count]
            if let revision { request["revision"] = revision }
            let response = try call(request)
            guard let bytes = response["bytes"] as? [UInt8] else { throw POSIXError(.EIO) }
            output.append(contentsOf: bytes)
            if bytes.count < count { break }
        }
        return output
    }
    func write(node: String, offset: UInt64, data: Data) throws {
        var position = 0
        while position < data.count {
            let end = min(position + 1024 * 1024, data.count)
            try call(["op": "write", "node": node, "offset": offset + UInt64(position),
                      "bytes": Array(data[position..<end])])
            position = end
        }
    }
}
