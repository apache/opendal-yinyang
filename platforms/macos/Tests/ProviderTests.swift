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

import FileProvider
import Foundation

final class PageObserver: NSObject, NSFileProviderEnumerationObserver {
    let done = DispatchSemaphore(value: 0)
    var items: [NSFileProviderItem] = []
    var next: NSFileProviderPage?
    var error: Error?
    var suggestedPageSize: Int { 2 }
    func didEnumerate(_ items: [NSFileProviderItem]) { self.items += items }
    func finishEnumerating(upTo nextPage: NSFileProviderPage?) { next = nextPage; done.signal() }
    func finishEnumeratingWithError(_ error: Error) { self.error = error; done.signal() }
    func wait() throws {
        precondition(done.wait(timeout: .now() + 60) == .success, "enumeration timed out")
        if let error { throw error }
        precondition(next == nil || next!.rawValue.count <= 500)
    }
}
final class ChangeObserver: NSObject, NSFileProviderChangeObserver {
    let done = DispatchSemaphore(value: 0)
    var updated: [NSFileProviderItem] = []
    var deleted: [NSFileProviderItemIdentifier] = []
    var anchor: NSFileProviderSyncAnchor?
    var more = false
    var error: Error?
    var suggestedBatchSize: Int { 1 }
    func didUpdate(_ items: [NSFileProviderItem]) { updated += items }
    func didDeleteItems(withIdentifiers identifiers: [NSFileProviderItemIdentifier]) { deleted += identifiers }
    func finishEnumeratingChanges(upTo anchor: NSFileProviderSyncAnchor, moreComing: Bool) {
        self.anchor = anchor; more = moreComing; done.signal()
    }
    func finishEnumeratingWithError(_ error: Error) { self.error = error; done.signal() }
    func wait() throws {
        precondition(done.wait(timeout: .now() + 60) == .success, "changes timed out")
        if let error { throw error }
        precondition(anchor!.rawValue.count <= 500)
    }
}

@main
struct ProviderTests {
    static func main() throws {
        precondition(CommandLine.arguments.count == 2, "an isolated native configuration is required")
        var config = try JSONSerialization.jsonObject(with: Data(contentsOf: URL(fileURLWithPath: CommandLine.arguments[1]))) as! [String: Any]
        let staging = FileManager.default.temporaryDirectory.appendingPathComponent("yinyang-provider-tests-\(UUID())")
        try FileManager.default.createDirectory(at: staging, withIntermediateDirectories: false)
        config["staging"] = staging.appendingPathComponent("mount").path
        let mount = try NativeBridge(config: config, mode: "mount")
        let root = try mount.item(["op": "root"]).node
        let name = "provider-tests-\(UUID())"
        let folder = try mount.item(["op": "create", "parent": root, "name": name, "directory": true])
        var files: [NativeItem] = []
        for name in ["a", "b", "c"] {
            files.append(try mount.item(["op": "create", "parent": folder.node, "name": name, "directory": false]))
        }
        let nested = try mount.item(["op": "create", "parent": folder.node, "name": "nested", "directory": true])
        let leaf = try mount.item(["op": "create", "parent": nested.node, "name": "leaf", "directory": false])
        config["staging"] = staging.appendingPathComponent("sync").path
        let sync = try NativeBridge(config: config, mode: "sync")
        let domain = NSFileProviderDomain(identifier: NSFileProviderDomainIdentifier(UUID().uuidString), displayName: "Unregistered callback test")
        let session = ProviderSession(domain: domain)
        session.bridge = sync; session.root = root
        let container = NSFileProviderItemIdentifier(folder.node)
        let firstEnumerator = ProviderEnumerator(session: session, container: container)
        let first = PageObserver()
        firstEnumerator.enumerateItems(for: first, startingAt: NSFileProviderPage(NSFileProviderPage.initialPageSortedByName as Data))
        try first.wait()
        precondition(first.items.count == 2 && first.next != nil)
        firstEnumerator.invalidate()
        _ = try mount.item(["op": "create", "parent": folder.node, "name": "aa", "directory": false])
        var names = first.items.map(\.filename)
        var page = first.next
        while let token = page {
            let enumerator = ProviderEnumerator(session: session, container: container)
            let observer = PageObserver()
            enumerator.enumerateItems(for: observer, startingAt: token)
            try observer.wait()
            names += observer.items.map(\.filename); page = observer.next
            enumerator.invalidate()
        }
        precondition(names == ["a", "b", "c", "nested"], "pages mixed revisions")
        print("pinned pages survive enumerator recreation and concurrent namespace edits PASS")

        let working = ProviderEnumerator(session: session, container: .workingSet)
        page = NSFileProviderPage(NSFileProviderPage.initialPageSortedByName as Data)
        var identities: Set<String> = []
        while let token = page {
            let observer = PageObserver()
            working.enumerateItems(for: observer, startingAt: token)
            try observer.wait()
            for item in observer.items { precondition(identities.insert(item.itemIdentifier.rawValue).inserted) }
            page = observer.next
        }
        precondition(identities.contains(leaf.node))
        let anchored = DispatchSemaphore(value: 0)
        var anchor: NSFileProviderSyncAnchor?
        working.currentSyncAnchor { anchor = $0; anchored.signal() }
        precondition(anchored.wait(timeout: .now() + 60) == .success && anchor != nil)
        try mount.call(["op": "remove", "node": files[1].node])
        var deleted: Set<String> = []
        while true {
            let observer = ChangeObserver()
            working.enumerateChanges(for: observer, from: anchor!)
            try observer.wait()
            deleted.formUnion(observer.deleted.map(\.rawValue))
            anchor = observer.anchor
            if !observer.more { break }
        }
        precondition(deleted.contains(files[1].node))
        working.invalidate()
        print("recursive working set, bounded anchors and split change receipts PASS")

        let adapter = FileProviderExtension(session: session)
        let item = try session.item(NSFileProviderItemIdentifier(files[0].node))
        let upload = staging.appendingPathComponent("upload")
        try Data("provider-upload".utf8).write(to: upload)
        func modify(expectError: Bool) {
            let done = DispatchSemaphore(value: 0)
            var failure: Error?
            _ = adapter.modifyItem(item, baseVersion: item.itemVersion, changedFields: .contents, contents: upload,
                                   options: [], request: NSFileProviderRequest()) { result, fields, pending, error in
                failure = error
                if error == nil { precondition(result != nil && fields.isEmpty && !pending) }
                done.signal()
            }
            precondition(done.wait(timeout: .now() + 60) == .success)
            precondition((failure != nil) == expectError)
        }
        modify(expectError: false)
        modify(expectError: false)
        try Data("conflicting-upload".utf8).write(to: upload)
        modify(expectError: true)
        let accepted = try sync.item(["op": "node", "node": files[0].node])
        let bytes = try sync.read(node: accepted.node, offset: 0, length: 64, revision: accepted.revision)
        precondition(bytes == Data("provider-upload".utf8))
        adapter.invalidate()
        print("callback upload, identical retry and conditional conflict retention PASS")
        print("Remote test subtree retained: \(name); staging retained: \(staging.path)")
        print("These are real-core callback tests, not OS domain acceptance.")
    }
}
