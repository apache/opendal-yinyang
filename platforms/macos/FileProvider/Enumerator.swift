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

private struct ProviderCursor: Codable {
  let domain: String
  let container: String
  let revision: String
  var ordinal: UInt32 = .max
  var offset: Int = 0
}

final class ProviderEnumerator: NSObject, NSFileProviderEnumerator {
  let session: ProviderSession
  let container: NSFileProviderItemIdentifier
  private var invalidated = false
  init(session: ProviderSession, container: NSFileProviderItemIdentifier) {
    self.session = session
    self.container = container
    super.init()
    session.queue.async { session.enumerators[container, default: 0] += 1 }
  }
  func invalidate() {
    session.queue.async {
      guard !self.invalidated else { return }
      self.invalidated = true
      let count = self.session.enumerators[self.container, default: 1] - 1
      if count == 0 {
        self.session.enumerators.removeValue(forKey: self.container)
      } else {
        self.session.enumerators[self.container] = count
      }
    }
  }
  private func decode(_ data: Data) throws -> ProviderCursor {
    guard data.count <= 500,
      let cursor = try? JSONDecoder().decode(ProviderCursor.self, from: data),
      cursor.domain == session.domain.identifier.rawValue,
      cursor.container == container.rawValue, cursor.offset >= 0, cursor.offset <= 1_000_000
    else {
      throw NSFileProviderError(.syncAnchorExpired)
    }
    return cursor
  }
  private func current() throws -> ProviderCursor {
    let item = try session.item(.rootContainer)
    return ProviderCursor(
      domain: session.domain.identifier.rawValue,
      container: container.rawValue, revision: item.metadata.revision)
  }
  func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
    session.queue.async {
      guard !self.invalidated, let cursor = try? self.current(),
        let data = try? JSONEncoder().encode(cursor)
      else { return completionHandler(nil) }
      completionHandler(NSFileProviderSyncAnchor(data))
    }
  }
  func enumerateItems(
    for observer: NSFileProviderEnumerationObserver, startingAt page: NSFileProviderPage
  ) {
    session.queue.async {
      do {
        guard !self.invalidated else { throw CocoaError(.userCancelled) }
        let bridge = try self.session.connected()
        var cursor: ProviderCursor
        if page.rawValue == NSFileProviderPage.initialPageSortedByDate as Data
          || page.rawValue == NSFileProviderPage.initialPageSortedByName as Data
        {
          cursor = try self.current()
          let item = try self.session.item(self.container, revision: cursor.revision)
          if !item.metadata.directory {
            observer.didEnumerate([item])
            observer.finishEnumerating(upTo: nil)
            return
          }
        } else {
          cursor = try self.decode(page.rawValue)
        }
        // Persist only revision + ordinal: provider tokens are limited
        // to 500 bytes. Rewalk the immutable tree after restart instead
        // of embedding a potentially unbounded directory queue.
        var directories = [try self.session.identity(self.container)]
        var items: [NSFileProviderItem] = []
        var ordinal = 0
        var directoryIndex = 0
        let limit = min(256, max(1, observer.suggestedPageSize ?? 256))
        walk: while directoryIndex < directories.count {
          let directory = directories[directoryIndex]
          var offset = 0
          while true {
            let result = try bridge.call([
              "op": "scan", "node": directory, "revision": cursor.revision,
              "offset": offset, "limit": 256,
            ])
            guard let entries = result["entries"] as? [[String: Any]] else {
              throw POSIXError(.EIO)
            }
            for entry in entries {
              let item = ProviderItem(try NativeItem(entry), root: self.session.root!)
              if self.container == .workingSet && item.metadata.directory {
                directories.append(item.metadata.node)
              }
              if ordinal >= cursor.offset { items.append(item) }
              ordinal += 1
              if items.count == limit { break walk }
            }
            guard let next = result["next"] as? Int else { break }
            offset = next
          }
          directoryIndex += 1
        }
        cursor.offset += items.count
        let next = items.count == limit ? NSFileProviderPage(try JSONEncoder().encode(cursor)) : nil
        observer.didEnumerate(items)
        observer.finishEnumerating(upTo: next)
      } catch { observer.finishEnumeratingWithError(self.enumerationError(error)) }
    }
  }
  func enumerateChanges(
    for observer: NSFileProviderChangeObserver, from anchor: NSFileProviderSyncAnchor
  ) {
    session.queue.async {
      do {
        guard !self.invalidated else { throw CocoaError(.userCancelled) }
        var cursor = try self.decode(anchor.rawValue)
        let bridge = try self.session.connected()
        let identity = try self.session.identity(self.container)
        let result = try bridge.call([
          "op": "changes", "revision": cursor.revision,
          "ordinal": cursor.ordinal, "limit": 1,
        ])
        guard let changes = result["changes"] as? [[String: Any]],
          let revision = result["revision"] as? String,
          let ordinal = result["ordinal"] as? UInt32, let more = result["more"] as? Bool
        else { throw POSIXError(.EIO) }
        var updates: [String: ProviderItem] = [:]
        var deletions: Set<String> = []
        guard cursor.offset <= changes.count else { throw NSFileProviderError(.syncAnchorExpired) }
        let end = min(
          changes.count, cursor.offset + min(256, max(1, observer.suggestedBatchSize ?? 256)))
        for change in changes[cursor.offset..<end] {
          let before = try (change["before"] as? [String: Any]).map { try NativeItem($0) }
          let after = try (change["after"] as? [String: Any]).map { try NativeItem($0) }
          func included(_ item: NativeItem?) -> Bool {
            guard let item else { return false }
            return self.container == .workingSet || item.parent == identity || item.node == identity
          }
          if let after, included(after) {
            updates[after.node] = ProviderItem(after, root: self.session.root!)
            deletions.remove(after.node)
          } else if let before, included(before) {
            deletions.insert(before.node)
            updates.removeValue(forKey: before.node)
          }
        }
        let partial = end < changes.count
        if partial {
          cursor.offset = end
        } else {
          cursor = ProviderCursor(
            domain: cursor.domain, container: cursor.container, revision: revision, ordinal: ordinal
          )
        }
        observer.didDeleteItems(
          withIdentifiers: deletions.map {
            $0 == self.session.root! ? .rootContainer : NSFileProviderItemIdentifier($0)
          })
        observer.didUpdate(Array(updates.values))
        observer.finishEnumeratingChanges(
          upTo: NSFileProviderSyncAnchor(try JSONEncoder().encode(cursor)),
          moreComing: partial || more)
      } catch { observer.finishEnumeratingWithError(self.enumerationError(error)) }
    }
  }
  private func enumerationError(_ error: Error) -> Error {
    if let native = error as? NativeFailure, native.kind == "Invalid" {
      return NSFileProviderError(.syncAnchorExpired)
    }
    return providerError(error)
  }
}
