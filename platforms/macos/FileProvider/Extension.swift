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

let providerGroup = "group.com.xuanwo.yinyang.prototype"

func providerError(_ error: Error) -> Error {
  guard let error = error as? NativeFailure else { return error }
  switch error.kind {
  case "NotFound": return NSFileProviderError(.noSuchItem)
  case "Conflict": return NSFileProviderError(.cannotSynchronize)
  case "Storage", "Io", "Retryable", "Unknown": return NSFileProviderError(.serverUnreachable)
  default: return NSFileProviderError(.cannotSynchronize)
  }
}
final class ProviderSession {
  let queue = DispatchQueue(label: "org.apache.yinyang.provider")
  let domain: NSFileProviderDomain
  var bridge: NativeBridge?
  var root: String?
  var enumerators: [NSFileProviderItemIdentifier: Int] = [:]
  private var polling: DispatchSourceTimer?
  private var observedRevision: String?
  init(domain: NSFileProviderDomain) { self.domain = domain }
  func startPolling() {
    queue.async { [self] in
      let timer = DispatchSource.makeTimerSource(queue: self.queue)
      timer.schedule(deadline: .now(), repeating: 5)
      timer.setEventHandler { [weak self] in
        guard let self else { return }
        do {
          let revision = try self.connected().item(["op": "root"]).revision
          guard revision != self.observedRevision else { return }
          self.observedRevision = revision
          guard let manager = NSFileProviderManager(for: self.domain) else { return }
          for identifier in Set(self.enumerators.keys).union([.workingSet]) {
            manager.signalEnumerator(for: identifier) { _ in }
          }
        } catch { /* A later poll retries without advancing the observation. */  }
      }
      self.polling = timer
      timer.resume()
    }
  }
  func invalidate() {
    queue.async {
      self.polling?.cancel()
      self.polling = nil
      self.bridge = nil
    }
  }
  func connected() throws -> NativeBridge {
    if let bridge { return bridge }
    guard
      let directory = FileManager.default.containerURL(
        forSecurityApplicationGroupIdentifier: providerGroup)
    else { throw NSFileProviderError(.notAuthenticated) }
    // Domain IDs are generated UUIDs, never caller-controlled file paths.
    guard let id = UUID(uuidString: domain.identifier.rawValue) else { throw POSIXError(.EINVAL) }
    let folder = directory.appendingPathComponent(id.uuidString)
    let data = try Data(contentsOf: folder.appendingPathComponent("volume.json"))
    guard var config = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
      throw POSIXError(.EINVAL)
    }
    config["staging"] = folder.appendingPathComponent("sync-state").path
    let opened = try NativeBridge(config: config, mode: "sync")
    root = try opened.item(["op": "root"]).node
    bridge = opened
    return opened
  }
  func identity(_ identifier: NSFileProviderItemIdentifier) throws -> String {
    _ = try connected()
    return identifier == .rootContainer || identifier == .workingSet ? root! : identifier.rawValue
  }
  func item(_ id: NSFileProviderItemIdentifier, revision: String? = nil) throws -> ProviderItem {
    let bridge = try connected()
    var request: [String: Any] = ["op": "node", "node": try identity(id)]
    if let revision { request["revision"] = revision }
    return ProviderItem(try bridge.item(request), root: root!)
  }
}
final class FileProviderExtension: NSObject, NSFileProviderReplicatedExtension,
  NSFileProviderPartialContentFetching
{
  let session: ProviderSession
  init(session: ProviderSession) {
    self.session = session
    super.init()
  }
  required convenience init(domain: NSFileProviderDomain) {
    self.init(session: ProviderSession(domain: domain))
    session.startPolling()
  }
  func invalidate() { session.invalidate() }
  func item(
    for identifier: NSFileProviderItemIdentifier, request: NSFileProviderRequest,
    completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void
  ) -> Progress {
    let progress = Progress(totalUnitCount: 1)
    session.queue.async {
      do {
        if progress.isCancelled { throw CocoaError(.userCancelled) }
        completionHandler(try self.session.item(identifier), nil)
        progress.completedUnitCount = 1
      } catch { completionHandler(nil, providerError(error)) }
    }
    return progress
  }
  private func fetch(
    _ identifier: NSFileProviderItemIdentifier, version: NSFileProviderItemVersion?,
    range: NSRange?, alignment: Int,
    completion: @escaping (URL?, NSFileProviderItem?, NSRange, Error?) -> Void
  ) -> Progress {
    let progress = Progress(totalUnitCount: 1)
    session.queue.async {
      var temporary: URL?
      do {
        if progress.isCancelled { throw CocoaError(.userCancelled) }
        let revision = version.flatMap { String(data: $0.contentVersion, encoding: .utf8) }
        if version != nil && revision == nil {
          throw NSFileProviderError(.versionNoLongerAvailable)
        }
        let item = try self.session.item(identifier, revision: revision)
        guard !item.metadata.directory, item.metadata.size <= UInt64(Int.max) else {
          throw POSIXError(.EFBIG)
        }
        let size = Int(item.metadata.size)
        let lower: Int
        let upper: Int
        if let range {
          guard alignment > 0, range.location != NSNotFound, range.location <= size,
            range.length <= size - range.location
          else { throw POSIXError(.EINVAL) }
          lower = range.location / alignment * alignment
          let end = range.location + range.length
          let padding = (alignment - end % alignment) % alignment
          upper = end + min(padding, size - end)
        } else {
          lower = 0
          upper = size
        }
        guard let manager = NSFileProviderManager(for: self.session.domain) else {
          throw NSFileProviderError(.noSuchItem)
        }
        let directory = try manager.temporaryDirectoryURL()
        let output = directory.appendingPathComponent(UUID().uuidString)
        temporary = output
        guard
          FileManager.default.createFile(
            atPath: output.path, contents: nil, attributes: [.posixPermissions: 0o600])
        else { throw POSIXError(.EIO) }
        let file = try FileHandle(forWritingTo: output)
        defer { try? file.close() }
        try file.seek(toOffset: UInt64(lower))
        var position = lower
        progress.totalUnitCount = Int64(upper - lower)
        let bridge = try self.session.connected()
        while position < upper {
          if progress.isCancelled { throw CocoaError(.userCancelled) }
          let count = min(1024 * 1024, upper - position)
          let bytes = try bridge.read(
            node: item.metadata.node, offset: UInt64(position), length: count,
            revision: item.metadata.revision)
          guard bytes.count == count else { throw POSIXError(.EIO) }
          try file.write(contentsOf: bytes)
          position += count
          progress.completedUnitCount = Int64(position - lower)
        }
        try file.synchronize()
        try file.close()
        if progress.isCancelled { throw CocoaError(.userCancelled) }
        completion(output, item, NSRange(location: lower, length: upper - lower), nil)
        temporary = nil  // The OS owns the returned download.
      } catch {
        if let temporary { try? FileManager.default.removeItem(at: temporary) }
        completion(nil, nil, NSRange(location: 0, length: 0), providerError(error))
      }
    }
    return progress
  }
  func fetchContents(
    for itemIdentifier: NSFileProviderItemIdentifier,
    version requestedVersion: NSFileProviderItemVersion?,
    request: NSFileProviderRequest,
    completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void
  ) -> Progress {
    fetch(itemIdentifier, version: requestedVersion, range: nil, alignment: 1) {
      url, item, _, error in completionHandler(url, item, error)
    }
  }
  func fetchPartialContents(
    for itemIdentifier: NSFileProviderItemIdentifier,
    version requestedVersion: NSFileProviderItemVersion,
    request: NSFileProviderRequest, minimalRange: NSRange, aligningTo: Int,
    options: NSFileProviderFetchContentsOptions,
    completionHandler:
      @escaping (URL?, NSFileProviderItem?, NSRange, NSFileProviderMaterializationFlags, Error?) ->
      Void
  ) -> Progress {
    fetch(itemIdentifier, version: requestedVersion, range: minimalRange, alignment: aligningTo) {
      url, item, range, error in completionHandler(url, item, range, [], error)
    }
  }
  func modifyItem(
    _ item: NSFileProviderItem, baseVersion version: NSFileProviderItemVersion,
    changedFields: NSFileProviderItemFields,
    contents newContents: URL?, options: NSFileProviderModifyItemOptions,
    request: NSFileProviderRequest,
    completionHandler:
      @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void
  ) -> Progress {
    let progress = Progress(totalUnitCount: 1)
    session.queue.async {
      do {
        guard changedFields.contains(.contents), let url = newContents,
          let revision = String(data: version.contentVersion, encoding: .utf8)
        else { throw CocoaError(.featureUnsupported) }
        if progress.isCancelled { throw CocoaError(.userCancelled) }
        let bridge = try self.session.connected()
        let edit = try bridge.call([
          "op": "stage", "node": try self.session.identity(item.itemIdentifier),
          "revision": revision, "path": url.path,
        ])
        if progress.isCancelled { throw CocoaError(.userCancelled) }
        let committed = try bridge.call(["op": "publish", "edit": edit["edit"]!])
        guard let accepted = committed["revision"] as? String else { throw POSIXError(.EIO) }
        let result = try self.session.item(item.itemIdentifier, revision: accepted)
        progress.completedUnitCount = 1
        // The OS associates this completion with this local version; a
        // later local edit is never acknowledged by a separate flag.
        completionHandler(result, changedFields.subtracting(.contents), false, nil)
      } catch { completionHandler(nil, changedFields, false, providerError(error)) }
    }
    return progress
  }
  func createItem(
    basedOn itemTemplate: NSFileProviderItem, fields: NSFileProviderItemFields, contents url: URL?,
    options: NSFileProviderCreateItemOptions, request: NSFileProviderRequest,
    completionHandler:
      @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void
  ) -> Progress {
    completionHandler(nil, fields, false, CocoaError(.featureUnsupported))
    return Progress()
  }
  func deleteItem(
    identifier: NSFileProviderItemIdentifier, baseVersion: NSFileProviderItemVersion,
    options: NSFileProviderDeleteItemOptions, request: NSFileProviderRequest,
    completionHandler: @escaping (Error?) -> Void
  ) -> Progress {
    completionHandler(CocoaError(.featureUnsupported))
    return Progress()
  }
  func enumerator(
    for containerItemIdentifier: NSFileProviderItemIdentifier, request: NSFileProviderRequest
  ) throws -> NSFileProviderEnumerator {
    ProviderEnumerator(session: session, container: containerItemIdentifier)
  }
}
