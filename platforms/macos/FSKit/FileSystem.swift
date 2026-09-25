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
import FSKit
import OSLog

@main
struct YinYangExtension: UnaryFileSystemExtension {
    var fileSystem: FSUnaryFileSystem & FSUnaryFileSystemOperations { YinYangFileSystem() }
}
final class YinYangFileSystem: FSUnaryFileSystem, FSUnaryFileSystemOperations {
    private var resource: FSPathURLResource?
    private let queue = DispatchQueue(label: "org.apache.yinyang.fskit.lifecycle")
    func probeResource(resource: FSResource, replyHandler: @escaping (FSProbeResult?, Error?) -> Void) {
        guard resource is FSPathURLResource else { return replyHandler(nil, POSIXError(.ENODEV)) }
        replyHandler(.usable(name: "YinYang", containerID: FSContainerIdentifier(uuid: UUID())), nil)
    }
    func loadResource(resource: FSResource, options: FSTaskOptions, replyHandler: @escaping (FSVolume?, Error?) -> Void) {
        queue.async {
            guard self.resource == nil, let resource = resource as? FSPathURLResource,
                  resource.url.startAccessingSecurityScopedResource() else {
                return replyHandler(nil, POSIXError(.EACCES))
            }
            do {
                let data = try Data(contentsOf: resource.url.appendingPathComponent("volume.json"))
                guard var config = try JSONSerialization.jsonObject(with: data) as? [String: Any] else { throw POSIXError(.EINVAL) }
                // Staging stays inside the explicitly selected isolated resource.
                config["staging"] = resource.url.appendingPathComponent("mount-state").path
                let bridge = try NativeBridge(config: config, mode: "mount")
                let volume = try YinYangVolume(bridge: bridge)
                self.resource = resource
                self.containerStatus = .ready
                replyHandler(volume, nil)
            } catch {
                resource.url.stopAccessingSecurityScopedResource()
                replyHandler(nil, (error as? NativeFailure)?.posix ?? error)
            }
        }
    }
    func unloadResource(resource: FSResource, options: FSTaskOptions, replyHandler: @escaping (Error?) -> Void) {
        queue.async {
            self.resource?.url.stopAccessingSecurityScopedResource()
            self.resource = nil
            replyHandler(nil)
        }
    }
}
final class YinYangItem: FSItem {
    let identity: String
    let number: UInt64
    var metadata: NativeItem
    init(_ metadata: NativeItem, number: UInt64) {
        identity = metadata.node
        self.number = number
        self.metadata = metadata
        super.init()
    }
}
final class YinYangVolume: FSVolume, FSVolume.Handler, FSVolume.ReadWriteHandler, FSVolume.DataCacheHandler {
    let bridge: NativeBridge
    let root: YinYangItem
    private let queue = DispatchQueue(label: "org.apache.yinyang.fskit.requests")
    private var items: [String: YinYangItem] = [:]
    private var numbers: [String: UInt64] = [:]
    private var nextID: UInt64 = 3
    private var enumerations: [UInt64: (String, String)] = [:]
    private var nextVerifier: UInt64 = 1
    private var references: [String: Int] = [:]
    private let logger = Logger(subsystem: "org.apache.yinyang.fskit", category: "io")
    init(bridge: NativeBridge) throws {
        self.bridge = bridge
        let metadata = try bridge.item(["op": "root"])
        root = YinYangItem(metadata, number: 2)
        super.init(volumeID: FSVolume.Identifier(uuid: UUID(uuidString:
            metadata.node.prefix(8) + "-" + metadata.node.dropFirst(8).prefix(4) + "-" +
            metadata.node.dropFirst(12).prefix(4) + "-" + metadata.node.dropFirst(16).prefix(4) + "-" +
            metadata.node.dropFirst(20)) ?? UUID()), volumeName: FSFileName(string: "YinYang"))
        items[root.identity] = root
        numbers[root.identity] = 2
    }
    private func item(_ raw: FSItem) throws -> YinYangItem {
        guard let item = raw as? YinYangItem else { throw POSIXError(.EINVAL) }
        return item
    }
    private func number(_ identity: String) -> UInt64 {
        if let number = numbers[identity] { return number }
        let number = nextID
        nextID += 1
        numbers[identity] = number
        return number
    }
    private func remember(_ metadata: NativeItem) -> YinYangItem {
        if let item = items[metadata.node] {
            // A pinned directory enumeration may outlive a newer observation.
            // Do not publish old attributes with a newer FSKit sequence number.
            if metadata.generation >= item.metadata.generation { item.metadata = metadata }
            return item
        }
        let item = YinYangItem(metadata, number: number(metadata.node))
        items[metadata.node] = item
        return item
    }
    private func attrs(_ item: YinYangItem) -> FSItem.Attributes {
        let m = item.metadata
        let result = FSItem.Attributes()
        result.type = m.directory ? .directory : .file
        result.fileID = FSItem.Identifier(rawValue: item.number)!
        result.parentID = FSItem.Identifier(rawValue: number(m.parent ?? root.identity))!
        result.size = m.size
        result.allocSize = m.size
        result.mode = m.directory || m.executable ? 0o755 : 0o644
        result.uid = getuid()
        result.gid = getgid()
        result.linkCount = m.directory ? 2 : 1
        result.flags = 0
        result.modifyTime = timespec(tv_sec: Int(clamping: m.generation), tv_nsec: 0)
        result.changeTime = result.modifyTime
        result.accessTime = result.modifyTime
        result.birthTime = timespec(tv_sec: 0, tv_nsec: 0)
        return result
    }
    private func perform<T>(_ operation: @escaping () throws -> T, _ reply: @escaping (T?, Error?) -> Void) {
        queue.async {
            do { reply(try operation(), nil) }
            catch {
                self.logger.error("Operation failed: \(String(describing: error), privacy: .public)")
                reply(nil, (error as? NativeFailure)?.posix ?? error)
            }
        }
    }
    private func checked<T>(_ result: T?) throws -> T {
        guard let result else { throw POSIXError(.EIO) }
        return result
    }
    var volumeStatistics: FSStatFSResult {
        let result = FSStatFSResult(fileSystemTypeName: "yinyang")
        result.blockSize = 4096
        result.ioSize = 64 * 1024
        return result
    }
    var supportedVolumeCapabilities: FSVolume.SupportedCapabilities {
        let result = FSVolume.SupportedCapabilities()
        result.caseFormat = .insensitiveCasePreserving
        result.doesNotSupportVolumeSizes = true
        result.doesNotSupportSettingFilePermissions = true
        result.supportsSparseFiles = true
        return result
    }
    var maximumLinkCount: Int { 1 }
    var maximumNameLength: Int { 255 }
    var restrictsOwnershipChanges: Bool { true }
    var truncatesLongNames: Bool { false }
    var maximumFileSizeInBits: Int { 63 }
    var maximumXattrSizeInBits: Int { 0 }
    func activateVolume(options: FSTaskOptions, replyHandler: @escaping (FSActivateResult?, Error?) -> Void) { replyHandler(FSActivateResult(rootItem: root), nil) }
    func deactivateVolume(options: FSDeactivateOptions, replyHandler: @escaping (Error?) -> Void) { replyHandler(nil) }
    func mount(options: FSTaskOptions, replyHandler: @escaping (Error?) -> Void) { replyHandler(nil) }
    func unmount(replyHandler: @escaping () -> Void) { queue.async { replyHandler() } }
    func synchronize(flags: FSSyncFlags, replyHandler: @escaping (Error?) -> Void) {
        perform({ try self.bridge.call(["op": "fsync"]) }) { _, error in replyHandler(error) }
    }
    func getAttributes(_ desiredAttributes: FSItem.GetAttributesRequest, of raw: FSItem, context: FSContext, replyHandler: @escaping (FSGetAttributesResult?, Error?) -> Void) {
        perform({
            let item = try self.item(raw)
            if !item.metadata.directory { try self.refresh(item) }
            do { item.metadata = try self.bridge.item(["op": "node", "node": item.identity]) }
            catch let error as NativeFailure where error.kind == "NotFound" { /* Retained open-unlink metadata. */ }
            return try self.checked(FSGetAttributesResult(attributes: self.attrs(item)))
        }, replyHandler)
    }
    func setAttributes(_ requested: FSItem.SetAttributesRequest, on raw: FSItem, context: FSContext, replyHandler: @escaping (FSSetAttributesResult?, Error?) -> Void) {
        perform({
            let item = try self.item(raw)
            // Validate the complete request before making any content change.
            if requested.isValid(.mode) && requested.mode & 0o7777 != self.attrs(item).mode { throw POSIXError(.ENOTSUP) }
            if requested.isValid(.flags) && requested.flags != 0 { throw POSIXError(.ENOTSUP) }
            if requested.isValid(.uid) && requested.uid != getuid() { throw POSIXError(.ENOTSUP) }
            if requested.isValid(.gid) && requested.gid != getgid() { throw POSIXError(.ENOTSUP) }
            for attribute: FSItem.Attribute in [.accessTime, .modifyTime, .changeTime, .birthTime, .backupTime, .addedTime, .allocSize] {
                if requested.isValid(attribute) { throw POSIXError(.ENOTSUP) }
            }
            if requested.isValid(.size) {
                try self.bridge.call(["op": "truncate", "node": item.identity, "length": requested.size])
                requested.consumedAttributes.insert(.size)
            }
            for attribute: FSItem.Attribute in [.mode, .flags, .uid, .gid] {
                if requested.isValid(attribute) { requested.consumedAttributes.insert(attribute) }
            }
            item.metadata = try self.bridge.item(["op": "node", "node": item.identity])
            return try self.checked(FSSetAttributesResult(attributes: self.attrs(item), freeSpace: nil))
        }, replyHandler)
    }
    func lookupItem(named name: FSFileName, in directory: FSItem, context: FSContext, replyHandler: @escaping (FSLookupItemResult?, Error?) -> Void) {
        perform({
            guard let name = name.string else { throw POSIXError(.EINVAL) }
            let directory = try self.item(directory)
            if name == "." { return directory }
            if name == ".." {
                guard let parent = directory.metadata.parent else { return self.root }
                return self.remember(try self.bridge.item(["op": "node", "node": parent]))
            }
            return self.remember(try self.bridge.item(["op": "lookup", "parent": directory.identity, "name": name]))
        }) { (item: YinYangItem?, error) in
            replyHandler(item.flatMap { FSLookupItemResult(foundItem: $0, itemName: FSFileName(string: $0.metadata.name), itemAttributes: self.attrs($0)) }, error)
        }
    }
    func reclaimItem(_ raw: FSItem, replyHandler: @escaping (Error?) -> Void) {
        perform({
            let item = try self.item(raw)
            var failure: Error?
            _ = item.tryReclaim {
                do { try self.bridge.call(["op": "release", "node": item.identity]) }
                catch { failure = error }
            }
            if let failure { throw failure }
            // Keep the numeric identity stable for the lifetime of the volume.
            return true
        }) { _, error in replyHandler(error) }
    }
    func readSymbolicLink(_ item: FSItem, context: FSContext, replyHandler: @escaping (FSReadSymlinkResult?, Error?) -> Void) { replyHandler(nil, POSIXError(.ENOTSUP)) }
    func createSymbolicLink(named name: FSFileName, in directory: FSItem, attributes: FSItem.SetAttributesRequest, linkContents: FSFileName, context: FSContext, replyHandler: @escaping (FSCreateSymlinkResult?, Error?) -> Void) { replyHandler(nil, POSIXError(.ENOTSUP)) }
    func createLink(to item: FSItem, named name: FSFileName, in directory: FSItem, context: FSContext, replyHandler: @escaping (FSCreateLinkResult?, Error?) -> Void) { replyHandler(nil, POSIXError(.ENOTSUP)) }
    func createItem(named name: FSFileName, type: FSItem.ItemType, in directory: FSItem, attributes: FSItem.SetAttributesRequest, context: FSContext, replyHandler: @escaping (FSCreateItemResult?, Error?) -> Void) {
        perform({
            guard type == .file || type == .directory, let name = name.string else { throw POSIXError(.ENOTSUP) }
            if attributes.isValid(.mode) && type == .file && attributes.mode & 0o111 != 0 { throw POSIXError(.ENOTSUP) }
            if attributes.isValid(.size) && attributes.size != 0 { throw POSIXError(.ENOTSUP) }
            if attributes.isValid(.flags) && attributes.flags != 0 { throw POSIXError(.ENOTSUP) }
            let parent = try self.item(directory)
            let item = self.remember(try self.bridge.item(["op": "create", "parent": parent.identity, "name": name, "directory": type == .directory]))
            parent.metadata = try self.bridge.item(["op": "node", "node": parent.identity])
            return try self.checked(FSCreateItemResult(newItem: item, newItemName: FSFileName(string: item.metadata.name), newItemAttributes: self.attrs(item), directoryAttributes: self.attrs(parent), freeSpace: nil))
        }, replyHandler)
    }
    func removeItem(_ raw: FSItem, named name: FSFileName, from directory: FSItem, context: FSContext, replyHandler: @escaping (FSRemoveItemResult?, Error?) -> Void) {
        perform({
            let item = try self.item(raw), parent = try self.item(directory)
            try self.bridge.call(["op": "remove", "node": item.identity])
            parent.metadata = try self.bridge.item(["op": "node", "node": parent.identity])
            let removed = self.attrs(item)
            removed.linkCount = 0
            return try self.checked(FSRemoveItemResult(itemAttributes: removed, directoryAttributes: self.attrs(parent), freeSpace: nil))
        }, replyHandler)
    }
    func renameItem(_ raw: FSItem, inDirectory sourceDirectory: FSItem, named sourceName: FSFileName, to destinationName: FSFileName, inDirectory destinationDirectory: FSItem, overItem: FSItem?, context: FSContext, replyHandler: @escaping (FSRenameItemResult?, Error?) -> Void) {
        perform({
            guard let name = destinationName.string else { throw POSIXError(.EINVAL) }
            try self.bridge.call(["op": "rename", "node": self.item(raw).identity, "parent": self.item(destinationDirectory).identity, "name": name, "replace": overItem != nil])
            let item = try self.item(raw), source = try self.item(sourceDirectory), destination = try self.item(destinationDirectory)
            for item in [item, source, destination] { item.metadata = try self.bridge.item(["op": "node", "node": item.identity]) }
            let replaced = try overItem.map { self.attrs(try self.item($0)) }
            replaced?.linkCount = 0
            return try self.checked(FSRenameItemResult(newName: FSFileName(string: name), renamedItemAttributes: self.attrs(item), sourceDirectoryAttributes: self.attrs(source), destinationDirectoryAttributes: self.attrs(destination), overItemAttributes: replaced, freeSpace: nil))
        }, replyHandler)
    }
    func enumerateDirectory(_ raw: FSItem, startingAt cookie: FSDirectoryCookie, verifier: FSDirectoryVerifier, attributes: FSItem.GetAttributesRequest?, packer: FSDirectoryEntryPacker, context: FSContext, replyHandler: @escaping (FSEnumerateDirectoryResult?, Error?) -> Void) {
        perform({
            let directory = try self.item(raw)
            var request: [String: Any] = ["op": "scan", "node": directory.identity, "offset": cookie.rawValue, "limit": 256]
            let currentVerifier: UInt64
            if cookie.rawValue == 0 {
                currentVerifier = self.nextVerifier
                self.nextVerifier += 1
                if self.enumerations.count >= 1024, let oldest = self.enumerations.keys.min() {
                    self.enumerations.removeValue(forKey: oldest)
                }
            } else {
                currentVerifier = verifier.rawValue
                guard let (identity, revision) = self.enumerations[verifier.rawValue], identity == directory.identity else { throw POSIXError(.ESTALE) }
                request["revision"] = revision
            }
            var offset = cookie.rawValue
            while true {
                request["offset"] = offset
                let page = try self.bridge.call(request)
                guard let revision = page["revision"] as? String, let entries = page["entries"] as? [[String: Any]] else { throw POSIXError(.EIO) }
                self.enumerations[currentVerifier] = (directory.identity, revision)
                request["revision"] = revision
                for value in entries {
                    let item = self.remember(try NativeItem(value))
                    if !packer.packEntry(name: FSFileName(string: item.metadata.name), itemType: item.metadata.directory ? .directory : .file,
                        itemID: FSItem.Identifier(rawValue: item.number)!, nextCookie: FSDirectoryCookie(offset + 1),
                        attributes: self.attrs(item)) { return currentVerifier }
                    offset += 1
                }
                if page["next"] is NSNull { break }
            }
            return currentVerifier
        }) { value, error in replyHandler(value.flatMap { FSEnumerateDirectoryResult(verifier: $0) }, error) }
    }
    func read(from raw: FSItem, at offset: off_t, length: Int, into buffer: FSMutableFileDataBuffer, replyHandler: @escaping (FSReadFileResult?, Error?) -> Void) {
        perform({
            guard offset >= 0, length >= 0 else { throw POSIXError(.EINVAL) }
            let item = try self.item(raw)
            try self.refresh(item)
            let data = try self.bridge.read(node: item.identity, offset: UInt64(offset), length: length)
            _ = buffer.withUnsafeMutableBytes { target in data.copyBytes(to: target) }
            do { item.metadata = try self.bridge.item(["op": "node", "node": item.identity]) }
            catch let error as NativeFailure where error.kind == "NotFound" { /* Retained unlinked read. */ }
            guard let result = FSReadFileResult(bytesRead: data.count, itemAttributes: self.attrs(item)) else { throw POSIXError(.EIO) }
            return result
        }, replyHandler)
    }
    private func refresh(_ item: YinYangItem) throws {
        do { try bridge.call(["op": "refresh", "node": item.identity]) }
        catch let error as NativeFailure where error.kind == "Busy" || error.kind == "NotFound" { /* Retain dirty or unlinked bytes. */ }
    }
    func write(contents: Data, to raw: FSItem, at offset: off_t, replyHandler: @escaping (FSWriteFileResult?, Error?) -> Void) {
        perform({
            guard offset >= 0 else { throw POSIXError(.EINVAL) }
            let item = try self.item(raw)
            try self.bridge.write(node: item.identity, offset: UInt64(offset), data: contents)
            item.metadata.size = max(item.metadata.size, UInt64(offset) + UInt64(contents.count))
            guard let result = FSWriteFileResult(bytesWritten: contents.count, itemAttributes: self.attrs(item), freeSpace: nil) else { throw POSIXError(.EIO) }
            return result
        }, replyHandler)
    }
    func open(_ raw: FSItem, modes: FSVolume.OpenModes, cacheMode: FSVolume.DataCacheMode, context: FSContext, replyHandler: @escaping (FSOpenItemResult?, Error?) -> Void) {
        perform({
            let item = try self.item(raw)
            self.references[item.identity, default: 0] += 1
            return FSOpenItemResult(grantedCoherency: .noCache)
        }, replyHandler)
    }
    func upgrade(_ item: FSItem, cacheMode: FSVolume.DataCacheMode, context: FSContext, replyHandler: @escaping (FSUpgradeItemResult?, Error?) -> Void) {
        replyHandler(FSUpgradeItemResult(grantedCoherency: .noCache), nil)
    }
    func close(_ raw: FSItem, context: FSContext, replyHandler: @escaping () -> Void) {
        queue.async {
            do {
                let item = try self.item(raw)
                let remaining = max(0, self.references[item.identity, default: 1] - 1)
                self.references[item.identity] = remaining
                try self.bridge.call(["op": "fsync"])
                if remaining == 0 { try self.bridge.call(["op": "release", "node": item.identity]) }
            } catch { self.logger.error("Close retained pending state: \(String(describing: error), privacy: .public)") }
            replyHandler()
        }
    }
}
