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

#if YINYANG_FILE_PROVIDER
import AppKit
import FileProvider
import SwiftUI

@MainActor
final class ProviderDomains: ObservableObject {
    @Published var status = "No domain selected."
    @Published var busy = false
    private let key = "YinYangPrototypeDomainIDs"
    private var identifiers: [String] {
        get { UserDefaults.standard.stringArray(forKey: key) ?? [] }
        set { UserDefaults.standard.set(newValue, forKey: key) }
    }
    func add() {
        let picker = NSOpenPanel()
        picker.canChooseDirectories = true
        picker.canChooseFiles = false
        picker.message = "Choose an isolated test resource containing volume.json. Its credentials will be copied into this prototype's App Group."
        guard picker.runModal() == .OK, let source = picker.url else { return }
        let scoped = source.startAccessingSecurityScopedResource()
        defer { if scoped { source.stopAccessingSecurityScopedResource() } }
        do {
            guard let group = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: "group.com.xuanwo.yinyang.prototype") else { throw POSIXError(.EACCES) }
            let data = try Data(contentsOf: source.appendingPathComponent("volume.json"))
            guard var config = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                  config["storage"] is [String: String], let profile = config["profile"] as? String,
                  ["minio", "amazon-s3"].contains(profile), config["read_only"] as? Bool != true else { throw POSIXError(.EINVAL) }
            config.removeValue(forKey: "staging")
            let id = UUID().uuidString
            let directory = group.appendingPathComponent(id)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
            let output = try JSONSerialization.data(withJSONObject: config, options: [.sortedKeys])
            guard FileManager.default.createFile(atPath: directory.appendingPathComponent("volume.json").path, contents: output, attributes: [.posixPermissions: 0o600]) else { throw POSIXError(.EIO) }
            // Persist the generated ID before the OS call, including an ambiguous
            // completion, so registration never creates an unmanageable domain.
            identifiers.append(id)
            let domain = NSFileProviderDomain(identifier: NSFileProviderDomainIdentifier(id), displayName: "YinYang Prototype \(id.prefix(8))")
            busy = true
            NSFileProviderManager.add(domain) { error in
                Task { @MainActor in
                    self.busy = false
                    self.status = error.map { "Domain registration failed: \($0.localizedDescription). Configuration retained." } ?? "Registered isolated domain \(id)."
                }
            }
        } catch { status = error.localizedDescription }
    }
    func refresh() {
        let owned = Set(identifiers)
        busy = true
        NSFileProviderManager.getDomainsWithCompletionHandler { domains, error in
            for domain in domains where owned.contains(domain.identifier.rawValue) {
                guard let manager = NSFileProviderManager(for: domain) else { continue }
                manager.signalEnumerator(for: .workingSet) { _ in }
                manager.signalEnumerator(for: .rootContainer) { _ in }
            }
            Task { @MainActor in self.busy = false; self.status = error?.localizedDescription ?? "Refresh requested for this prototype's domains." }
        }
    }
    func remove() {
        let alert = NSAlert()
        alert.messageText = "Remove this prototype's test domains?"
        alert.informativeText = "Only UUIDs registered by this app are affected. The OS may remove downloaded test copies. App Group staging is retained for recovery; export pending edits before removal."
        alert.addButton(withTitle: "Remove Test Domains")
        alert.addButton(withTitle: "Cancel")
        guard alert.runModal() == .alertFirstButtonReturn else { return }
        let owned = Set(identifiers)
        busy = true
        NSFileProviderManager.getDomainsWithCompletionHandler { domains, error in
            if let error {
                Task { @MainActor in self.busy = false; self.status = error.localizedDescription }
                return
            }
            let group = DispatchGroup()
            let lock = NSLock()
            var failures: [String] = []
            for domain in domains where owned.contains(domain.identifier.rawValue) {
                group.enter()
                NSFileProviderManager.remove(domain) { error in
                    if let error { lock.lock(); failures.append(error.localizedDescription); lock.unlock() }
                    group.leave()
                }
            }
            group.notify(queue: .main) {
                Task { @MainActor in
                    self.busy = false
                    self.status = failures.isEmpty ? "Test domains removed; staging retained." : failures.joined(separator: "\n")
                }
            }
        }
    }
}

struct ProviderControls: View {
    @StateObject private var domains = ProviderDomains()
    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Button("Add Isolated Provider Domain") { domains.add() }
                Button("Refresh") { domains.refresh() }
                Button("Remove Test Domains") { domains.remove() }
            }.disabled(domains.busy)
            Text(domains.status).font(.caption).textSelection(.enabled)
        }
    }
}
#endif
