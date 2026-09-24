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
import UniformTypeIdentifiers

final class ProviderItem: NSObject, NSFileProviderItem {
    let metadata: NativeItem
    let root: String
    init(_ metadata: NativeItem, root: String) { self.metadata = metadata; self.root = root }
    var itemIdentifier: NSFileProviderItemIdentifier { metadata.node == root ? .rootContainer : NSFileProviderItemIdentifier(metadata.node) }
    var parentItemIdentifier: NSFileProviderItemIdentifier {
        guard let parent = metadata.parent, parent != root else { return .rootContainer }
        return NSFileProviderItemIdentifier(parent)
    }
    var filename: String { metadata.name }
    var contentType: UTType { metadata.directory ? .folder : .data }
    var documentSize: NSNumber? { metadata.directory ? nil : NSNumber(value: metadata.size) }
    var capabilities: NSFileProviderItemCapabilities {
        metadata.directory ? [.allowsReading, .allowsContentEnumerating] : [.allowsReading, .allowsWriting]
    }
    var itemVersion: NSFileProviderItemVersion {
        NSFileProviderItemVersion(contentVersion: Data(metadata.revision.utf8), metadataVersion: Data(metadata.revision.utf8))
    }
}

