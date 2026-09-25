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


import SwiftUI
import FSKit

@main
struct YinYangPrototype: App {
    var body: some Scene {
        WindowGroup {
            VStack(alignment: .leading, spacing: 16) {
                Text("YinYang native prototype").font(.title)
                Text("Use an isolated directory containing volume.json. Enable the file system extension, then mount it with mount -F -t yinyang SOURCE TARGET.")
                    .textSelection(.enabled)
                Button("Open File System Extensions") { _ = FSClient.shared.openFileSystemExtensionsSettings() }
                #if YINYANG_FILE_PROVIDER
                ProviderControls()
                #endif
                Text("Experimental: no production support or existing sync folders.").font(.caption)
            }.padding(24).frame(width: 540)
        }
    }
}
