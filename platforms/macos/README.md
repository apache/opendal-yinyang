# macOS native prototypes

Requires macOS 27, Xcode 27, XcodeGen and the repository Rust toolchain. These
extensions operate on isolated test volumes only. See
[the specification](../../specs/macos-frontends.md) for supported semantics.

Build without signing:

```sh
cargo x macos
```

Build with a local Developer ID identity and a profile authorizing the FSKit
module entitlement for `com.xuanwo.yinyang.prototype.fskit`:

```sh
cargo x macos --identity 'Developer ID Application: NAME (TEAM)' \
  --team TEAM --fskit-profile 'FSKit profile name'
```

The result is `target/macos/DerivedData/Build/Products/Debug/YinYang.app`.
Open the host, enable its filesystem extension in System Settings, and check
`pluginkit -m -v -i com.xuanwo.yinyang.prototype.fskit` points to this build.
An older extension with the same identifier must not be used for acceptance.

Create an isolated resource directory with `volume.json` describing an **existing**
Managed filesystem. Use dedicated test credentials and restrictive file
permissions; never commit credentials or provisioning profiles.

```json
{
  "profile": "minio",
  "storage": {
    "endpoint": "http://127.0.0.1:19000",
    "bucket": "isolated-test-bucket",
    "root": "isolated-prefix",
    "region": "us-east-1",
    "access_key_id": "TEST_KEY",
    "secret_access_key": "TEST_SECRET"
  }
}
```

The adapter supplies staging beneath the selected resource. Mount into a separate
empty directory using absolute paths:

```sh
mount -F -t yinyang /absolute/test-resource /absolute/test-mount
```

Acceptance must check `mount` reports `yinyang` before and after I/O, and compare
results with an independent `yy restore` from the same remote filesystem. An
extension crash can unmount the volume and expose the underlying local directory;
successful local writes alone do not establish native or remote correctness.

Exercise multiple descriptors, close-one/write-another, fsync, truncate, safe-save
replacement, directory pagination, independent remote updates, backend outage and
extension restart with the original staging directory. Do not remove pending
staging or use an existing user sync directory. Unmount before rebuilding; ensure
the next launch runs the newly built extension rather than the previous process.

## File Provider

Build both extensions without signing in a separate output directory:

```sh
cargo x macos --file-provider --output target/macos-provider
```

The host and `.fileprovider` App IDs need profiles authorizing App Group
`group.com.xuanwo.yinyang.prototype`. The FSKit profile remains separate:

```sh
cargo x macos --file-provider --output target/macos-provider \
  --identity 'Developer ID Application: NAME (TEAM)' --team TEAM \
  --fskit-profile 'FSKit profile name' \
  --host-profile 'Host profile name' --provider-profile 'Provider profile name'
```

Open the signed host and choose **Add Isolated Provider Domain**. Only existing
Managed filesystem configurations should be selected; the extension reports
connection/validation errors when opening the authority. The host copies test credentials
into its private App Group; use a dedicated backend prefix. Keep `mount-state`
and `sync-state` separate. Browse the new domain in Finder, hydrate a file and
modify existing file content. Namespace mutations are deliberately unsupported.

The **Refresh** action signals registered test domains. **Remove Test Domains**
requires confirmation, unregisters only UUIDs this host recorded, and retains
staging for recovery. Export pending edits before removing a domain. The host
does not delete retained App Group files automatically.

Run the Swift callback contract tests against a dedicated configuration:

```sh
cargo x macos --file-provider --output target/macos-provider \
  --test-config /absolute/isolated-native-config.json
```

This command creates and retains a uniquely named remote test subtree and local
staging. It checks pinned pagination, recursive working-set enumeration, bounded
change anchors, upload completion, identical retry and conflict preservation
through the actual Swift callbacks and Rust runtime. It does **not** register an
OS domain and must not be reported as Finder or File Provider daemon acceptance.
