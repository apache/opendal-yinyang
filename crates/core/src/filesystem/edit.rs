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

use super::{directory_membership, nodes_by_id, *};
use crate::ErrorKind;

/// A batch of namespace edits based on one immutable observation.
///
/// Operations preserve identities and reject invalid namespace changes without
/// modifying the batch. Generations are assigned once, from the final state,
/// when `finish` builds the successor tree. Publish it with the same observation.
pub struct TreeEdit<'a> {
    base: &'a Tree,
    tree: Tree,
    names: BTreeMap<(NodeId, String), NodeId>,
}

impl<'a> TreeEdit<'a> {
    pub(crate) fn new(base: &'a Tree) -> Self {
        let names = base
            .entries
            .iter()
            .filter_map(|(path, node)| {
                let parent = path.parent()?;
                Some(((base.entries[&parent].id, folded(path)), node.id))
            })
            .collect();
        Self {
            base,
            tree: base.clone(),
            names,
        }
    }

    /// Inspect staged nodes. Their generations are finalized by `finish`.
    pub fn tree(&self) -> &Tree {
        &self.tree
    }

    pub fn create_dir(&mut self, path: Path, executable: bool) -> Result<NodeId> {
        let node = Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            executable,
            Generation::FIRST,
        );
        self.create(path, node)
    }

    pub fn create_file(&mut self, path: Path, file: File, executable: bool) -> Result<NodeId> {
        let node = Node::file(NodeId::generate(), Generation::FIRST, executable, file);
        self.create(path, node)
    }

    fn create(&mut self, path: Path, node: Node) -> Result<NodeId> {
        let key = self.available_name(&path, None)?;
        let id = node.id;
        self.names.insert(key, id);
        self.tree.entries.insert(path, node);
        Ok(id)
    }

    /// Replace a file's content while preserving its identity and attributes.
    pub fn replace_file(&mut self, path: &Path, file: File) -> Result<()> {
        let node = self.tree.entries.get_mut(path).ok_or_else(missing)?;
        if !matches!(node.body, NodeBody::File(_)) {
            return Err(Error::invalid("edit YinYang tree", "entry is not a file"));
        }
        node.body = NodeBody::File(file);
        Ok(())
    }

    pub fn set_executable(&mut self, path: &Path, executable: bool) -> Result<()> {
        self.tree
            .entries
            .get_mut(path)
            .ok_or_else(missing)?
            .executable = executable;
        Ok(())
    }

    /// Move a file or an entire directory subtree. Never overwrites a destination.
    /// A case-only rename is allowed; moving the root or into oneself is not.
    pub fn rename(&mut self, source: &Path, destination: Path) -> Result<()> {
        let node = self.tree.entries.get(source).ok_or_else(missing)?;
        if source.is_root() || destination.is_root() {
            return Err(Error::invalid(
                "rename YinYang entry",
                "cannot rename the root",
            ));
        }
        if source == &destination {
            return Ok(());
        }
        let prefix = format!("{source}/");
        if destination.as_str().starts_with(&prefix) {
            return Err(Error::invalid(
                "rename YinYang entry",
                "cannot move a directory into itself",
            ));
        }
        let id = node.id;
        let destination_key = self.available_name(&destination, Some(id))?;
        let source_key = self.name_key(source)?;
        let descendants = self.descendants(source);
        let mut moved = Vec::with_capacity(descendants.len() + 1);
        moved.push((source.clone(), destination));
        for path in descendants {
            let suffix = &path.as_str()[source.as_str().len()..];
            moved.push((path.clone(), Path::new(format!("{}{suffix}", moved[0].1))?));
        }
        // All fallible path and collision checks happen before any mutation.
        let nodes = moved
            .iter()
            .map(|(old, _)| {
                self.tree
                    .entries
                    .remove(old)
                    .expect("selected entry exists")
            })
            .collect::<Vec<_>>();
        for ((_, new), node) in moved.into_iter().zip(nodes) {
            self.tree.entries.insert(new, node);
        }
        self.names.remove(&source_key);
        self.names.insert(destination_key, id);
        Ok(())
    }

    /// Remove a file or empty directory. Non-empty directories require `remove_tree`.
    pub fn remove(&mut self, path: &Path) -> Result<()> {
        self.tree.entries.get(path).ok_or_else(missing)?;
        if !self.descendants(path).is_empty() {
            return Err(Error::invalid(
                "remove YinYang entry",
                "directory is not empty",
            ));
        }
        self.remove_tree(path)
    }

    /// Explicitly remove an entire subtree, but never the filesystem root.
    pub fn remove_tree(&mut self, path: &Path) -> Result<()> {
        self.tree.entries.get(path).ok_or_else(missing)?;
        if path.is_root() {
            return Err(Error::invalid(
                "remove YinYang entry",
                "cannot remove the root",
            ));
        }
        let mut paths = self.descendants(path);
        paths.push(path.clone());
        let keys = paths
            .iter()
            .map(|path| self.name_key(path))
            .collect::<Result<Vec<_>>>()?;
        for (path, key) in paths.into_iter().zip(keys) {
            self.tree.entries.remove(&path);
            self.names.remove(&key);
        }
        Ok(())
    }

    /// Finalize generations relative to the observation, not individual operations.
    /// Even a no-op batch can be published as an idempotently identified commit.
    pub fn finish(mut self) -> Result<Tree> {
        let previous = nodes_by_id(self.base);
        let before = directory_membership(self.base)?;
        let after = directory_membership(&self.tree)?;
        for node in self.tree.entries.values_mut() {
            let Some((_, old)) = previous.get(&node.id) else {
                continue;
            };
            let changed = node.executable != old.executable || node.file_body() != old.file_body();
            node.generation = if changed {
                old.generation.next()?
            } else {
                old.generation
            };
            if let NodeBody::Dir { entries_generation } = &mut node.body {
                let old_generation = old.dir_generation().expect("stable node kind");
                *entries_generation = if before.get(&node.id) != after.get(&node.id) {
                    old_generation.next()?
                } else {
                    old_generation
                };
            }
        }
        let root = self.base.root_id()?;
        self.tree.validate(root)?;
        self.base.validate_successor(&self.tree, root)?;
        Ok(self.tree)
    }

    fn descendants(&self, path: &Path) -> Vec<Path> {
        let prefix = format!("{path}/");
        // The slash prefix sorts before every child but is not itself a valid path.
        self.tree
            .entries
            .range(Path(prefix.clone())..)
            .take_while(|(candidate, _)| candidate.as_str().starts_with(&prefix))
            .map(|(path, _)| path.clone())
            .collect()
    }

    fn name_key(&self, path: &Path) -> Result<(NodeId, String)> {
        let parent = path
            .parent()
            .ok_or_else(|| Error::invalid("edit YinYang tree", "cannot replace the root"))?;
        let parent = self.tree.entries.get(&parent).ok_or_else(missing)?;
        if parent.dir_generation().is_none() {
            return Err(Error::invalid(
                "edit YinYang tree",
                "parent is not a directory",
            ));
        }
        Ok((parent.id, folded(path)))
    }

    fn available_name(&self, path: &Path, ignored: Option<NodeId>) -> Result<(NodeId, String)> {
        let key = self.name_key(path)?;
        if self.names.get(&key).is_some_and(|id| Some(*id) != ignored) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "edit YinYang tree",
                "destination or case-folded name already exists",
            ));
        }
        Ok(key)
    }
}

fn folded(path: &Path) -> String {
    path.name()
        .expect("a namespace entry is not the root")
        .case_fold()
        .nfc()
        .collect()
}

fn missing() -> Error {
    Error::new(
        ErrorKind::NotFound,
        "edit YinYang tree",
        "entry or parent is missing",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> Path {
        Path::new(name).unwrap()
    }
    fn empty() -> File {
        File::new(ContentId::new(blake3::hash(&[]).into(), 0), vec![]).unwrap()
    }

    #[test]
    fn batch_generations_follow_the_final_state() {
        let base = Tree::genesis(NodeId::generate());
        let mut edit = TreeEdit::new(&base);
        let dir = edit.create_dir(path("dir"), false).unwrap();
        edit.create_file(path("dir/a"), empty(), false).unwrap();
        edit.create_file(path("dir/b"), empty(), true).unwrap();
        let first = edit.finish().unwrap();
        assert_eq!(
            first
                .get(&Path::root())
                .unwrap()
                .dir_generation()
                .unwrap()
                .value(),
            2
        );
        assert_eq!(
            first.get(&path("dir")).unwrap().dir_generation(),
            Some(Generation::FIRST)
        );
        let mut edit = TreeEdit::new(&first);
        edit.create_file(path("dir/c"), empty(), false).unwrap();
        edit.remove(&path("dir/a")).unwrap();
        edit.set_executable(&path("dir/b"), false).unwrap();
        edit.set_executable(&path("dir/b"), true).unwrap();
        let second = edit.finish().unwrap();
        assert_eq!(second.get(&path("dir")).unwrap().id(), dir);
        assert_eq!(
            second
                .get(&path("dir"))
                .unwrap()
                .dir_generation()
                .unwrap()
                .value(),
            2
        );
        assert_eq!(second.get(&Path::root()), first.get(&Path::root()));
        assert_eq!(second.get(&path("dir/b")), first.get(&path("dir/b")));
    }

    #[test]
    fn subtree_rename_preserves_all_identities_and_internal_generations() {
        let base = Tree::genesis(NodeId::generate());
        let mut edit = TreeEdit::new(&base);
        edit.create_dir(path("a"), false).unwrap();
        edit.create_dir(path("b"), false).unwrap();
        edit.create_dir(path("a/inner"), false).unwrap();
        edit.create_file(path("a/inner/file"), empty(), false)
            .unwrap();
        let base = edit.finish().unwrap();
        let mut edit = TreeEdit::new(&base);
        edit.rename(&path("a/inner"), path("b/moved")).unwrap();
        edit.rename(&path("b/moved"), path("b/Moved")).unwrap();
        let next = edit.finish().unwrap();
        assert_eq!(next.get(&path("b/Moved")), base.get(&path("a/inner")));
        assert_eq!(
            next.get(&path("b/Moved/file")),
            base.get(&path("a/inner/file"))
        );
        for parent in ["a", "b"] {
            assert_eq!(
                next.get(&path(parent))
                    .unwrap()
                    .dir_generation()
                    .unwrap()
                    .value(),
                2
            );
        }
    }

    #[test]
    fn invalid_operations_leave_the_batch_unchanged() {
        let base = Tree::genesis(NodeId::generate());
        let mut edit = TreeEdit::new(&base);
        edit.create_dir(path("dir"), false).unwrap();
        edit.create_file(path("dir/File"), empty(), false).unwrap();
        let before = edit.tree().clone();
        assert_eq!(
            edit.create_file(path("dir/file"), empty(), false)
                .unwrap_err()
                .kind(),
            ErrorKind::AlreadyExists
        );
        assert_eq!(
            edit.create_dir(path("missing/sub"), false)
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
        assert!(edit.create_dir(path("dir/File/sub"), false).is_err());
        assert!(edit.rename(&path("dir"), path("dir/sub")).is_err());
        assert!(edit.rename(&Path::root(), path("new")).is_err());
        assert!(edit.rename(&path("dir/File"), path("dir")).is_err());
        assert!(edit.remove(&path("dir")).is_err());
        assert!(edit.remove_tree(&Path::root()).is_err());
        assert!(edit.replace_file(&path("dir"), empty()).is_err());
        assert_eq!(edit.tree(), &before);
        edit.finish().unwrap();
    }

    #[test]
    fn rename_checks_descendant_path_limits_before_mutating() {
        let base = Tree::genesis(NodeId::generate());
        let mut edit = TreeEdit::new(&base);
        let mut parent = String::new();
        for _ in 0..16 {
            if !parent.is_empty() {
                parent.push('/');
            }
            parent.push_str(&"a".repeat(250));
            edit.create_dir(path(&parent), false).unwrap();
        }
        edit.create_dir(path("source"), false).unwrap();
        edit.create_file(path(&format!("source/{}", "b".repeat(100))), empty(), false)
            .unwrap();
        let before = edit.tree().clone();
        assert!(
            edit.rename(&path("source"), path(&format!("{parent}/moved")))
                .is_err()
        );
        assert_eq!(edit.tree(), &before);
    }

    #[test]
    fn removal_releases_names_and_recreation_gets_a_new_identity() {
        let base = Tree::genesis(NodeId::generate());
        let mut edit = TreeEdit::new(&base);
        let old = edit.create_dir(path("dir"), false).unwrap();
        edit.create_file(path("dir/file"), empty(), false).unwrap();
        let base = edit.finish().unwrap();
        let mut edit = TreeEdit::new(&base);
        edit.remove_tree(&path("dir")).unwrap();
        let new = edit.create_file(path("DIR"), empty(), false).unwrap();
        assert_ne!(old, new);
        let next = edit.finish().unwrap();
        assert_eq!(
            next.get(&path("DIR")).unwrap().generation(),
            Generation::FIRST
        );
        assert_eq!(next.iter().count(), 2);
    }

    #[test]
    fn generation_overflow_is_reported_at_finish() {
        let root = NodeId::generate();
        let mut base = Tree::genesis(root);
        base.insert(
            Path::root(),
            Node::dir(
                root,
                Generation::FIRST,
                false,
                Generation::from_value(u64::MAX),
            ),
        );
        let mut edit = TreeEdit::new(&base);
        edit.create_dir(path("dir"), false).unwrap();
        assert_eq!(edit.finish().unwrap_err().kind(), ErrorKind::Corrupt);
    }
}
