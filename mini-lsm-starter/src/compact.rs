// Copyright (c) 2022-2026 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::Key;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    pub(crate) fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = Key<&'a [u8]>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut entries_in_builder = 0;
        let mut ssts = Vec::new();

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            if !compact_to_bottom_level || !iter.value().is_empty() {
                builder_inner.add(iter.key(), iter.value());
                entries_in_builder += 1;
            }
            iter.next()?;

            if entries_in_builder > 0
                && builder_inner.estimated_size() >= self.options.target_sst_size
            {
                let sst_id = self.next_sst_id();
                let builder = builder.take().unwrap();
                ssts.push(Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?));
                entries_in_builder = 0;
            }
        }

        if let Some(builder) = builder
            && entries_in_builder > 0
        {
            let sst_id = self.next_sst_id();
            ssts.push(Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?));
        }

        Ok(ssts)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = self.state.read().clone();
        match task {
            CompactionTask::Leveled(leveled_compaction_task) => todo!(),
            CompactionTask::Tiered(tiered_compaction_task) => todo!(),
            CompactionTask::Simple(simple_leveled_compaction_task) => todo!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_iters = l0_sstables
                    .iter()
                    .map(|id| {
                        Ok(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables[id].clone(),
                        )?))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let l1_ssts = l1_sstables
                    .iter()
                    .map(|id| snapshot.sstables[id].clone())
                    .collect::<Vec<_>>();

                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_ssts)?,
                )?;

                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sst, l1_sst) = {
            let snapshot = self.state.read();
            (snapshot.l0_sstables.clone(), snapshot.levels[0].1.clone())
        };

        let ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sst.clone(),
            l1_sstables: l1_sst.clone(),
        })?;

        {
            let _guard = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();

            l0_sst
                .iter()
                .chain(l1_sst.iter())
                .for_each(|id| assert!(snapshot.sstables.remove(id).is_some()));

            let ids = ssts
                .into_iter()
                .map(|s| {
                    let id = s.sst_id();
                    assert!(snapshot.sstables.insert(id, s).is_none());

                    id
                })
                .collect::<Vec<_>>();

            assert_eq!(snapshot.levels[0].1, l1_sst);
            snapshot.levels[0].1 = ids;

            let mut captured = l0_sst.iter().copied().collect::<HashSet<_>>();
            snapshot.l0_sstables.retain(|id| !captured.remove(id));
            assert!(captured.is_empty());

            *self.state.write() = Arc::new(snapshot);
        }

        l0_sst
            .iter()
            .chain(l1_sst.iter())
            .try_for_each(|&id| std::fs::remove_file(self.path_of_sst(id)))?;

        self.sync_dir()?;

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
