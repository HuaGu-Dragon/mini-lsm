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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        Bytes::from_iter(
            self.data
                .iter()
                .copied()
                .chain(self.offsets.iter().flat_map(|b| b.to_be_bytes()))
                .chain((self.offsets.len() as u16).to_be_bytes().iter().copied()),
        )
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        const SIZEOF_U16: usize = std::mem::size_of::<u16>();
        let n = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - n * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];

        let offsets = offsets_raw
            .as_chunks()
            .0
            .iter()
            .map(|b| u16::from_be_bytes(*b))
            .collect();

        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
