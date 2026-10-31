//! The parent columns an inherited column copies, kept per thread by the block they were drawn for,
//! so a run of child blocks draws each parent block once rather than once per child block.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use arrow_array::Array;
use arrow_array::ArrayRef;

/// The bytes of parent columns one thread keeps before it lets the least recently read go.
const HELD_BYTES: usize = 64 << 20;

static NEXT_OWNER: AtomicU64 = AtomicU64::new(0);

/// A number no other draw of parent columns in the process holds, which keys its blocks.
pub(crate) fn owner() -> u64 {
    NEXT_OWNER.fetch_add(1, Ordering::Relaxed)
}

struct Held {
    owner: u64,
    block: u64,
    columns: Arc<[ArrayRef]>,
    bytes: usize,
    read: u64,
}

#[derive(Default)]
struct HeldBlocks {
    held: Vec<Held>,
    bytes: usize,
    clock: u64,
}

thread_local! {
    static HELD: RefCell<HeldBlocks> = RefCell::new(HeldBlocks::default());
}

/// `owner`'s columns for parent block `block`, drawn by `draw` where this thread holds none.
pub(crate) fn parent_columns(owner: u64, block: u64, draw: impl FnOnce() -> Vec<ArrayRef>) -> Arc<[ArrayRef]> {
    let found = HELD.with_borrow_mut(|blocks| {
        blocks.clock += 1;
        let clock = blocks.clock;
        blocks
            .held
            .iter_mut()
            .find(|held| held.owner == owner && held.block == block)
            .map(|held| {
                held.read = clock;
                held.columns.clone()
            })
    });
    if let Some(columns) = found {
        return columns;
    }
    let columns: Arc<[ArrayRef]> = draw().into();
    let bytes = columns.iter().map(|column| column.get_array_memory_size()).sum();
    HELD.with_borrow_mut(|blocks| {
        while blocks.bytes + bytes > HELD_BYTES && !blocks.held.is_empty() {
            let oldest = (0..blocks.held.len())
                .min_by_key(|&at| blocks.held[at].read)
                .expect("held is not empty");
            blocks.bytes -= blocks.held.swap_remove(oldest).bytes;
        }
        blocks.bytes += bytes;
        let read = blocks.clock;
        blocks.held.push(Held {
            owner,
            block,
            columns: columns.clone(),
            bytes,
            read,
        });
    });
    columns
}
