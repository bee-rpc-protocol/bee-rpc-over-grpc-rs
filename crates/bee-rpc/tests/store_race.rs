//! A block file that changes while the reader works must not go out under a valid block
//! marker: the file that is checked is the file that is sent, and what is sent is hashed
//! again.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bee_rpc::buffer::Buffer;
use bee_rpc::hash::HashAlgorithm;
use bee_rpc::store::Store;
use bee_rpc::Error;

const ALG: HashAlgorithm = HashAlgorithm::Sha3_256;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A root with a block directory, an object that is one block, and the id of the block.
fn tree(name: &str, content: &[u8]) -> (PathBuf, PathBuf, PathBuf, String) {
    let root = std::env::temp_dir().join(format!("bee-rpc-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let blocks = root.join("blocks");
    let object = root.join("object");
    std::fs::create_dir_all(&blocks).unwrap();
    std::fs::create_dir_all(&object).unwrap();
    let id = hex(&ALG.digest(content));
    std::fs::write(object.join("_.json"), format!("[[\"{id}\", []]]")).unwrap();
    std::fs::write(blocks.join(&id), content).unwrap();
    (root, blocks, object, id)
}

/// A thread that replaces the block file, by rename, with good and bad content in turn.
#[test]
fn a_block_file_that_is_swapped_by_rename_never_goes_out_with_other_bytes() {
    let good = vec![b'G'; 64];
    let bad = vec![b'B'; 64];
    let (root, blocks, object, id) = tree("swap", &good);
    let target = blocks.join(&id);
    std::fs::write(root.join("good"), &good).unwrap();
    std::fs::write(root.join("bad"), &bad).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let swapper = {
        let (stop, root, target) = (stop.clone(), root.clone(), target.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for (from, tmp) in [("good", "g.tmp"), ("bad", "b.tmp")] {
                    std::fs::copy(root.join(from), root.join(tmp)).unwrap();
                    std::fs::rename(root.join(tmp), &target).unwrap();
                }
            }
        })
    };

    let store = Store::new(&blocks);
    let start = Instant::now();
    let mut leaked = 0;
    while start.elapsed() < Duration::from_secs(3) {
        let read: Result<Vec<Buffer>, Error> =
            store.read_registry(&object).and_then(|r| r.collect());
        if let Ok(buffers) = read {
            if buffers.iter().any(|b| b.chunk.as_deref() == Some(&bad[..])) {
                leaked += 1;
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    swapper.join().unwrap();
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(
        leaked, 0,
        "bytes that do not hash to the id went out with no error"
    );
}

/// A block of more than one chunk arrives whole, with its two markers.
#[test]
fn a_block_of_many_chunks_still_arrives_whole() {
    let content: Vec<u8> = (0..(bee_rpc::CHUNK_SIZE * 2 + 5))
        .map(|i| (i % 251) as u8)
        .collect();
    let (root, blocks, object, _) = tree("big", &content);
    let buffers: Vec<Buffer> = Store::new(&blocks)
        .read_registry(&object)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let sent: Vec<u8> = buffers
        .iter()
        .filter_map(|b| b.chunk.clone())
        .flatten()
        .collect();
    assert_eq!(sent, content);
    assert_eq!(buffers.iter().filter(|b| b.block.is_some()).count(), 2);
    let _ = std::fs::remove_dir_all(&root);
}

/// A block of exactly one chunk: the end of the file is found by a read that gives 0.
#[test]
fn a_block_of_exactly_one_chunk_arrives_whole() {
    let content = vec![7u8; bee_rpc::CHUNK_SIZE];
    let (root, blocks, object, _) = tree("exact", &content);
    let buffers: Vec<Buffer> = Store::new(&blocks)
        .read_registry(&object)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let sent: usize = buffers
        .iter()
        .filter_map(|b| b.chunk.as_ref())
        .map(Vec::len)
        .sum();
    assert_eq!(sent, content.len());
    let _ = std::fs::remove_dir_all(&root);
}
