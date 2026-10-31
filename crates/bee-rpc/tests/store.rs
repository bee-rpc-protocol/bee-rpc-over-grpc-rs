//! The block store: what the reader sends for a stored object must be, Buffer for Buffer,
//! what the Python reference sends for the same directories (`serialize_to_buffer` of a
//! `Dir`), and the reads that the reference refuses must fail here too.
//!
//! The directories and the streams come from `tools/vectors/generate.py`: `store` has
//! each stored object with the blocks that it points to, and `store_errors` the same
//! trees after one change, with the exception that the reference raised.

mod common;

use std::path::Path;

use bee_rpc::buffer::Buffer;
use bee_rpc::store::Store;
use bee_rpc::Error;
use common::{cases, manifest, stream, vectors_dir};

/// A copy of `from` at `to`, for the tests that change a tree.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn read_all(store: &Store, index: i32, obj: &Path) -> Result<Vec<Buffer>, Error> {
    store.read_message(index, obj)?.collect()
}

fn store_case(name: &str) -> serde_json::Value {
    cases(&manifest(), "store")
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("the vector {name}"))
        .clone()
}

fn depth_of(case: &serde_json::Value) -> usize {
    case["block_depth"].as_u64().unwrap() as usize
}

#[test]
fn the_buffers_of_a_stored_object_are_those_of_the_reference() {
    let m = manifest();
    let list = cases(&m, "store");
    assert!(!list.is_empty());
    for case in list {
        let name = case["name"].as_str().unwrap();
        let store = Store::new(vectors_dir().join(case["blocks"].as_str().unwrap()))
            .with_block_depth(depth_of(case));
        let index = case["head_index"].as_i64().unwrap() as i32;
        let obj = vectors_dir().join(case["obj"].as_str().unwrap());
        let got = read_all(&store, index, &obj).unwrap_or_else(|e| panic!("{name}: {e}"));
        let expected = stream(case["stream"].as_str().unwrap());
        assert_eq!(got.len(), expected.len(), "{name}: number of Buffers");
        for (i, (g, e)) in got.iter().zip(&expected).enumerate() {
            assert_eq!(g, e, "{name}: Buffer {i}");
        }
    }
}

#[test]
fn a_depth_that_is_not_the_one_of_the_vector_gives_other_markers() {
    // The same object framed one level deeper has the markers of the leaf too: a reader
    // that ignores the depth cannot give both vectors.
    let case = store_case("nested_depth2");
    let blocks = vectors_dir().join(case["blocks"].as_str().unwrap());
    let obj = vectors_dir().join(case["obj"].as_str().unwrap());
    let markers = |depth: usize| {
        read_all(&Store::new(&blocks).with_block_depth(depth), 1, &obj)
            .unwrap()
            .iter()
            .filter(|b| b.block.is_some())
            .count()
    };
    assert!(markers(2) > markers(1));
    assert_eq!(markers(2), markers(5));
}

#[test]
fn the_reads_that_the_reference_refuses_fail() {
    let m = manifest();
    let list = cases(&m, "store_errors");
    assert!(!list.is_empty());
    for case in list {
        let name = case["name"].as_str().unwrap();
        assert!(case["python_raises"].is_string(), "{name}");
        let base = store_case(case["base"].as_str().unwrap());
        let copy = std::env::temp_dir().join(format!("bee-store-{}-{name}", std::process::id()));
        copy_tree(
            &vectors_dir().join(base["obj"].as_str().unwrap()),
            &copy.join("obj"),
        );
        copy_tree(
            &vectors_dir().join(base["blocks"].as_str().unwrap()),
            &copy.join("blocks"),
        );
        let mutation = &case["mutation"];
        let target = copy.join(mutation["path"].as_str().unwrap());
        match mutation["op"].as_str().unwrap() {
            "flip_first_byte" => {
                let mut data = std::fs::read(&target).unwrap();
                data[0] ^= 0xFF;
                std::fs::write(&target, data).unwrap();
            }
            "delete" => std::fs::remove_file(&target).unwrap(),
            "write" => std::fs::write(&target, mutation["text"].as_str().unwrap()).unwrap(),
            other => panic!("unknown mutation {other}"),
        }
        let store = Store::new(copy.join("blocks")).with_block_depth(depth_of(&base));
        let error = read_all(&store, 1, &copy.join("obj"))
            .expect_err("the reference raises, so this fails");
        match name {
            "block_with_other_bytes" => {
                assert!(matches!(error, Error::BlockIdMismatch { .. }), "{error}")
            }
            "block_missing" | "entry_without_lengths" => {
                assert!(matches!(error, Error::Store(_)), "{name}: {error}")
            }
            other => panic!("a case without a check: {other}"),
        }
        std::fs::remove_dir_all(&copy).unwrap();
    }
}

#[test]
fn a_plain_file_is_read_in_chunks_with_no_head_marker_or_check() {
    let dir = std::env::temp_dir().join(format!("bee-store-file-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("data");
    let data: Vec<u8> = (0..bee_rpc::CHUNK_SIZE + 10)
        .map(|i| (i % 251) as u8)
        .collect();
    std::fs::write(&file, &data).unwrap();
    let got = Store::new(&dir)
        .read_registry(&file)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].chunk.as_ref().unwrap().len(), bee_rpc::CHUNK_SIZE);
    let bytes: Vec<u8> = got.iter().flat_map(|b| b.chunk.clone().unwrap()).collect();
    assert_eq!(bytes, data);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_store_that_points_to_itself_ends_with_an_error() {
    let dir = std::env::temp_dir().join(format!("bee-store-loop-{}", std::process::id()));
    let blocks = dir.join("blocks");
    // The block `ab` is a directory whose only entry is the block `ab`.
    let looped = blocks.join("ab");
    std::fs::create_dir_all(&looped).unwrap();
    std::fs::write(looped.join("_.json"), r#"[["ab", [1]]]"#).unwrap();
    let obj = dir.join("obj");
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::write(obj.join("_.json"), r#"[["ab", [1]]]"#).unwrap();
    let result = Store::new(&blocks)
        .with_block_depth(1000)
        .read_registry(&obj)
        .unwrap()
        .collect::<Result<Vec<_>, _>>();
    assert!(
        matches!(result, Err(Error::NestingTooDeep(_))),
        "{result:?}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
