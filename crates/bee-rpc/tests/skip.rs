//! Step 5: reverse-direction block skip.
mod common;

use bee_rpc::buffer::Buffer;
use bee_rpc::hash::HashAlgorithm;
use bee_rpc::skip::{skip_buffer, skipped_block_id, strip_skip, SkipFilter, StreamControl};
use bee_rpc::Message;

const ALG: HashAlgorithm = HashAlgorithm::Sha3_256;

#[test]
fn skip_buffer_matches_the_reference() {
    let m = common::manifest();
    let v = &m["skip_buffer"];
    let id = common::unhex(&v["block_id"]);
    let b = skip_buffer(&id, ALG);
    assert_eq!(
        hex::encode(b.encode_to_vec()),
        v["buffer"].as_str().unwrap()
    );
    assert_eq!(
        skipped_block_id(&b, ALG).map(hex::encode).as_deref(),
        v["skipped_block_id"].as_str()
    );
    assert_eq!(skipped_block_id(&b, HashAlgorithm::Sha256), None);
    assert_eq!(skipped_block_id(&Buffer::default(), ALG), None);
}

#[test]
fn filter_matches_the_reference() {
    let m = common::manifest();
    for case in common::cases(&m, "skip") {
        let name = case["name"].as_str().unwrap();
        let input = common::stream(case["input"].as_str().unwrap());
        let ids: Vec<Vec<u8>> = case["skip_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(common::unhex)
            .collect();
        let after = case["request_after"].as_u64().unwrap() as usize;

        let control = StreamControl::new(ALG);
        let mut filter = SkipFilter::new(control.clone());
        let mut out = Vec::new();
        for (i, b) in input.into_iter().enumerate() {
            if i == after {
                for id in &ids {
                    assert!(control.observe(&skip_buffer(id, ALG)));
                }
            }
            out.extend(filter.push(b));
        }
        let want = common::records(case["output"].as_str().unwrap());
        assert_eq!(out.len(), want.len(), "{name}: buffer count");
        assert!(
            common::encode(&out) == want,
            "{name}: output differs from the reference"
        );
    }
}

#[test]
fn strip_matches_the_reference() {
    let m = common::manifest();
    for case in common::cases(&m, "strip_skips") {
        let name = case["name"].as_str().unwrap();
        let control = StreamControl::new(ALG);
        let out: Vec<Buffer> = common::stream(case["input"].as_str().unwrap())
            .into_iter()
            .filter_map(|b| strip_skip(&control, b))
            .collect();
        assert!(
            common::encode(&out) == common::records(case["output"].as_str().unwrap()),
            "{name}"
        );
        let observed: Vec<String> = control.peer_skips().iter().map(hex::encode).collect();
        let want: Vec<String> = case["observed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(observed, want, "{name}");
    }
}

#[test]
fn request_skip_queues_once_per_id() {
    let c = StreamControl::new(ALG);
    c.request_skip(&[1u8; 32]);
    c.request_skip(&[2u8; 32]);
    c.request_skip(&[1u8; 32]);
    let out = c.take_outbound();
    assert_eq!(
        out,
        vec![skip_buffer(&[1u8; 32], ALG), skip_buffer(&[2u8; 32], ALG)]
    );
    assert!(c.take_outbound().is_empty());
    c.request_skip(&[1u8; 32]);
    assert!(
        c.take_outbound().is_empty(),
        "an id asked before is not asked again"
    );
}

#[test]
fn observe_ignores_other_buffers() {
    let c = StreamControl::new(ALG);
    assert!(!c.observe(&Buffer {
        chunk: Some(b"x".to_vec()),
        ..Default::default()
    }));
    assert!(!c.should_skip(&[1u8; 32]));
    assert!(c.peer_skips().is_empty());
}

#[test]
fn control_is_shared_and_thread_safe() {
    fn check<T: Send + Sync + 'static>() {}
    check::<StreamControl>();
    let c = StreamControl::new(ALG);
    let d = c.clone();
    std::thread::spawn(move || d.request_skip(&[3u8; 32]))
        .join()
        .unwrap();
    assert_eq!(c.take_outbound().len(), 1);
}

#[test]
fn filter_without_requests_passes_everything() {
    let m = common::manifest();
    let input = common::stream("parse_nested_depth2.bee");
    let _ = &m;
    let mut f = SkipFilter::new(StreamControl::new(ALG));
    let out: Vec<Buffer> = input.iter().cloned().filter_map(|b| f.push(b)).collect();
    assert_eq!(out, input);
}
