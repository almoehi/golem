//! Reproducer for the `io::poll::poll` replay-order bug (golem-worker-executor
//! `durable_host/io/poll.rs`, `map_recorded_poll_ready`).
//!
//! Like wstd's reactor (`wstd 0.6.5`, `runtime/reactor.rs`), `wait_any` builds the `poll()`
//! target list by iterating a `HashMap` whose keys carry a process-wide, monotonically
//! increasing counter — so the list ORDER depends on the instance's whole history, which a
//! snapshot does not capture. Unlike wstd, the reproducer TRUSTS `poll()`'s answer (it does not
//! re-check `ready()` on the woken pollable), which is the WASI contract and what a JS timer
//! callback in wasm-rquickjs effectively does: a woken timeout fires.
//!
//! The hasher has fixed keys, so the order is a deterministic function of the counter — and the
//! counter restarts at 0 in every fresh instance (e.g. after a snapshot restore), while the live
//! instance kept counting from its earlier invocations (`warm_up`).

use golem_rust::{agent_definition, agent_implementation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicU64, Ordering};
use wasi::clocks::monotonic_clock::subscribe_duration;
use wasi::http::outgoing_handler;
use wasi::http::types::{FutureIncomingResponse, Method, OutgoingBody, OutgoingRequest, Scheme};
use wasi::io::poll::{Pollable, poll};
use wasi::io::streams::StreamError;

/// A wasi-http fetch's default "no timeout" timer (~317 years), as in the production oplog.
const NEVER_NANOS: u64 = 10_000_000_000_000_000_000;
const UPLOAD_SIZE: usize = 2 * 1024 * 1024;
const UPLOAD_CHUNK: usize = 16 * 1024;

/// Process-wide, like wstd's `WaitFor` `COUNTER` — not part of any snapshot.
static WAIT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Waiter {
    Response,
    Timeout,
    BodyWrite,
}

#[derive(PartialEq, Eq, Hash)]
struct WaitKey {
    waiter: Waiter,
    unique: u64,
}

type FixedKeyHashMap<K, V> = HashMap<K, V, BuildHasherDefault<DefaultHasher>>;

/// Waits until at least one of `waiting` is ready and returns which ones `poll()` reported.
/// Like wstd's `WaitFor::poll`, checks `ready()` first and only registers a waiter otherwise.
fn wait_any(waiting: &[(Waiter, &Pollable)]) -> Vec<Waiter> {
    let ready_now: Vec<Waiter> = waiting
        .iter()
        .filter(|(_, pollable)| pollable.ready())
        .map(|(waiter, _)| *waiter)
        .collect();
    if !ready_now.is_empty() {
        return ready_now;
    }

    let mut wakers: FixedKeyHashMap<WaitKey, &Pollable> = FixedKeyHashMap::default();
    for (waiter, pollable) in waiting {
        let unique = WAIT_COUNTER.fetch_add(1, Ordering::Relaxed);
        wakers.insert(
            WaitKey {
                waiter: *waiter,
                unique,
            },
            pollable,
        );
    }
    let (order, targets): (Vec<Waiter>, Vec<&Pollable>) = wakers
        .iter()
        .map(|(key, pollable)| (key.waiter, *pollable))
        .unzip();
    poll(&targets)
        .into_iter()
        .map(|index| order[index as usize])
        .collect()
}

/// One request racing a never-firing timeout; a body (if any) is written with backpressure,
/// waiting on body-write + response + timeout together (a 3-pollable `poll()`).
fn fetch(port: u16, method: Method, path: &str, body: Option<&[u8]>) -> String {
    let request = OutgoingRequest::new(wasi::http::types::Fields::new());
    request.set_method(&method).unwrap();
    request.set_scheme(Some(&Scheme::Http)).unwrap();
    request
        .set_authority(Some(&format!("127.0.0.1:{port}")))
        .unwrap();
    request.set_path_with_query(Some(path)).unwrap();
    let outgoing_body = request.body().unwrap();

    let future = match outgoing_handler::handle(request, None) {
        Ok(future) => future,
        Err(err) => return format!("handle-error:{err:?}"),
    };
    let timeout = subscribe_duration(NEVER_NANOS);
    let response_ready = future.subscribe();

    if let Some(body) = body {
        let stream = outgoing_body.write().unwrap();
        let writable = stream.subscribe();
        let mut offset = 0;
        while offset < body.len() {
            let permit = match stream.check_write() {
                Ok(permit) => permit as usize,
                Err(err) => return format!("check-write-error:{err:?}"),
            };
            if permit == 0 {
                let woken = wait_any(&[
                    (Waiter::BodyWrite, &writable),
                    (Waiter::Response, &response_ready),
                    (Waiter::Timeout, &timeout),
                ]);
                if woken.contains(&Waiter::Timeout) {
                    return "timeout-while-uploading".to_string();
                }
                continue;
            }
            let end = (offset + permit.min(UPLOAD_CHUNK)).min(body.len());
            if let Err(err) = stream.write(&body[offset..end]) {
                return format!("write-error:{err:?}");
            }
            offset = end;
        }
        drop(writable);
        drop(stream);
    }
    OutgoingBody::finish(outgoing_body, None).unwrap();

    loop {
        let woken = wait_any(&[
            (Waiter::Response, &response_ready),
            (Waiter::Timeout, &timeout),
        ]);
        if woken.contains(&Waiter::Timeout) {
            return "timeout".to_string();
        }
        if woken.contains(&Waiter::Response) {
            break;
        }
    }
    drop(response_ready);
    drop(timeout);
    read_response(future)
}

fn read_response(future: FutureIncomingResponse) -> String {
    let response = match future.get() {
        Some(Ok(Ok(response))) => response,
        other => return format!("response-error:{other:?}"),
    };
    let status = response.status();
    let body = response.consume().unwrap();
    let stream = body.stream().unwrap();
    let mut bytes = Vec::new();
    loop {
        match stream.blocking_read(64 * 1024) {
            Ok(chunk) => bytes.extend_from_slice(&chunk),
            Err(StreamError::Closed) => break,
            Err(err) => return format!("read-error:{err:?}"),
        }
    }
    drop(stream);
    drop(body);
    drop(response);
    format!("{status}:{}", String::from_utf8_lossy(&bytes))
}

#[agent_definition(snapshotting = "enabled")]
pub trait PollOrderClient {
    fn new(name: String) -> Self;

    /// Stands in for earlier async activity: advances the process-wide wait counter.
    fn warm_up(&mut self, waits: u64) -> u64;

    /// Per round: a GET racing a timeout, then a large POST racing a timeout. The results are
    /// kept in the agent state, so a replay that diverges shows up in `results()`.
    fn racing_fetches(&mut self, rounds: u32) -> Vec<String>;

    fn results(&self) -> Vec<String>;
}

#[derive(Serialize, Deserialize)]
struct PollOrderClientImpl {
    name: String,
    warm_up_waits: u64,
    results: Vec<String>,
}

#[agent_implementation]
impl PollOrderClient for PollOrderClientImpl {
    fn new(name: String) -> Self {
        Self {
            name,
            warm_up_waits: 0,
            results: Vec::new(),
        }
    }

    fn warm_up(&mut self, waits: u64) -> u64 {
        WAIT_COUNTER.fetch_add(waits, Ordering::Relaxed);
        self.warm_up_waits += waits;
        self.warm_up_waits
    }

    fn racing_fetches(&mut self, rounds: u32) -> Vec<String> {
        let port: u16 = std::env::var("PORT").unwrap().parse().unwrap();
        let upload = vec![b'x'; UPLOAD_SIZE];
        for round in 0..rounds {
            let get = fetch(port, Method::Get, &format!("/delayed/{round}"), None);
            self.results.push(get);
            let post = fetch(
                port,
                Method::Post,
                &format!("/upload/{round}"),
                Some(&upload),
            );
            self.results.push(post);
        }
        self.results.clone()
    }

    fn results(&self) -> Vec<String> {
        self.results.clone()
    }
}
