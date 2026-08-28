use golem_rust::bindings::golem::api::host::{
    GetPromiseResult, PromiseId, complete_promise, create_promise, get_promise,
};
use golem_rust::bindings::golem::durability::durability::{
    DurableFunctionType, LazyInitializedPollable,
};
use golem_rust::durability::Durability;
use golem_rust::golem_wasm::{NodeBuilder, Pollable, WitValueExtractor};
use golem_rust::value_and_type::type_builder::TypeNodeBuilder;
use golem_rust::value_and_type::{FromValueAndType, IntoValue};
use golem_rust::{
    PersistenceLevel, agent_definition, agent_implementation, with_persistence_level,
};
use golem_wasi_http::{Client, IncomingBody, InputStream, Method};
use std::cell::RefCell;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
struct StructuredInput {
    pub payload: String,
}

impl IntoValue for StructuredInput {
    fn add_to_builder<T: NodeBuilder>(self, builder: T) -> T::Result {
        builder.record().item().string(&self.payload).finish()
    }

    fn add_to_type_builder<T: TypeNodeBuilder>(builder: T) -> T::Result {
        builder
            .record(
                Some("StructuredInput".to_string()),
                Some("golem:it/golem-it-api".to_string()),
            )
            .field("payload")
            .string()
            .finish()
    }
}

impl FromValueAndType for StructuredInput {
    fn from_extractor<'a, 'b>(
        extractor: &'a impl WitValueExtractor<'a, 'b>,
    ) -> Result<Self, String> {
        Ok(Self {
            payload: extractor
                .field(0)
                .ok_or_else(|| "Missing field: 'payload'".to_string())?
                .string()
                .ok_or_else(|| "The 'payload' field is not a string".to_string())?
                .to_string(),
        })
    }
}

#[derive(Debug, Clone)]
struct StructuredResult {
    pub result: String,
}

impl IntoValue for StructuredResult {
    fn add_to_builder<T: NodeBuilder>(self, builder: T) -> T::Result {
        builder.record().item().string(&self.result).finish()
    }

    fn add_to_type_builder<T: TypeNodeBuilder>(builder: T) -> T::Result {
        builder
            .record(
                Some("StructuredResult".to_string()),
                Some("golem:it/golem-it-api".to_string()),
            )
            .field("result")
            .string()
            .finish()
    }
}

impl FromValueAndType for StructuredResult {
    fn from_extractor<'a, 'b>(
        extractor: &'a impl WitValueExtractor<'a, 'b>,
    ) -> Result<Self, String> {
        Ok(Self {
            result: extractor
                .field(0)
                .ok_or_else(|| "Missing field: 'result'".to_string())?
                .string()
                .ok_or_else(|| "The 'result' field is not a string".to_string())?
                .to_string(),
        })
    }
}

#[derive(Debug)]
struct UnusedError;

impl Display for UnusedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnusedError")
    }
}

impl IntoValue for UnusedError {
    fn add_to_builder<T: NodeBuilder>(self, builder: T) -> T::Result {
        builder.variant_unit(0)
    }

    fn add_to_type_builder<T: TypeNodeBuilder>(builder: T) -> T::Result {
        builder
            .variant(
                Some("UnusedError".to_string()),
                Some("golem:it/golem-it-api".to_string()),
            )
            .unit_case("unused-error")
            .finish()
    }
}

impl FromValueAndType for UnusedError {
    fn from_extractor<'a, 'b>(
        extractor: &'a impl WitValueExtractor<'a, 'b>,
    ) -> Result<Self, String> {
        let (idx, _inner) = extractor
            .variant()
            .ok_or_else(|| "UnusedError should be variant".to_string())?;
        if idx == 0 {
            Ok(UnusedError)
        } else {
            Err(format!("UnusedError should be variant 0, but got {idx}"))
        }
    }
}

#[agent_definition]
pub trait CustomDurability {
    fn new(name: String) -> Self;

    fn callback(&self, payload: String) -> String;

    fn lazy_pollable_init(&mut self);
    fn lazy_pollable_test(&self, n: u32) -> String;

    /// Creates 4 concurrently-pending promises (matching the production `scene_plates` shape —
    /// several concurrently in-flight pollables in one worker, backed by the same
    /// create_promise()/await_promise() primitive `workflowToolStart`/`Finish` uses for render
    /// waits) and initializes their lazy pollables. No persist-nothing wrapping — exercises the
    /// real ready()/poll() durability path (unlike lazy_pollable_test, which deliberately skips
    /// it).
    fn concurrent_promise_init(&mut self);
    /// Completes promise `idx` (0..4) with a payload identifying it, from OUTSIDE the poll
    /// loop — mirrors a different agent completing the promise asynchronously.
    fn concurrent_promise_complete(&self, idx: u32);
    /// Does ONE round of batched `poll()` over still-pending promise pollables (blocks until
    /// >=1 is ready, consumes all that are via `get_promise(..).get()`), returning the current
    /// state joined by `|` in dispatch order ("-" for still-unresolved slots). Call repeatedly
    /// (safe across a worker restart between calls — already-resolved slots are skipped) until
    /// no "-" remain.
    fn concurrent_promise_test(&self) -> String;
}

const CONCURRENT_POLLABLE_COUNT: usize = 4;

pub struct CustomDurabilityImpl {
    _name: String,
    lazy_pollable: Option<LazyInitializedPollable>,
    pollable: Option<Pollable>,
    response: RefCell<Option<golem_wasi_http::Response>>,
    input_stream: RefCell<Option<InputStream>>,
    body: RefCell<Option<IncomingBody>>,

    concurrent_promise_ids: Vec<PromiseId>,
    // Kept alive alongside the subscribed pollable below — a subscribed Pollable is a CHILD
    // resource of its parent GetPromiseResult; dropping the parent while the child is
    // still alive traps with "resource has children".
    concurrent_promise_entries: Vec<GetPromiseResult>,
    concurrent_lazy_pollables: Vec<LazyInitializedPollable>,
    concurrent_pollables: Vec<Pollable>,
    concurrent_results: RefCell<Vec<Option<String>>>,
}

#[agent_implementation]
impl CustomDurability for CustomDurabilityImpl {
    fn new(name: String) -> Self {
        Self {
            _name: name,
            lazy_pollable: None,
            pollable: None,
            response: RefCell::new(None),
            input_stream: RefCell::new(None),
            body: RefCell::new(None),
            concurrent_promise_ids: Vec::new(),
            concurrent_promise_entries: Vec::new(),
            concurrent_lazy_pollables: Vec::new(),
            concurrent_pollables: Vec::new(),
            concurrent_results: RefCell::new(vec![None; CONCURRENT_POLLABLE_COUNT]),
        }
    }

    fn callback(&self, payload: String) -> String {
        let durability = Durability::<StructuredResult, UnusedError>::new(
            "golem-it",
            "test-callback",
            DurableFunctionType::WriteRemote,
        );
        if durability.is_live() {
            let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                perform_callback(payload.clone())
            });
            durability
                .persist_infallible(StructuredInput { payload }, StructuredResult { result })
                .result
        } else {
            durability.replay_infallible::<StructuredResult>().result
        }
    }

    fn lazy_pollable_init(&mut self) {
        let lazy_pollable = LazyInitializedPollable::new();
        let pollable = lazy_pollable.subscribe();
        self.lazy_pollable = Some(lazy_pollable);
        self.pollable = Some(pollable);
    }

    fn lazy_pollable_test(&self, n: u32) -> String {
        let durability = Durability::<StructuredResult, UnusedError>::new(
            "golem-it",
            "test-callback",
            DurableFunctionType::WriteRemote,
        );
        if durability.is_live() {
            let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                let mut response = self.response.borrow_mut();
                if response.is_none() {
                    let port = std::env::var("PORT").unwrap_or("9999".to_string());
                    let client = Client::new();
                    let mut new_response = client
                        .request(
                            Method::GET,
                            format!("http://localhost:{port}/fetch?idx={n}"),
                        )
                        .send()
                        .expect("Request failed");
                    let (input_stream, body) = new_response.get_raw_input_stream();
                    let pollable = input_stream.subscribe();
                    self.lazy_pollable
                        .as_ref()
                        .expect("lazy_pollable_init must be called first")
                        .set(unsafe { std::mem::transmute(pollable) });
                    *response = Some(new_response);
                    self.body.replace(Some(body));
                    self.input_stream.replace(Some(input_stream));
                }

                self.pollable
                    .as_ref()
                    .expect("lazy_pollable_init must be called first")
                    .block();
                let buf = self
                    .input_stream
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .read(100)
                    .unwrap();
                String::from_utf8(buf).unwrap()
            });

            durability
                .persist_infallible(
                    StructuredInput {
                        payload: n.to_string(),
                    },
                    StructuredResult { result },
                )
                .result
        } else {
            durability.replay_infallible::<StructuredResult>().result
        }
    }

    fn concurrent_promise_init(&mut self) {
        for _idx in 0..CONCURRENT_POLLABLE_COUNT {
            let promise_id = create_promise();
            let lazy_pollable = LazyInitializedPollable::new();
            let pollable = lazy_pollable.subscribe();
            self.concurrent_lazy_pollables.push(lazy_pollable);
            self.concurrent_pollables.push(pollable);

            let promise_entry = get_promise(&promise_id);
            let promise_pollable = promise_entry.subscribe();
            self.concurrent_lazy_pollables[_idx]
                .set(unsafe { std::mem::transmute(promise_pollable) });
            self.concurrent_promise_entries.push(promise_entry);
            self.concurrent_promise_ids.push(promise_id);
        }
    }

    fn concurrent_promise_complete(&self, idx: u32) {
        let promise_id = &self.concurrent_promise_ids[idx as usize];
        let payload = format!("promise-{idx}").into_bytes();
        complete_promise(promise_id, &payload);
    }

    fn concurrent_promise_test(&self) -> String {
        // Real (non-persist-nothing) polling over N concurrently in-flight promise-backed
        // pollables — the exact shape of the production trap (and the exact primitive
        // `workflowToolStart`/`Finish` uses for concurrent render waits): several pollables
        // live at once, only some ready at any given round, no ordering guarantee about which
        // becomes ready first. Uses the batched wasi:io/poll.poll() free function (blocks
        // until >=1 ready, returns the ready indices) — this is the `Host::poll` path with
        // `in_.len() > 1`, not just the single-pollable `block()`/`ready()` path
        // lazy_pollable_test already covers.
        //
        // Does exactly ONE round (one poll() call, consuming everything ready that round) and
        // returns the current state, unresolved slots shown as "-" — mirroring the production
        // round-based invocation shape (one bounded unit of work per external call) rather than
        // looping to completion in-process. The test driver calls this repeatedly, restarting
        // the executor between rounds to force genuine oplog replay of the partially-resolved
        // concurrent state.
        let all_done = self.concurrent_results.borrow().iter().all(|r| r.is_some());
        if !all_done {
            let pending_indices: Vec<usize> = self
                .concurrent_results
                .borrow()
                .iter()
                .enumerate()
                .filter(|(_, r)| r.is_none())
                .map(|(i, _)| i)
                .collect();
            let pending_pollables: Vec<&Pollable> = pending_indices
                .iter()
                .map(|&i| &self.concurrent_pollables[i])
                .collect();
            let ready_positions = golem_rust::wasip2::io::poll::poll(&pending_pollables);
            for pos in ready_positions {
                let idx = pending_indices[pos as usize];
                let promise_id = &self.concurrent_promise_ids[idx];
                let data = get_promise(promise_id)
                    .get()
                    .expect("promise pollable was ready but get() returned None");
                self.concurrent_results.borrow_mut()[idx] =
                    Some(String::from_utf8(data).unwrap());
            }
        }
        self.concurrent_results
            .borrow()
            .iter()
            .map(|r| r.clone().unwrap_or_else(|| "-".to_string()))
            .collect::<Vec<_>>()
            .join("|")
    }
}

fn perform_callback(payload: String) -> String {
    let port = std::env::var("PORT").unwrap_or("9999".to_string());
    Client::new()
        .request(
            Method::GET,
            format!("http://localhost:{port}/callback?payload={payload}"),
        )
        .send()
        .expect("Request failed")
        .text()
        .expect("Failed to read response text")
}
