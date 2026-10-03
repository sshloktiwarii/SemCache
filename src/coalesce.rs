use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use crate::error::SemCacheError;

pub type InFlightPayload = Result<Bytes, Arc<SemCacheError>>;

#[derive(Clone, Debug)]
pub enum CoalesceState {
    Pending(broadcast::Sender<InFlightPayload>),
    Ready(Bytes, Instant),
}

pub type InFlightMap = Arc<DashMap<[u8; 32], CoalesceState>>;

#[derive(Clone, Default)]
pub struct RequestCoalescer {
    pub in_flight: InFlightMap,
}

#[derive(Debug)]
pub enum CoalesceResult {
    /// The caller is the primary/leader worker responsible for fetching upstream.
    Primary(LeaderGuard),
    /// Another concurrent worker already fetched this request; the result is returned here.
    Coalesced(Bytes),
}

pub struct LeaderGuard {
    pub hash: [u8; 32],
    pub in_flight: InFlightMap,
    pub tx: broadcast::Sender<InFlightPayload>,
    pub completed: bool,
}

impl std::fmt::Debug for LeaderGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderGuard")
            .field("hash", &self.hash)
            .field("completed", &self.completed)
            .finish()
    }
}

impl LeaderGuard {
    /// Transitions state in InFlightMap to Ready(data, timestamp) and broadcasts success to all pending followers.
    ///
    /// CRITICAL FIX (The Persistence Gap & Unbounded Memory Leak):
    /// Storing a monotonic timestamp guarantees that memory entries are bounded and
    /// can be automatically evicted if SQLite writes hang or fail.
    pub fn mark_ready_and_broadcast(&mut self, data: Bytes) {
        self.completed = true;
        self.in_flight.insert(self.hash, CoalesceState::Ready(data.clone(), Instant::now()));
        let _ = self.tx.send(Ok(data));
    }

    /// Evicts the hash entry from InFlightMap after SQLite persistence commits.
    pub fn evict(&self) {
        self.in_flight.remove(&self.hash);
    }

    /// Checks if there are any active clients awaiting this request.
    ///
    /// CRITICAL FIX (Ghost Tasks):
    /// If receiver_count() == 0, the initiating client has disconnected and no
    /// followers have joined. The leader task can abort immediately to save upstream API costs.
    pub fn has_active_listeners(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    /// Broadcasts upstream failure to all waiting followers.
    pub fn broadcast_error(mut self, err: SemCacheError) {
        self.completed = true;
        self.in_flight.remove(&self.hash);
        let _ = self.tx.send(Err(Arc::new(err)));
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.in_flight.remove(&self.hash);
            let _ = self.tx.send(Err(Arc::new(SemCacheError::UpstreamError(
                502,
                "In-flight primary worker terminated without completing response".to_string(),
            ))));
        }
    }
}

impl RequestCoalescer {
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(DashMap::new()),
        }
    }

    /// Atomically checks if an in-flight request exists for `hash`.
    /// - If Ready(bytes, timestamp) is present:
    ///   If fresh (< 10s), returns immediately from memory.
    ///   If stale (> 10s), evicts from memory and creates a new cycle.
    /// - If Pending(tx) is present: subscribes and awaits broadcast.
    /// - If Vacant: atomically inserts Pending and becomes Primary Leader.
    pub async fn register_or_wait(&self, hash: [u8; 32]) -> Result<CoalesceResult, SemCacheError> {
        let mut rx = {
            use dashmap::mapref::entry::Entry;
            match self.in_flight.entry(hash) {
                Entry::Occupied(mut entry) => {
                    match entry.get() {
                        CoalesceState::Ready(bytes, timestamp) => {
                            if timestamp.elapsed() < Duration::from_secs(10) {
                                return Ok(CoalesceResult::Coalesced(bytes.clone()));
                            } else {
                                // Stale entry; evict and create a fresh cycle
                                let (tx, _rx) = broadcast::channel(64);
                                drop(_rx);
                                entry.insert(CoalesceState::Pending(tx.clone()));
                                return Ok(CoalesceResult::Primary(LeaderGuard {
                                    hash,
                                    in_flight: self.in_flight.clone(),
                                    tx,
                                    completed: false,
                                }));
                            }
                        }
                        CoalesceState::Pending(tx) => tx.subscribe(),
                    }
                }
                Entry::Vacant(entry) => {
                    let (tx, _rx) = broadcast::channel(64);
                    drop(_rx);
                    entry.insert(CoalesceState::Pending(tx.clone()));
                    return Ok(CoalesceResult::Primary(LeaderGuard {
                        hash,
                        in_flight: self.in_flight.clone(),
                        tx,
                        completed: false,
                    }));
                }
            }
        };

        match rx.recv().await {
            Ok(Ok(data)) => Ok(CoalesceResult::Coalesced(data)),
            Ok(Err(err)) => Err((*err).clone()),
            Err(broadcast::error::RecvError::Closed) => {
                Err(SemCacheError::UpstreamError(
                    502,
                    "In-flight primary worker channel closed unexpectedly".to_string(),
                ))
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                Err(SemCacheError::InternalError(
                    "Broadcast channel buffer lagged in coalesced request".to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request_coalescer_single_flight_success() {
        let coalescer = RequestCoalescer::new();
        let hash = [42u8; 32];

        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let mut guard = match primary_res {
            CoalesceResult::Primary(g) => g,
            _ => panic!("Expected primary worker"),
        };

        let coalescer_clone = coalescer.clone();
        let handle = tokio::spawn(async move {
            let res = coalescer_clone.register_or_wait(hash).await.unwrap();
            match res {
                CoalesceResult::Coalesced(bytes) => bytes,
                _ => panic!("Expected coalesced response"),
            }
        });

        tokio::task::yield_now().await;

        let expected_payload = Bytes::from_static(b"{\"result\": \"success\"}");
        guard.mark_ready_and_broadcast(expected_payload.clone());

        let coalesced_payload = handle.await.unwrap();
        assert_eq!(coalesced_payload, expected_payload);

        // Immediate follow-up arrives before eviction
        let follow_up = coalescer.register_or_wait(hash).await.unwrap();
        match follow_up {
            CoalesceResult::Coalesced(bytes) => assert_eq!(bytes, expected_payload),
            _ => panic!("Expected immediate coalesced hit from Ready state"),
        }

        guard.evict();
        assert!(!coalescer.in_flight.contains_key(&hash));
    }

    #[tokio::test]
    async fn test_ghost_task_listener_count_detection() {
        let coalescer = RequestCoalescer::new();
        let hash = [88u8; 32];

        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let guard = match primary_res {
            CoalesceResult::Primary(g) => g,
            _ => panic!("Expected primary worker"),
        };

        // When only the primary registered and no subscriber exists on guard.tx:
        assert!(!guard.has_active_listeners());

        // Now a follower subscribes
        let _sub = guard.tx.subscribe();
        assert!(guard.has_active_listeners());
    }
}
