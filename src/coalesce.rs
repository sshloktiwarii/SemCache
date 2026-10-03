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
    /// Provides the `LeaderGuard` and the leader's own atomically subscribed receiver.
    Primary(LeaderGuard, broadcast::Receiver<InFlightPayload>),
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
    /// CRITICAL FIX (Ghost Tasks without t=0 race):
    /// Because the leader is subscribed atomically at `register_or_wait`,
    /// `receiver_count() == 0` strictly indicates that the initiating client
    /// has dropped connection (e.g. laptop closed) and zero followers have subscribed.
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
    /// - If Vacant: atomically inserts Pending and becomes Primary Leader with an active subscriber.
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
                                let leader_rx = tx.subscribe();
                                drop(_rx);
                                entry.insert(CoalesceState::Pending(tx.clone()));
                                return Ok(CoalesceResult::Primary(
                                    LeaderGuard {
                                        hash,
                                        in_flight: self.in_flight.clone(),
                                        tx,
                                        completed: false,
                                    },
                                    leader_rx,
                                ));
                            }
                        }
                        CoalesceState::Pending(tx) => tx.subscribe(),
                    }
                }
                Entry::Vacant(entry) => {
                    let (tx, _rx) = broadcast::channel(64);
                    let leader_rx = tx.subscribe();
                    drop(_rx);
                    entry.insert(CoalesceState::Pending(tx.clone()));
                    return Ok(CoalesceResult::Primary(
                        LeaderGuard {
                            hash,
                            in_flight: self.in_flight.clone(),
                            tx,
                            completed: false,
                        },
                        leader_rx,
                    ));
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
        let (mut guard, _leader_rx) = match primary_res {
            CoalesceResult::Primary(g, rx) => (g, rx),
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
    async fn test_request_coalescer_follower_receives_error_without_deadlock() {
        let coalescer = RequestCoalescer::new();
        let hash = [99u8; 32];

        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let (guard, _leader_rx) = match primary_res {
            CoalesceResult::Primary(g, rx) => (g, rx),
            _ => panic!("Expected primary worker"),
        };

        let coalescer_clone = coalescer.clone();
        let follower_handle = tokio::spawn(async move {
            coalescer_clone.register_or_wait(hash).await
        });

        tokio::task::yield_now().await;

        // Leader broadcasts upstream error (e.g. 504 Gateway Timeout)
        guard.broadcast_error(SemCacheError::UpstreamError(504, "Upstream Gateway Timeout".to_string()));

        let follower_res = follower_handle.await.unwrap();
        match follower_res {
            Err(SemCacheError::UpstreamError(code, msg)) => {
                assert_eq!(code, 504);
                assert!(msg.contains("Upstream Gateway Timeout"));
            }
            other => panic!("Expected UpstreamError, got: {:?}", other),
        }

        assert!(!coalescer.in_flight.contains_key(&hash));
    }

    #[tokio::test]
    async fn test_leader_dropped_prematurely_notifies_followers() {
        let coalescer = RequestCoalescer::new();
        let hash = [123u8; 32];

        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let (guard, _leader_rx) = match primary_res {
            CoalesceResult::Primary(g, rx) => (g, rx),
            _ => panic!("Expected primary worker"),
        };

        let coalescer_clone = coalescer.clone();
        let follower_handle = tokio::spawn(async move {
            coalescer_clone.register_or_wait(hash).await
        });

        tokio::task::yield_now().await;

        // Simulate crash / panic / cancel: LeaderGuard dropped without calling mark_ready
        drop(guard);

        let follower_res = follower_handle.await.unwrap();
        match follower_res {
            Err(SemCacheError::UpstreamError(code, msg)) => {
                assert_eq!(code, 502);
                assert!(msg.contains("terminated without completing response"));
            }
            other => panic!("Expected 502 error from dropped guard, got: {:?}", other),
        }

        assert!(!coalescer.in_flight.contains_key(&hash));
    }

    #[tokio::test]
    async fn test_ghost_task_listener_count_detection() {
        let coalescer = RequestCoalescer::new();
        let hash = [88u8; 32];

        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let (guard, leader_rx) = match primary_res {
            CoalesceResult::Primary(g, rx) => (g, rx),
            _ => panic!("Expected primary worker"),
        };

        // When leader is connected:
        assert!(guard.has_active_listeners(), "Leader must be registered as active listener at t=0");

        // Follower connects:
        let follower_sub = guard.tx.subscribe();
        assert_eq!(guard.tx.receiver_count(), 2);

        // Leader disconnects (client closes laptop):
        drop(leader_rx);
        assert!(guard.has_active_listeners(), "Follower still keeps task alive");

        // Follower disconnects:
        drop(follower_sub);
        assert!(!guard.has_active_listeners(), "With zero listeners, task is detected as ghost");
    }
}
