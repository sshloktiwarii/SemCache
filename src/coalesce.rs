use std::sync::atomic::{AtomicUsize, Ordering};
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

#[derive(Clone)]
pub struct RequestCoalescer {
    pub in_flight: InFlightMap,
    pub ready_bytes_total: Arc<AtomicUsize>,
    pub max_ready_bytes: usize,
}

impl Default for RequestCoalescer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardState {
    Pending,
    Ready,
    Evicted,
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
    pub state: GuardState,
    pub completed: bool,
    pub ready_bytes_total: Arc<AtomicUsize>,
    pub max_ready_bytes: usize,
}

impl std::fmt::Debug for LeaderGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderGuard")
            .field("hash", &self.hash)
            .field("state", &self.state)
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
    /// Also enforces `max_ready_bytes` to prevent RAM exhaustion from many large responses.
    pub fn mark_ready_and_broadcast(&mut self, data: Bytes) {
        self.state = GuardState::Ready;
        self.completed = true;
        let data_len = data.len();

        let current = self.ready_bytes_total.load(Ordering::Relaxed);
        if current + data_len <= self.max_ready_bytes {
            self.ready_bytes_total.fetch_add(data_len, Ordering::Relaxed);
            self.in_flight.insert(self.hash, CoalesceState::Ready(data.clone(), Instant::now()));
            let _ = self.tx.send(Ok(data));
        } else {
            tracing::warn!(
                "Ready state memory cap reached ({}/{} bytes); broadcasting and bypassing RAM retention",
                current + data_len,
                self.max_ready_bytes
            );
            let _ = self.tx.send(Ok(data));
            self.evict();
        }
    }

    /// Evicts the hash entry from InFlightMap after SQLite persistence commits.
    pub fn evict(&mut self) {
        self.state = GuardState::Evicted;
        if let Some((_hash, CoalesceState::Ready(bytes, _))) = self.in_flight.remove(&self.hash) {
            self.ready_bytes_total.fetch_sub(bytes.len(), Ordering::Relaxed);
        }
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
        self.state = GuardState::Evicted;
        self.completed = true;
        if let Some((_hash, CoalesceState::Ready(bytes, _))) = self.in_flight.remove(&self.hash) {
            self.ready_bytes_total.fetch_sub(bytes.len(), Ordering::Relaxed);
        }
        let _ = self.tx.send(Err(Arc::new(err)));
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if self.state == GuardState::Pending {
            // Only broadcast error and remove if it was abandoned mid-flight
            let _ = self.tx.send(Err(Arc::new(SemCacheError::UpstreamError(
                500,
                "Leader aborted".to_string(),
            ))));
            self.in_flight.remove(&self.hash);
        }
    }
}

impl RequestCoalescer {
    pub fn new() -> Self {
        Self::with_max_ready_bytes(128 * 1024 * 1024) // 128 MB default
    }

    pub fn with_max_ready_bytes(max_ready_bytes: usize) -> Self {
        Self {
            in_flight: Arc::new(DashMap::new()),
            ready_bytes_total: Arc::new(AtomicUsize::new(0)),
            max_ready_bytes,
        }
    }

    pub fn ready_bytes(&self) -> usize {
        self.ready_bytes_total.load(Ordering::Relaxed)
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
                                let bytes_len = bytes.len();
                                self.ready_bytes_total.fetch_sub(bytes_len, Ordering::Relaxed);
                                let (tx, _rx) = broadcast::channel(64);
                                let leader_rx = tx.subscribe();
                                drop(_rx);
                                entry.insert(CoalesceState::Pending(tx.clone()));
                                return Ok(CoalesceResult::Primary(
                                    LeaderGuard {
                                        hash,
                                        in_flight: self.in_flight.clone(),
                                        tx,
                                        state: GuardState::Pending,
                                        completed: false,
                                        ready_bytes_total: self.ready_bytes_total.clone(),
                                        max_ready_bytes: self.max_ready_bytes,
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
                            state: GuardState::Pending,
                            completed: false,
                            ready_bytes_total: self.ready_bytes_total.clone(),
                            max_ready_bytes: self.max_ready_bytes,
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

    /// Periodically cleans stale Ready entries older than `ttl` from InFlightMap.
    /// This prevents unrequested completed responses from leaking memory.
    pub fn sweep_stale_ready(&self, ttl: Duration) -> usize {
        let mut swept = 0;
        self.in_flight.retain(|_hash, state| {
            if let CoalesceState::Ready(bytes, timestamp) = state {
                if timestamp.elapsed() >= ttl {
                    self.ready_bytes_total.fetch_sub(bytes.len(), Ordering::Relaxed);
                    swept += 1;
                    return false;
                }
            }
            true
        });
        swept
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
                assert_eq!(code, 500);
                assert!(msg.contains("Leader aborted"));
            }
            other => panic!("Expected 500 error from dropped guard, got: {:?}", other),
        }

        assert!(!coalescer.in_flight.contains_key(&hash));
    }

    #[tokio::test]
    async fn test_ghost_task_listener_count_detection() {
        let coalescer = RequestCoalescer::new();
        let hash = [88u8; 32];

        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let (mut guard, leader_rx) = match primary_res {
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

        guard.evict();
    }

    #[tokio::test]
    async fn test_sweep_stale_ready() {
        let coalescer = RequestCoalescer::new();
        let hash = [77u8; 32];

        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let (mut guard, _rx) = match primary_res {
            CoalesceResult::Primary(g, rx) => (g, rx),
            _ => panic!("Expected primary worker"),
        };

        guard.mark_ready_and_broadcast(Bytes::from_static(b"{\"ok\":true}"));
        assert_eq!(coalescer.in_flight.len(), 1);

        // Sweep with 0 duration should immediately purge the ready entry
        let swept = coalescer.sweep_stale_ready(Duration::from_millis(0));
        assert_eq!(swept, 1);
        assert_eq!(coalescer.in_flight.len(), 0);
    }

    #[tokio::test]
    async fn test_ready_memory_budget_enforcement() {
        // Coalescer with a tiny 100-byte budget
        let coalescer = RequestCoalescer::with_max_ready_bytes(100);
        let hash1 = [1u8; 32];
        let hash2 = [2u8; 32];

        // First payload: 60 bytes (within budget)
        let primary1 = coalescer.register_or_wait(hash1).await.unwrap();
        let (mut guard1, _rx1) = match primary1 {
            CoalesceResult::Primary(g, rx) => (g, rx),
            _ => panic!("Expected primary"),
        };
        let payload1 = Bytes::from(vec![1u8; 60]);
        guard1.mark_ready_and_broadcast(payload1);

        assert_eq!(coalescer.ready_bytes(), 60);
        assert!(coalescer.in_flight.contains_key(&hash1));

        // Second payload: 50 bytes (60 + 50 = 110 > 100 budget -> exceeds!)
        let primary2 = coalescer.register_or_wait(hash2).await.unwrap();
        let (mut guard2, mut rx2) = match primary2 {
            CoalesceResult::Primary(g, rx) => (g, rx),
            _ => panic!("Expected primary"),
        };
        let payload2 = Bytes::from(vec![2u8; 50]);
        guard2.mark_ready_and_broadcast(payload2.clone());

        // Follower/leader receiver still receives the bytes
        let recv2 = rx2.recv().await.unwrap().unwrap();
        assert_eq!(recv2, payload2);

        // But entry is NOT held in in_flight Ready cache
        assert!(!coalescer.in_flight.contains_key(&hash2));
        assert_eq!(coalescer.ready_bytes(), 60);

        // Evicting guard1 decrements to 0
        guard1.evict();
        assert_eq!(coalescer.ready_bytes(), 0);
        assert!(!coalescer.in_flight.contains_key(&hash1));
    }
}
