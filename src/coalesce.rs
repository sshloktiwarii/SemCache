use std::sync::Arc;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use crate::error::SemCacheError;

pub type InFlightPayload = Result<Bytes, Arc<SemCacheError>>;

#[derive(Clone, Debug)]
pub enum CoalesceState {
    Pending(broadcast::Sender<InFlightPayload>),
    Ready(Bytes),
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
    pub rx: broadcast::Receiver<InFlightPayload>,
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
    /// Transitions state in InFlightMap to Ready(data) and broadcasts success to all pending followers.
    ///
    /// CRITICAL FIX (The Persistence Gap):
    /// Retaining Ready(Bytes) in InFlightMap ensures that any request arriving
    /// during the async SQLite disk-commit window is served directly from memory,
    /// eliminating redundant upstream calls.
    pub fn mark_ready_and_broadcast(&mut self, data: Bytes) {
        self.completed = true;
        self.in_flight.insert(self.hash, CoalesceState::Ready(data.clone()));
        let _ = self.tx.send(Ok(data));
    }

    /// Evicts the hash entry from InFlightMap after SQLite persistence commits.
    pub fn evict(&self) {
        self.in_flight.remove(&self.hash);
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
    /// - If Ready(bytes) is present: returns immediately from memory (Persistence Gap closed).
    /// - If Pending(tx) is present: subscribes and awaits broadcast.
    /// - If Vacant: atomically inserts Pending and becomes Primary Leader.
    pub async fn register_or_wait(&self, hash: [u8; 32]) -> Result<CoalesceResult, SemCacheError> {
        let mut rx = {
            use dashmap::mapref::entry::Entry;
            match self.in_flight.entry(hash) {
                Entry::Occupied(entry) => {
                    match entry.get() {
                        CoalesceState::Ready(bytes) => {
                            return Ok(CoalesceResult::Coalesced(bytes.clone()));
                        }
                        CoalesceState::Pending(tx) => tx.subscribe(),
                    }
                }
                Entry::Vacant(entry) => {
                    let (tx, rx) = broadcast::channel(64);
                    entry.insert(CoalesceState::Pending(tx.clone()));
                    return Ok(CoalesceResult::Primary(LeaderGuard {
                        hash,
                        in_flight: self.in_flight.clone(),
                        tx,
                        rx,
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

        // Immediate follow-up arrives before eviction (Persistence Gap check)
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
        let guard = match primary_res {
            CoalesceResult::Primary(g) => g,
            _ => panic!("Expected primary worker"),
        };

        let coalescer_clone = coalescer.clone();
        let handle = tokio::spawn(async move {
            coalescer_clone.register_or_wait(hash).await
        });

        tokio::task::yield_now().await;

        guard.broadcast_error(SemCacheError::UpstreamError(429, "Rate limit exceeded".to_string()));

        let follower_res = handle.await.unwrap();
        assert!(follower_res.is_err());
        match follower_res.unwrap_err() {
            SemCacheError::UpstreamError(code, msg) => {
                assert_eq!(code, 429);
                assert_eq!(msg, "Rate limit exceeded");
            }
            other => panic!("Expected UpstreamError 429, got {:?}", other),
        }
        assert!(!coalescer.in_flight.contains_key(&hash));
    }

    #[tokio::test]
    async fn test_leader_dropped_prematurely_notifies_followers() {
        let coalescer = RequestCoalescer::new();
        let hash = [123u8; 32];

        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let guard = match primary_res {
            CoalesceResult::Primary(g) => g,
            _ => panic!("Expected primary worker"),
        };

        let coalescer_clone = coalescer.clone();
        let handle = tokio::spawn(async move {
            coalescer_clone.register_or_wait(hash).await
        });

        tokio::task::yield_now().await;

        drop(guard);

        let follower_res = handle.await.unwrap();
        assert!(follower_res.is_err());
        assert!(!coalescer.in_flight.contains_key(&hash));
    }
}
