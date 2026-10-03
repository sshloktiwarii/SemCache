use std::sync::Arc;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use crate::error::SemCacheError;

pub type InFlightPayload = Result<Bytes, Arc<SemCacheError>>;
pub type InFlightMap = Arc<DashMap<[u8; 32], broadcast::Sender<InFlightPayload>>>;

#[derive(Clone, Default)]
pub struct RequestCoalescer {
    in_flight: InFlightMap,
}

#[derive(Debug)]
pub enum CoalesceResult {
    /// The caller is the primary/leader worker responsible for fetching upstream and broadcasting.
    Primary(LeaderGuard),
    /// Another concurrent worker already fetched this request; the result is returned here.
    Coalesced(Bytes),
}

pub struct LeaderGuard {
    hash: [u8; 32],
    in_flight: InFlightMap,
    tx: broadcast::Sender<InFlightPayload>,
    completed: bool,
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
    /// Broadcasts success payload to all waiting followers.
    ///
    /// CRITICAL FIX (Flaw #6): Remove the hash from `in_flight` map FIRST.
    /// This closes the race window where a late-arriving request subscribes
    /// after `send()` has already occurred and deadlocks.
    pub fn broadcast_success(mut self, data: Bytes) {
        self.completed = true;
        // 1. Remove from map first so any subsequent arrival becomes a new leader
        self.in_flight.remove(&self.hash);
        // 2. Broadcast result to all currently awaiting subscribers
        let _ = self.tx.send(Ok(data));
    }

    /// Broadcasts upstream failure to all waiting followers.
    ///
    /// CRITICAL FIX (Flaw #2 & #5): Followers receive the exact upstream failure
    /// rather than deadlocking or hanging on channel closure.
    pub fn broadcast_error(mut self, err: SemCacheError) {
        self.completed = true;
        self.in_flight.remove(&self.hash);
        let _ = self.tx.send(Err(Arc::new(err)));
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.completed {
            // Leader aborted or dropped prematurely (e.g. client cancellation or panic)
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

    /// Atomically checks if an in-flight request exists for `hash` using DashMap entry shard locking.
    ///
    /// CRITICAL FIX (Flaw #1): Acquire the shard lock via `DashMap::entry(hash)`.
    /// `Entry::Vacant` atomically becomes Primary Leader.
    /// `Entry::Occupied` atomically subscribes to the leader's broadcast channel.
    pub async fn register_or_wait(&self, hash: [u8; 32]) -> Result<CoalesceResult, SemCacheError> {
        let mut rx = {
            use dashmap::mapref::entry::Entry;
            match self.in_flight.entry(hash) {
                Entry::Occupied(entry) => entry.get().subscribe(),
                Entry::Vacant(entry) => {
                    let (tx, _rx) = broadcast::channel(16);
                    entry.insert(tx.clone());
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
        let guard = match primary_res {
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
        guard.broadcast_success(expected_payload.clone());

        let coalesced_payload = handle.await.unwrap();
        assert_eq!(coalesced_payload, expected_payload);
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

        // Leader broadcasts an upstream failure
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

        // Leader drops without explicit broadcast
        drop(guard);

        let follower_res = handle.await.unwrap();
        assert!(follower_res.is_err());
        assert!(!coalescer.in_flight.contains_key(&hash));
    }
}
