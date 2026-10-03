use std::sync::Arc;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use crate::error::SemCacheError;

pub type InFlightMap = Arc<DashMap<[u8; 32], broadcast::Sender<Bytes>>>;

#[derive(Clone, Default)]
pub struct RequestCoalescer {
    in_flight: InFlightMap,
}

pub enum CoalesceResult {
    /// The caller is the primary/leader worker responsible for fetching upstream and broadcasting.
    Primary(LeaderGuard),
    /// Another concurrent worker already fetched this request; the result is returned here.
    Coalesced(Bytes),
}

pub struct LeaderGuard {
    hash: [u8; 32],
    in_flight: InFlightMap,
    tx: broadcast::Sender<Bytes>,
}

impl LeaderGuard {
    /// Broadcasts the response payload to all coalesced waiting clients and cleans up the in-flight map.
    pub fn broadcast(self, data: Bytes) {
        let _ = self.tx.send(data);
        self.in_flight.remove(&self.hash);
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        // Guarantee the in-flight entry is cleaned up if dropped prematurely (e.g. on upstream error)
        self.in_flight.remove(&self.hash);
    }
}

impl RequestCoalescer {
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(DashMap::new()),
        }
    }

    /// Atomically checks if an in-flight request exists for `hash`.
    /// - If found, subscribes to the channel and awaits the response.
    /// - If not found, registers a new channel and returns `CoalesceResult::Primary`.
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
                    }));
                }
            }
        };

        match rx.recv().await {
            Ok(data) => Ok(CoalesceResult::Coalesced(data)),
            Err(broadcast::error::RecvError::Closed) => {
                Err(SemCacheError::UpstreamError(
                    502,
                    "In-flight primary worker terminated without broadcasting response".to_string(),
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
    async fn test_request_coalescer_single_flight() {
        let coalescer = RequestCoalescer::new();
        let hash = [42u8; 32];

        // First caller registers as Primary
        let primary_res = coalescer.register_or_wait(hash).await.unwrap();
        let guard = match primary_res {
            CoalesceResult::Primary(g) => g,
            _ => panic!("Expected primary worker"),
        };

        // Spawn a second task awaiting the same hash
        let coalescer_clone = coalescer.clone();
        let handle = tokio::spawn(async move {
            let res = coalescer_clone.register_or_wait(hash).await.unwrap();
            match res {
                CoalesceResult::Coalesced(bytes) => bytes,
                _ => panic!("Expected coalesced response"),
            }
        });

        // Yield to allow task to subscribe
        tokio::task::yield_now().await;

        let expected_payload = Bytes::from_static(b"{\"result\": \"success\"}");
        guard.broadcast(expected_payload.clone());

        let coalesced_payload = handle.await.unwrap();
        assert_eq!(coalesced_payload, expected_payload);

        // After broadcast, in-flight map must be empty for this hash
        assert!(!coalescer.in_flight.contains_key(&hash));
    }
}
