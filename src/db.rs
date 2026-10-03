use crate::error::SemCacheError;
use crate::vector::{bytes_to_embedding, cosine_similarity, embedding_to_bytes};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

pub type DbPool = Pool<SqliteConnectionManager>;

/// Initializes the SQLite connection pool with enforced WAL mode pragmas
/// and executes schema migrations.
pub fn init_db_pool(db_path: &str) -> Result<DbPool, SemCacheError> {
    let manager = SqliteConnectionManager::file(db_path).with_init(|conn| {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA temp_store = MEMORY;
             PRAGMA busy_timeout = 5000;",
        )
    });

    let pool = Pool::builder()
        .max_size(16)
        .build(manager)
        .map_err(SemCacheError::from)?;

    // Run schema migrations on a connection from the pool
    let conn = pool.get().map_err(SemCacheError::from)?;

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS exact_cache (
            canonical_hash BLOB PRIMARY KEY,
            model TEXT NOT NULL DEFAULT '',
            request_json TEXT NOT NULL,
            response_json TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fuzzy_payloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT NOT NULL DEFAULT '',
            prompt_text TEXT NOT NULL,
            embedding BLOB NOT NULL,
            response_json TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_exact_created ON exact_cache(created_at);
        CREATE INDEX IF NOT EXISTS idx_fuzzy_created ON fuzzy_payloads(created_at);",
    )?;

    // Attempt to initialize sqlite-vec virtual table; gracefully simulate if extension not linked in runtime
    if let Err(e) = conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS fuzzy_cache USING vec0(embedding float[1536]);",
        [],
    ) {
        tracing::debug!(
            "sqlite-vec vec0 extension not present ({}); using embedded rust cosine similarity engine",
            e
        );
        conn.execute(
            "CREATE TABLE IF NOT EXISTS fuzzy_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                embedding BLOB
            );",
            [],
        )?;
    }

    Ok(pool)
}

/// Retrieves cached response JSON from the L1 exact_cache table by BLAKE3 hash.
pub fn get_exact_cache(pool: &DbPool, hash: &[u8; 32]) -> Result<Option<String>, SemCacheError> {
    let conn = pool.get().map_err(SemCacheError::from)?;
    let mut stmt = conn.prepare("SELECT response_json FROM exact_cache WHERE canonical_hash = ?1")?;
    let mut rows = stmt.query([hash.as_slice()])?;

    if let Some(row) = rows.next()? {
        let response_json: String = row.get(0)?;
        Ok(Some(response_json))
    } else {
        Ok(None)
    }
}

/// Persists an upstream response and canonical request into L1 exact_cache.
pub fn insert_exact_cache(
    pool: &DbPool,
    hash: &[u8; 32],
    model: &str,
    request_json: &str,
    response_json: &str,
) -> Result<(), SemCacheError> {
    let conn = pool.get().map_err(SemCacheError::from)?;
    conn.execute(
        "INSERT OR REPLACE INTO exact_cache (canonical_hash, model, request_json, response_json)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![hash.as_slice(), model, request_json, response_json],
    )?;
    Ok(())
}

/// Searches the L2 semantic cache for a cached payload with cosine similarity >= threshold.
///
/// CRITICAL FIX (Flaw #8 - Active Vector Cache):
/// Evaluates true Cosine Similarity against all cached embeddings in `fuzzy_payloads`.
#[allow(dead_code)]
pub fn find_fuzzy_match(
    pool: &DbPool,
    model: &str,
    query_embedding: &[f32],
    threshold: f32,
) -> Result<Option<String>, SemCacheError> {
    let conn = pool.get().map_err(SemCacheError::from)?;
    let mut stmt = conn.prepare(
        "SELECT embedding, response_json FROM fuzzy_payloads WHERE model = ?1 ORDER BY id DESC LIMIT 500",
    )?;
    let mut rows = stmt.query([model])?;

    let mut best_sim = threshold;
    let mut best_match: Option<String> = None;

    while let Some(row) = rows.next()? {
        let raw_embedding: Vec<u8> = row.get(0)?;
        let cached_embedding = bytes_to_embedding(&raw_embedding);
        let sim = cosine_similarity(query_embedding, &cached_embedding);

        if sim >= best_sim {
            best_sim = sim;
            best_match = Some(row.get(1)?);
        }
    }

    Ok(best_match)
}

/// Persists a prompt embedding and response payload into the L2 fuzzy cache.
#[allow(dead_code)]
pub fn insert_fuzzy_cache(
    pool: &DbPool,
    model: &str,
    prompt: &str,
    embedding: &[f32],
    response_json: &str,
) -> Result<(), SemCacheError> {
    let conn = pool.get().map_err(SemCacheError::from)?;
    let blob = embedding_to_bytes(embedding);
    conn.execute(
        "INSERT INTO fuzzy_payloads (model, prompt_text, embedding, response_json)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![model, prompt, blob, response_json],
    )?;
    Ok(())
}

/// Prunes expired cache records older than max_age_days.
///
/// CRITICAL FIX (Flaw #9 - TTL & Eviction):
/// Prevents boundless SQLite growth and eliminates stale model responses.
pub fn prune_expired_records(pool: &DbPool, max_age_days: i64) -> Result<usize, SemCacheError> {
    let conn = pool.get().map_err(SemCacheError::from)?;
    let modifier = format!("-{} days", max_age_days);

    let deleted_exact = conn.execute(
        "DELETE FROM exact_cache WHERE created_at < datetime('now', ?1)",
        [&modifier],
    )?;
    let deleted_fuzzy = conn.execute(
        "DELETE FROM fuzzy_payloads WHERE created_at < datetime('now', ?1)",
        [&modifier],
    )?;

    let total_deleted = deleted_exact + deleted_fuzzy;

    // Execute WAL checkpoint to reclaim pages and truncate log file to avoid unbound growth
    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");

    Ok(total_deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_pool_and_exact_cache_roundtrip() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("semcache_test_exact_{}.db", std::process::id()));
        let db_path_str = db_path.to_string_lossy().to_string();

        let pool = init_db_pool(&db_path_str).expect("init db pool");
        let hash = [7u8; 32];
        let model = "gpt-4o";
        let req_json = r#"{"messages":[{"role":"user","content":"test"}]}"#;
        let resp_json = r#"{"choices":[{"message":{"content":"response"}}]}"#;

        let miss = get_exact_cache(&pool, &hash).expect("get exact cache miss");
        assert!(miss.is_none());

        insert_exact_cache(&pool, &hash, model, req_json, resp_json).expect("insert exact cache");
        let hit = get_exact_cache(&pool, &hash).expect("get exact cache hit");
        assert_eq!(hit.as_deref(), Some(resp_json));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn test_fuzzy_vector_cache_similarity_match() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("semcache_test_fuzzy_{}.db", std::process::id()));
        let db_path_str = db_path.to_string_lossy().to_string();

        let pool = init_db_pool(&db_path_str).expect("init db pool");
        let model = "gpt-4o";
        let prompt = "Explain quantum computing in simple terms";
        let embedding = vec![0.1, 0.2, 0.3, 0.4];
        let resp_json = r#"{"choices":[{"message":{"content":"quantum explanation"}}]}"#;

        insert_fuzzy_cache(&pool, model, prompt, &embedding, resp_json).expect("insert fuzzy");

        // Query with identical vector (similarity = 1.0)
        let exact_hit = find_fuzzy_match(&pool, model, &embedding, 0.92).expect("find fuzzy");
        assert_eq!(exact_hit.as_deref(), Some(resp_json));

        // Query with slightly altered vector (high similarity ~0.99)
        let similar_embedding = vec![0.101, 0.201, 0.301, 0.401];
        let fuzzy_hit = find_fuzzy_match(&pool, model, &similar_embedding, 0.92).expect("find fuzzy");
        assert_eq!(fuzzy_hit.as_deref(), Some(resp_json));

        // Query with orthogonal vector (similarity = 0.0) -> Miss
        let orthogonal = vec![-0.4, -0.3, 0.2, 0.1];
        let miss = find_fuzzy_match(&pool, model, &orthogonal, 0.92).expect("find fuzzy");
        assert!(miss.is_none());

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn test_ttl_pruning_execution() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("semcache_test_ttl_{}.db", std::process::id()));
        let db_path_str = db_path.to_string_lossy().to_string();

        let pool = init_db_pool(&db_path_str).expect("init db pool");
        let deleted = prune_expired_records(&pool, 30).expect("prune expired");
        // Fresh DB has 0 expired records
        assert_eq!(deleted, 0);

        let _ = std::fs::remove_file(db_path);
    }
}
