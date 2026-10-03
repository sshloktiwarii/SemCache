use crate::error::SemCacheError;
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
             PRAGMA temp_store = MEMORY;",
        )
    });

    let pool = Pool::builder()
        .max_size(16)
        .build(manager)
        .map_err(SemCacheError::PoolError)?;

    // Run schema migrations on a connection from the pool
    let conn = pool.get().map_err(SemCacheError::PoolError)?;

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
            prompt_text TEXT NOT NULL,
            response_json TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );",
    )?;

    // Attempt to initialize sqlite-vec virtual table; gracefully simulate if extension not linked in runtime
    if let Err(e) = conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS fuzzy_cache USING vec0(embedding float[1536]);",
        [],
    ) {
        tracing::warn!(
            "sqlite-vec vec0 extension not present ({}); initializing simulated fuzzy_cache table for MVP",
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
    let conn = pool.get().map_err(SemCacheError::PoolError)?;
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
    let conn = pool.get().map_err(SemCacheError::PoolError)?;
    conn.execute(
        "INSERT OR REPLACE INTO exact_cache (canonical_hash, model, request_json, response_json)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![hash.as_slice(), model, request_json, response_json],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_pool_and_exact_cache_roundtrip() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("semcache_test_{}.db", std::process::id()));
        let db_path_str = db_path.to_string_lossy().to_string();

        let pool = init_db_pool(&db_path_str).expect("init db pool");
        let hash = [7u8; 32];
        let model = "gpt-4o";
        let req_json = r#"{"messages":[{"role":"user","content":"test"}]}"#;
        let resp_json = r#"{"choices":[{"message":{"content":"response"}}]}"#;

        // Verify miss on empty DB
        let miss = get_exact_cache(&pool, &hash).expect("get exact cache miss");
        assert!(miss.is_none());

        // Insert and verify hit
        insert_exact_cache(&pool, &hash, model, req_json, resp_json).expect("insert exact cache");
        let hit = get_exact_cache(&pool, &hash).expect("get exact cache hit");
        assert_eq!(hit.as_deref(), Some(resp_json));

        // Cleanup test db
        let _ = std::fs::remove_file(db_path);
    }
}
