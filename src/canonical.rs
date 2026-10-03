use crate::error::SemCacheError;
use serde_json::Value;

// Only strip non-generative, client-side tracking metadata.
//
// CRITICAL FIX (Flaw #7): Do NOT strip temperature, top_p, seed, or repetition penalties.
// A prompt at temperature 0.0 (deterministic) must generate a completely different
// cache key from temperature 1.0 (creative).
const NON_GENERATIVE_VOLATILE_KEYS: &[&str] = &[
    "user",
];

pub struct CanonicalResult {
    pub hash: [u8; 32],
    pub canonical_value: Value,
    pub is_streaming: bool,
}

/// Canonicalizes an incoming JSON payload.
///
/// CRITICAL FIX (Flaw #4 - Multi-Tenant Isolation):
/// Incorporates `auth_header` into the BLAKE3 digest. Tenant A (API Key A)
/// and Tenant B (API Key B or unauthenticated) will NEVER share cache entries.
///
/// CRITICAL FIX (Flaw #3 - Syntax Preservation):
/// Does NOT collapse newlines or strip internal whitespace. Preserves code blocks,
/// Python indentation, YAML spacing, and Markdown line breaks verbatim.
pub fn canonicalize_and_hash(
    payload_bytes: &[u8],
    auth_header: Option<&str>,
) -> Result<CanonicalResult, SemCacheError> {
    let mut value: Value = serde_json::from_slice(payload_bytes)?;
    let mut is_streaming = false;

    if let Value::Object(ref mut map) = value {
        if let Some(stream_val) = map.get("stream") {
            if stream_val.as_bool() == Some(true) {
                is_streaming = true;
            }
            // Strip stream: false so presence/absence of default false doesn't alter cache hash
            if !is_streaming {
                map.remove("stream");
            }
        }

        // Strip non-generative client tracking fields (e.g. "user")
        for key in NON_GENERATIVE_VOLATILE_KEYS {
            map.remove(*key);
        }

        // CRITICAL FIX (Flaw #3): Only trim outermost whitespace of prompt/content strings.
        // Never collapse internal newlines (\n\n) or strip line indentation.
        if let Some(Value::Array(messages)) = map.get_mut("messages") {
            for msg in messages {
                if let Value::Object(msg_map) = msg {
                    if let Some(Value::String(content)) = msg_map.get_mut("content") {
                        let trimmed = content.trim().to_string();
                        *content = trimmed;
                    }
                }
            }
        }

        if let Some(Value::String(prompt)) = map.get_mut("prompt") {
            let trimmed = prompt.trim().to_string();
            *prompt = trimmed;
        }
    }

    // Deterministic serialization: serde_json::Value's Map is backed by BTreeMap by default,
    // which guarantees lexicographically sorted keys.
    let canonical_bytes = serde_json::to_vec(&value)?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(&canonical_bytes);

    // Inject tenant isolation token into BLAKE3 hash
    hasher.update(b"|auth_tenant:");
    if let Some(auth) = auth_header {
        hasher.update(auth.trim().as_bytes());
    } else {
        hasher.update(b"anonymous");
    }

    let hash = *hasher.finalize().as_bytes();

    Ok(CanonicalResult {
        hash,
        canonical_value: value,
        is_streaming,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_syntax_preservation_code_and_newlines() {
        // Python code with crucial indentation and empty lines
        let python_prompt = "def hello():\n    # greeting\n\n    print('world')\n";
        let payload = serde_json::json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": python_prompt}],
            "temperature": 0.0
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let res = canonicalize_and_hash(&bytes, Some("Bearer key-1")).unwrap();
        let content = res.canonical_value["messages"][0]["content"].as_str().unwrap();

        // Exact indentation and double newline must be preserved
        assert_eq!(content, "def hello():\n    # greeting\n\n    print('world')");
    }

    #[test]
    fn test_multi_tenant_auth_isolation() {
        let payload = br#"{"model":"gpt-4o","prompt":"Hello world"}"#;

        let res_tenant_a = canonicalize_and_hash(payload, Some("Bearer sk-tenant-a")).unwrap();
        let res_tenant_b = canonicalize_and_hash(payload, Some("Bearer sk-tenant-b")).unwrap();
        let res_anon = canonicalize_and_hash(payload, None).unwrap();

        assert_ne!(res_tenant_a.hash, res_tenant_b.hash, "Different API keys must produce different cache keys");
        assert_ne!(res_tenant_a.hash, res_anon.hash, "Authenticated request must not match anonymous request");
    }

    #[test]
    fn test_temperature_variance_changes_hash() {
        let payload_deterministic = br#"{"model":"gpt-4o","prompt":"Poem","temperature":0.0}"#;
        let payload_creative = br#"{"model":"gpt-4o","prompt":"Poem","temperature":1.0}"#;

        let res_det = canonicalize_and_hash(payload_deterministic, Some("Bearer key")).unwrap();
        let res_cre = canonicalize_and_hash(payload_creative, Some("Bearer key")).unwrap();

        assert_ne!(res_det.hash, res_cre.hash, "Different temperatures must produce distinct cache hashes");
    }

    #[test]
    fn test_differing_key_orders_yield_identical_hash() {
        let json_a = br#"{"model":"gpt-4o","prompt":"A","temperature":0.7}"#;
        let json_b = br#"{"temperature":0.7,"prompt":"A","model":"gpt-4o"}"#;

        let res_a = canonicalize_and_hash(json_a, Some("Bearer key")).unwrap();
        let res_b = canonicalize_and_hash(json_b, Some("Bearer key")).unwrap();

        assert_eq!(res_a.hash, res_b.hash, "Key order variance must be normalized away");
    }
}
