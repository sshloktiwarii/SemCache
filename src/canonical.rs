use crate::error::SemCacheError;
use serde_json::Value;

// Only strip non-generative, client-side tracking metadata.
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
/// Incorporates `auth_header` into the BLAKE3 digest. Tenant A and Tenant B
/// will NEVER share cache entries.
///
/// CRITICAL FIX (The Default-Key Hash Divergence):
/// Normalizes OpenAI official default parameters so that omitting a key
/// or passing its official default value (e.g. `temperature: 1.0`, `top_p: 1.0`,
/// `presence_penalty: 0.0`, `frequency_penalty: 0.0`) produces the identical
/// canonical JSON and BLAKE3 hash!
///
/// CRITICAL FIX (Flaw #3 - Syntax Preservation):
/// Preserves code blocks, Python indentation, YAML spacing, and Markdown line breaks verbatim.
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
            if !is_streaming {
                map.remove("stream");
            }
        }

        // Strip non-generative client tracking fields
        for key in NON_GENERATIVE_VOLATILE_KEYS {
            map.remove(*key);
        }

        // CRITICAL FIX: Normalize OpenAI API Defaults
        // If a parameter is explicitly set to its official default value,
        // remove it so it converges with requests that omitted the key.
        normalize_openai_defaults(map);

        // Preserve internal whitespace/newlines verbatim; only trim outermost edges
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

    let canonical_bytes = serde_json::to_vec(&value)?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(&canonical_bytes);

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

/// Normalizes parameters that match OpenAI's default API values.
fn normalize_openai_defaults(map: &mut serde_json::Map<String, Value>) {
    // OpenAI Chat Completions Defaults:
    // temperature: default 1.0
    if let Some(temp) = map.get("temperature").and_then(|v| v.as_f64()) {
        if (temp - 1.0).abs() < f64::EPSILON {
            map.remove("temperature");
        }
    }

    // top_p: default 1.0
    if let Some(top_p) = map.get("top_p").and_then(|v| v.as_f64()) {
        if (top_p - 1.0).abs() < f64::EPSILON {
            map.remove("top_p");
        }
    }

    // presence_penalty: default 0.0
    if let Some(pp) = map.get("presence_penalty").and_then(|v| v.as_f64()) {
        if pp.abs() < f64::EPSILON {
            map.remove("presence_penalty");
        }
    }

    // frequency_penalty: default 0.0
    if let Some(fp) = map.get("frequency_penalty").and_then(|v| v.as_f64()) {
        if fp.abs() < f64::EPSILON {
            map.remove("frequency_penalty");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_key_hash_convergence() {
        // Request without temperature vs Request with default temperature 1.0
        let json_omitted = br#"{"model":"gpt-4o","prompt":"A"}"#;
        let json_explicit_default = br#"{"model":"gpt-4o","prompt":"A","temperature":1.0,"top_p":1.0,"presence_penalty":0.0}"#;

        let res_omitted = canonicalize_and_hash(json_omitted, Some("Bearer key")).unwrap();
        let res_explicit = canonicalize_and_hash(json_explicit_default, Some("Bearer key")).unwrap();

        assert_eq!(
            res_omitted.hash, res_explicit.hash,
            "Default parameters must converge to the identical cache key"
        );
    }

    #[test]
    fn test_non_default_temperature_generates_distinct_hash() {
        let json_default = br#"{"model":"gpt-4o","prompt":"A","temperature":1.0}"#;
        let json_deterministic = br#"{"model":"gpt-4o","prompt":"A","temperature":0.0}"#;

        let res_default = canonicalize_and_hash(json_default, Some("Bearer key")).unwrap();
        let res_deterministic = canonicalize_and_hash(json_deterministic, Some("Bearer key")).unwrap();

        assert_ne!(
            res_default.hash, res_deterministic.hash,
            "Non-default temperature must generate a different cache hash"
        );
    }

    #[test]
    fn test_syntax_preservation_code_and_newlines() {
        let python_prompt = "def hello():\n    # greeting\n\n    print('world')\n";
        let payload = serde_json::json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": python_prompt}],
            "temperature": 0.0
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let res = canonicalize_and_hash(&bytes, Some("Bearer key-1")).unwrap();
        let content = res.canonical_value["messages"][0]["content"].as_str().unwrap();

        assert_eq!(content, "def hello():\n    # greeting\n\n    print('world')");
    }

    #[test]
    fn test_multi_tenant_auth_isolation() {
        let payload = br#"{"model":"gpt-4o","prompt":"Hello world"}"#;

        let res_tenant_a = canonicalize_and_hash(payload, Some("Bearer sk-tenant-a")).unwrap();
        let res_tenant_b = canonicalize_and_hash(payload, Some("Bearer sk-tenant-b")).unwrap();

        assert_ne!(res_tenant_a.hash, res_tenant_b.hash);
    }
}
