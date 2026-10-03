use crate::error::SemCacheError;
use serde_json::Value;

// Only strip non-generative, client-side tracking metadata.
const NON_GENERATIVE_VOLATILE_KEYS: &[&str] = &[
    "user",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    #[default]
    OpenAi,
    Ollama,
    Generic,
}

impl Provider {
    /// Infers the LLM provider from an optional request header (e.g. `x-semcache-provider`)
    /// or from the target upstream URL.
    pub fn from_hint(header_val: Option<&str>, upstream_url: &str) -> Self {
        if let Some(h) = header_val {
            match h.to_ascii_lowercase().trim() {
                "openai" => return Provider::OpenAi,
                "ollama" => return Provider::Ollama,
                "generic" | "raw" => return Provider::Generic,
                _ => {}
            }
        }
        if upstream_url.contains("11434") || upstream_url.to_ascii_lowercase().contains("ollama") {
            Provider::Ollama
        } else {
            Provider::OpenAi
        }
    }
}

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
/// CRITICAL FIX (The Ollama Trap & Default-Key Hash Divergence):
/// Default parameter normalization is strictly PROVIDER-AWARE:
/// - OpenAI: Defaults to `temperature: 1.0`, `top_p: 1.0`, `presence_penalty: 0.0`, `frequency_penalty: 0.0`.
/// - Ollama: Defaults to `temperature: 0.8`, `top_p: 0.9`. An explicit `temperature: 1.0` is NOT default!
/// - Generic: No defaults are stripped; exact parameters are preserved verbatim.
///
/// CRITICAL FIX (Flaw #3 - Syntax Preservation):
/// Preserves code blocks, Python indentation, YAML spacing, and Markdown line breaks verbatim.
pub fn canonicalize_and_hash(
    payload_bytes: &[u8],
    auth_header: Option<&str>,
    provider: Provider,
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

        // CRITICAL FIX: Provider-aware default normalization
        normalize_provider_defaults(map, provider);

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

/// Normalizes parameters matching the specified provider's official API defaults.
fn normalize_provider_defaults(map: &mut serde_json::Map<String, Value>, provider: Provider) {
    match provider {
        Provider::OpenAi => {
            // OpenAI Defaults: temperature=1.0, top_p=1.0, penalties=0.0
            if let Some(temp) = map.get("temperature").and_then(|v| v.as_f64()) {
                if (temp - 1.0).abs() < f64::EPSILON {
                    map.remove("temperature");
                }
            }

            if let Some(top_p) = map.get("top_p").and_then(|v| v.as_f64()) {
                if (top_p - 1.0).abs() < f64::EPSILON {
                    map.remove("top_p");
                }
            }

            if let Some(pp) = map.get("presence_penalty").and_then(|v| v.as_f64()) {
                if pp.abs() < f64::EPSILON {
                    map.remove("presence_penalty");
                }
            }

            if let Some(fp) = map.get("frequency_penalty").and_then(|v| v.as_f64()) {
                if fp.abs() < f64::EPSILON {
                    map.remove("frequency_penalty");
                }
            }
        }
        Provider::Ollama => {
            // Ollama Defaults: temperature=0.8, top_p=0.9
            // CRITICAL: In Ollama, temperature: 1.0 is NON-DEFAULT.
            // Only temperature: 0.8 is removed to match omitted parameters.
            if let Some(temp) = map.get("temperature").and_then(|v| v.as_f64()) {
                if (temp - 0.8).abs() < f64::EPSILON {
                    map.remove("temperature");
                }
            }

            if let Some(top_p) = map.get("top_p").and_then(|v| v.as_f64()) {
                if (top_p - 0.9).abs() < f64::EPSILON {
                    map.remove("top_p");
                }
            }
        }
        Provider::Generic => {
            // Generic / Raw: Never strip or mutate specified parameters
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openai_default_key_hash_convergence() {
        // OpenAI: Request without temperature vs Request with default temperature 1.0
        let json_omitted = br#"{"model":"gpt-4o","prompt":"A"}"#;
        let json_explicit_default = br#"{"model":"gpt-4o","prompt":"A","temperature":1.0,"top_p":1.0,"presence_penalty":0.0}"#;

        let res_omitted = canonicalize_and_hash(json_omitted, Some("Bearer key"), Provider::OpenAi).unwrap();
        let res_explicit = canonicalize_and_hash(json_explicit_default, Some("Bearer key"), Provider::OpenAi).unwrap();

        assert_eq!(
            res_omitted.hash, res_explicit.hash,
            "OpenAI default parameters must converge to the identical cache key"
        );
    }

    #[test]
    fn test_ollama_provider_default_normalization() {
        // In Ollama, default temperature is 0.8, NOT 1.0!
        let json_omitted = br#"{"model":"llama3","prompt":"A"}"#;
        let json_default_ollama = br#"{"model":"llama3","prompt":"A","temperature":0.8}"#;
        let json_explicit_one = br#"{"model":"llama3","prompt":"A","temperature":1.0}"#;

        let res_omitted = canonicalize_and_hash(json_omitted, None, Provider::Ollama).unwrap();
        let res_default = canonicalize_and_hash(json_default_ollama, None, Provider::Ollama).unwrap();
        let res_explicit = canonicalize_and_hash(json_explicit_one, None, Provider::Ollama).unwrap();

        assert_eq!(
            res_omitted.hash, res_default.hash,
            "Ollama default temperature (0.8) must converge with omitted temperature"
        );
        assert_ne!(
            res_omitted.hash, res_explicit.hash,
            "Ollama explicit temperature 1.0 is non-default and MUST NOT hash to omitted bucket"
        );
    }

    #[test]
    fn test_non_default_temperature_generates_distinct_hash() {
        let json_default = br#"{"model":"gpt-4o","prompt":"A","temperature":1.0}"#;
        let json_deterministic = br#"{"model":"gpt-4o","prompt":"A","temperature":0.0}"#;

        let res_default = canonicalize_and_hash(json_default, Some("Bearer key"), Provider::OpenAi).unwrap();
        let res_deterministic = canonicalize_and_hash(json_deterministic, Some("Bearer key"), Provider::OpenAi).unwrap();

        assert_ne!(
            res_default.hash, res_deterministic.hash,
            "Non-default temperature must generate a different cache hash"
        );
    }

    #[test]
    fn test_differing_key_orders_yield_identical_hash() {
        let json_a = br#"{"model":"gpt-4o","prompt":"hello","temperature":0.0}"#;
        let json_b = br#"{"temperature":0.0,"prompt":"hello","model":"gpt-4o"}"#;

        let res_a = canonicalize_and_hash(json_a, Some("Bearer key"), Provider::OpenAi).unwrap();
        let res_b = canonicalize_and_hash(json_b, Some("Bearer key"), Provider::OpenAi).unwrap();

        assert_eq!(
            res_a.hash, res_b.hash,
            "Key ordering must not change canonical BLAKE3 hash"
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

        let res = canonicalize_and_hash(&bytes, Some("Bearer key-1"), Provider::OpenAi).unwrap();
        let content = res.canonical_value["messages"][0]["content"].as_str().unwrap();

        assert_eq!(content, "def hello():\n    # greeting\n\n    print('world')");
    }

    #[test]
    fn test_multi_tenant_auth_isolation() {
        let payload = br#"{"model":"gpt-4o","prompt":"Hello world"}"#;

        let res_tenant_a = canonicalize_and_hash(payload, Some("Bearer sk-tenant-a"), Provider::OpenAi).unwrap();
        let res_tenant_b = canonicalize_and_hash(payload, Some("Bearer sk-tenant-b"), Provider::OpenAi).unwrap();

        assert_ne!(res_tenant_a.hash, res_tenant_b.hash);
    }
}
