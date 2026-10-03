use crate::error::SemCacheError;
use serde_json::Value;

const VOLATILE_KEYS: &[&str] = &[
    "temperature",
    "top_p",
    "presence_penalty",
    "frequency_penalty",
    "user",
    "seed",
    "logit_bias",
];

/// Canonicalizes an incoming JSON payload by stripping volatile hyperparameters,
/// validating stream constraints, normalizing whitespace/newlines, and deterministically
/// hashing the key-sorted JSON with BLAKE3.
pub fn canonicalize_and_hash(payload_bytes: &[u8]) -> Result<([u8; 32], Value), SemCacheError> {
    let mut value: Value = serde_json::from_slice(payload_bytes)?;

    if let Value::Object(ref mut map) = value {
        // Validate streaming constraint
        if let Some(stream_val) = map.get("stream") {
            if stream_val.as_bool() == Some(true) {
                return Err(SemCacheError::StreamingNotSupported);
            }
            // Strip stream: false so presence/absence of false doesn't alter cache hash
            map.remove("stream");
        }

        // Strip non-semantic, volatile parameters
        for key in VOLATILE_KEYS {
            map.remove(*key);
        }

        // Normalize chat messages content
        if let Some(Value::Array(messages)) = map.get_mut("messages") {
            for msg in messages {
                if let Value::Object(msg_map) = msg {
                    if let Some(Value::String(content)) = msg_map.get_mut("content") {
                        *content = normalize_text(content);
                    }
                }
            }
        }

        // Normalize direct prompt string if present (completions / embeddings)
        if let Some(Value::String(prompt)) = map.get_mut("prompt") {
            *prompt = normalize_text(prompt);
        }
    }

    // Deterministic serialization: serde_json::Value's Map is backed by BTreeMap by default,
    // which guarantees lexicographically sorted keys.
    let canonical_bytes = serde_json::to_vec(&value)?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(&canonical_bytes);
    let hash = *hasher.finalize().as_bytes();

    Ok((hash, value))
}

/// Trims leading and trailing whitespace from the text and each line,
/// and collapses multiple consecutive newlines into a single newline.
fn normalize_text(text: &str) -> String {
    let trimmed = text.trim();
    let mut lines = Vec::new();

    for raw_line in trimmed.lines() {
        let line = raw_line.trim();
        if !line.is_empty() {
            lines.push(line);
        }
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical_hash_for_differing_key_orders_and_volatiles() {
        let json_a = br#"{"prompt":"A", "temperature":0.9}"#;
        let json_b = br#"{"temperature":0.1, "prompt":"A"}"#;

        let (hash_a, val_a) = canonicalize_and_hash(json_a).expect("canonicalize json_a");
        let (hash_b, val_b) = canonicalize_and_hash(json_b).expect("canonicalize json_b");

        assert_eq!(hash_a, hash_b, "Hashes must match despite key order and temperature variance");
        assert_eq!(val_a, val_b);
    }

    #[test]
    fn test_streaming_rejected() {
        let json_stream = br#"{"model":"gpt-4o", "stream": true, "messages":[{"role":"user","content":"hello"}]}"#;
        let err = canonicalize_and_hash(json_stream).expect_err("Must reject stream: true");
        match err {
            SemCacheError::StreamingNotSupported => (),
            other => panic!("Expected StreamingNotSupported, got {:?}", other),
        }
    }

    #[test]
    fn test_content_normalization() {
        let json_untrimmed = br#"{"messages":[{"role":"user","content":"  hello world \n\n\n test  "}]}"#;
        let json_clean = br#"{"messages":[{"role":"user","content":"hello world\ntest"}]}"#;

        let (hash_a, _) = canonicalize_and_hash(json_untrimmed).expect("canonicalize untrimmed");
        let (hash_b, _) = canonicalize_and_hash(json_clean).expect("canonicalize clean");

        assert_eq!(hash_a, hash_b, "Whitespace and newline collapsing must yield identical hash");
    }
}
