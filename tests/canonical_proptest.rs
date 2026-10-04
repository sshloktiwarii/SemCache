use proptest::prelude::*;
use semcache::canonical::{canonicalize_and_hash, Provider};
use serde_json::{json, Value};

proptest! {
    /// Invariant 1: Random key insertion order in JSON objects must produce the exact same canonical digest.
    #[test]
    fn test_proptest_key_order_invariance(
        k1 in "[a-z]{1,8}",
        v1 in "[a-z0-9]{1,16}",
        k2 in "[a-z]{1,8}",
        v2 in "[a-z0-9]{1,16}",
        k3 in "[a-z]{1,8}",
        v3 in "[a-z0-9]{1,16}",
        prompt in "[A-Za-z0-9 ]{1,32}"
    ) {
        // Construct payload with order A
        let json_a = format!(
            r#"{{"model":"gpt-4o","prompt":"{}","{}":"{}","{}":"{}","{}":"{}"}}"#,
            prompt, k1, v1, k2, v2, k3, v3
        );

        // Construct payload with reverse order B
        let json_b = format!(
            r#"{{"{}":"{}","{}":"{}","{}":"{}","prompt":"{}","model":"gpt-4o"}}"#,
            k3, v3, k2, v2, k1, v1, prompt
        );

        if let (Ok(val_a), Ok(val_b)) = (
            serde_json::from_str::<Value>(&json_a),
            serde_json::from_str::<Value>(&json_b)
        ) {
            // Verify semantic equivalence
            if val_a == val_b {
                let res_a = canonicalize_and_hash(json_a.as_bytes(), Some("tenant-1"), Provider::OpenAi).unwrap();
                let res_b = canonicalize_and_hash(json_b.as_bytes(), Some("tenant-1"), Provider::OpenAi).unwrap();
                prop_assert_eq!(res_a.hash, res_b.hash, "BLAKE3 digest must be identical regardless of key order");
            }
        }
    }

    /// Invariant 2: Random outer whitespace around prompt string must produce identical canonical digest.
    #[test]
    fn test_proptest_whitespace_invariance(
        leading_spaces in " {0,10}",
        trailing_spaces in " {0,10}",
        prompt in "[A-Za-z0-9]{1,32}"
    ) {
        let clean_json = json!({
            "model": "gpt-4o",
            "prompt": prompt
        });

        let padded_json = json!({
            "model": "gpt-4o",
            "prompt": format!("{}{}{}", leading_spaces, prompt, trailing_spaces)
        });

        let clean_bytes = serde_json::to_vec(&clean_json).unwrap();
        let padded_bytes = serde_json::to_vec(&padded_json).unwrap();

        let res_clean = canonicalize_and_hash(&clean_bytes, Some("tenant-1"), Provider::OpenAi).unwrap();
        let res_padded = canonicalize_and_hash(&padded_bytes, Some("tenant-1"), Provider::OpenAi).unwrap();

        prop_assert_eq!(res_clean.hash, res_padded.hash, "Prompt outer whitespace must be normalized");
    }

    /// Invariant 3: Fuzz safety: canonicalize_and_hash must never panic on arbitrary byte inputs.
    #[test]
    fn test_proptest_arbitrary_bytes_no_panic(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let _ = canonicalize_and_hash(&bytes, Some("fuzz-tenant"), Provider::OpenAi);
    }
}
