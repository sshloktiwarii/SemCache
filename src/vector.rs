#![allow(dead_code)]

/// Computes the cosine similarity between two float vectors:
/// (A · B) / (||A|| * ||B||)
/// Returns a value between -1.0 and 1.0.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    if norm_a <= 0.0 || norm_b <= 0.0 {
        return 0.0;
    }

    dot / (norm_a.sqrt() * norm_b.sqrt())
}

/// Computes Cosine Distance: 1.0 - Cosine Similarity
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

/// Serializes a slice of 32-bit floats into a byte vector for SQLite BLOB storage.
pub fn embedding_to_bytes(embedding: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(embedding.len() * 4);
    for &val in embedding {
        bytes.extend_from_slice(&val.to_le_bytes());
    }
    bytes
}

/// Deserializes a byte slice from SQLite BLOB storage back into a Vec<f32>.
pub fn bytes_to_embedding(bytes: &[u8]) -> Vec<f32> {
    if !bytes.len().is_multiple_of(4) {
        return Vec::new();
    }
    let mut embedding = Vec::with_capacity(bytes.len() / 4);
    for chunk in bytes.as_chunks::<4>().0 {
        let val = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        embedding.push(val);
    }
    embedding
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity_identical_vectors() {
        let v1 = vec![1.0, 2.0, 3.0, 4.0];
        let v2 = vec![1.0, 2.0, 3.0, 4.0];
        let sim = cosine_similarity(&v1, &v2);
        assert!((sim - 1.0).abs() < 1e-5);
        assert!((cosine_distance(&v1, &v2) - 0.0).abs() < 1e-5);
    }

    #[test]
    fn test_cosine_similarity_orthogonal_vectors() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![0.0, 1.0];
        let sim = cosine_similarity(&v1, &v2);
        assert!(sim.abs() < 1e-5);
        assert!((cosine_distance(&v1, &v2) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_embedding_byte_roundtrip() {
        let original = vec![0.123, -0.456, 0.789, 1.0];
        let bytes = embedding_to_bytes(&original);
        let restored = bytes_to_embedding(&bytes);
        assert_eq!(original, restored);
    }
}
