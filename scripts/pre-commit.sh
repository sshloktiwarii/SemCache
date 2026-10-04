#!/usr/bin/env bash
# SemCache Pre-Commit Quality & Invariant Enforcer
# Guarantees zero regressions, strict clippy compliance, and secrets cleanliness before commit.

set -e

echo "==> Running Cargo Test Suite..."
cargo test

echo "==> Running Cargo Clippy (Deny Warnings)..."
cargo clippy --all-targets -- -D warnings

echo "==> Running Secrets & Key Leak Audit..."
if git grep -E "\bsk-[a-zA-Z0-9_-]+" | grep -v -E "sk-(test|tenant|mock|alice|bob|shared|sentinel|soak)"; then
    echo "ERROR: Potential unredacted production secret detected in working tree!"
    exit 1
fi

echo "==> Pre-commit verification passed successfully!"
