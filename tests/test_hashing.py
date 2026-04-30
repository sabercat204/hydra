"""Tests for hashing utilities."""

from hydra.utils.hashing import compute_raw_hash


class TestComputeRawHash:
    """compute_raw_hash returns 16-char xxhash64 hex digest."""

    def test_returns_16_chars(self):
        h = compute_raw_hash(b"hello world")
        assert len(h) == 16

    def test_hex_string(self):
        h = compute_raw_hash(b"test")
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self):
        a = compute_raw_hash(b"same input")
        b = compute_raw_hash(b"same input")
        assert a == b

    def test_different_inputs_different_hashes(self):
        a = compute_raw_hash(b"input_a")
        b = compute_raw_hash(b"input_b")
        assert a != b

    def test_empty_bytes(self):
        h = compute_raw_hash(b"")
        assert len(h) == 16
