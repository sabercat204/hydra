"""Fast indicator lookup — Capability 6.

Indicator_Lookup_Cache (Redis + msgpack), classifier, normalizer, single-flight
lock, and the cold-path payload assembler (Design §2.4, §3.7).
"""
