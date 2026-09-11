# RocksDB admission probe

Use a fresh disposable LAB_ROOT containing a results/ directory. Run probe.py with
Python 3.13 and rocksdict 0.3.29. It creates two isolated RocksDB stores and kills
the writer immediately before write or after a synchronized WAL-backed batch.
The five keys model identity, body, pending record, producer receipt and sequence.
It reopens and verifies all absent/all present and second-owner refusal.

The recorded Tux process group was limited to 1 CPU, 256 MiB, zero swap and 50
tasks. Runtime 317 ms, CPU 130 ms, memory peak 30.9 MiB. This is Python-binding
and process-death evidence, not validation of the eventual Rust adapter or disk
failure, ENOSPC, firmware cache, system power loss or multi-host fencing.
