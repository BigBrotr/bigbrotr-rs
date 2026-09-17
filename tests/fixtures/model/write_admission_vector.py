"""Explicit two-record byte fixture; no production or research codec imports."""
import json
from pathlib import Path
import subprocess

dataset = "01" * 16
first_digest, second_digest = "11" * 32, "22" * 32
# array6: dataset bytes16, shard0, epoch1, first23, last24, array2 records.
# Each record is array2(source array4, digest bytes32). 24 requires head1818.
encoded = (b"bigbrotr/admission-batch/1\0".hex()
           + "8650" + dataset + "000117181882"
           + "828450" + dataset + "0001175820" + first_digest
           + "828450" + dataset + "000118185820" + second_digest)
digest = subprocess.check_output(["openssl", "dgst", "-sha256", "-binary"],
                                 input=bytes.fromhex(encoded)).hex()
fixture = {"format": "bigbrotr-admission-vector/1",
           "authoring": "Literal CBOR heads and field order; SHA-256 by system OpenSSL. No signature/commit claim.",
           "dataset_id": dataset, "shard_id": 0, "owner_epoch": "1",
           "records": [{"sequence": "23", "payload_digest": first_digest},
                       {"sequence": "24", "payload_digest": second_digest}],
           "preimage_hex": encoded, "sha256": digest}
Path(__file__).with_name("admission-two-records.json").write_text(json.dumps(fixture, indent=2) + "\n")
