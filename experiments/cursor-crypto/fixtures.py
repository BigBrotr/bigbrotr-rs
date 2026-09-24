"""Independent public vectors; never import BigBrotr or the RustCrypto code."""
import hashlib
import json
from pathlib import Path

import nacl
import nacl._sodium
from nacl.bindings import crypto_aead_xchacha20poly1305_ietf_encrypt as encrypt


def vectors():
    key = bytes(range(0x80, 0xA0))
    nonce = bytes(range(0x40, 0x58))
    aad = bytes.fromhex("50515253c0c1c2c3c4c5c6c7")
    message = (b"Ladies and Gentlemen of the class of '99: "
               b"If I could offer you only one tip for the future, sunscreen would be it.")
    # draft-irtf-cfrg-xchacha-03 A.3.1. Independently freeze the published answer.
    expected = bytes.fromhex(
        "bd6d179d3e83d43b9576579493c0e939572a1700252bfaccbed2902c21396cbb"
        "731c7f1b0b4aa6440bf3a82f4eda7e39ae64c6708c54c216cb96b72e1213b452"
        "2f8c9ba40db5d945b11b69b982c1bb9e3f3fac2bc369488f76b2383565d3fff9"
        "21f9664c97637da9768812f615c68b13b52ec0875924c1c7987947deafd8780acf49"
    )
    assert encrypt(message, aad, nonce, key) == expected
    cases = [("draft-a3.1", key, nonce, aad, message, expected)]
    for ordinal, length in enumerate([0, 1, 15, 16, 17, 63, 64, 65, 255, 4096]):
        # Distinct fixed nonces are reproducible test inputs, never product RNG.
        n = bytes([ordinal]) + nonce[1:]
        a = b"BigBrotr public AEAD test\x00" + bytes([ordinal])
        msg = bytes((i * 17 + ordinal) % 256 for i in range(length))
        cases.append((f"length-{length}", key, n, a, msg, encrypt(msg, a, n, key)))
    return [dict(name=name, key=k.hex(), nonce=n.hex(), aad=a.hex(),
                 plaintext=m.hex(), ciphertext_and_tag=c.hex())
            for name, k, n, a, m, c in cases]


if __name__ == "__main__":
    value = {
        "format": "bigbrotr-cursor-crypto-vectors/1",
        "scope": "Public algorithm fixtures, not a product cursor format",
        "published_vector": "https://datatracker.ietf.org/doc/html/draft-irtf-cfrg-xchacha-03#appendix-A.3.1",
        "oracle": {"pynacl": nacl.__version__,
                   "sodium_module_sha256": hashlib.sha256(Path(nacl._sodium.__file__).read_bytes()).hexdigest()},
        "vectors": vectors(),
    }
    print(json.dumps(value, indent=2))
