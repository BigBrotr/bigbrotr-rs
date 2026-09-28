#!/usr/bin/env python3
"""Deterministic PUBLIC TEST-ONLY TLS identities; never deployment credentials.

Requires cryptography 45.0.7. Fixed keys and serials make the independent
certificate oracle reproducible. Regeneration is not part of product startup.
"""
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

ROOT = Path(__file__).resolve().parent


def key(label):
    return Ed25519PrivateKey.from_private_bytes(
        hashlib.sha256(("BigBrotr public TLS fixture: " + label).encode()).digest()
    )


def certificate(label, issuer, serial, ca):
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, label)])
    issuer_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer)])
    builder = (x509.CertificateBuilder().subject_name(subject).issuer_name(issuer_name)
               .public_key(key(label).public_key()).serial_number(serial)
               .not_valid_before(datetime(2026, 1, 1, tzinfo=timezone.utc))
               .not_valid_after(datetime(2036, 1, 1, tzinfo=timezone.utc))
               .add_extension(x509.BasicConstraints(ca=ca, path_length=0 if ca else None), True)
               .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
                   key_encipherment=False, data_encipherment=False, key_agreement=False,
                   key_cert_sign=ca, crl_sign=ca, encipher_only=None, decipher_only=None), True))
    if not ca:
        builder = builder.add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        builder = builder.add_extension(x509.SubjectAlternativeName([
            x509.DNSName("fixture.example"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False)
    return builder.sign(key(issuer), None)


if __name__ == "__main__":
    certificates = {
        "ca.der": certificate("BigBrotr TEST CA", "BigBrotr TEST CA", 1, True),
        "wrong-ca.der": certificate("BigBrotr OTHER TEST CA", "BigBrotr OTHER TEST CA", 2, True),
        "server.der": certificate("fixture.example", "BigBrotr TEST CA", 3, False),
    }
    for name, cert in certificates.items():
        (ROOT / name).write_bytes(cert.public_bytes(serialization.Encoding.DER))
    (ROOT / "server-test-key.der").write_bytes(key("fixture.example").private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    (ROOT / "manifest.json").write_text(json.dumps({
        "purpose": "Public, deterministic test-only TLS identities; no production secrets",
        "server_names": ["fixture.example", "127.0.0.1"],
        "validity_utc": ["2026-01-01", "2036-01-01"],
        "sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(ROOT.glob("*.der"))},
    }, indent=2) + "\n")
