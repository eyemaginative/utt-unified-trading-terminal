import base64
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization

priv = Ed25519PrivateKey.generate()

# Raw forms (32-byte public key; 32-byte private seed)
priv_raw = priv.private_bytes(
    encoding=serialization.Encoding.Raw,
    format=serialization.PrivateFormat.Raw,
    encryption_algorithm=serialization.NoEncryption(),
)
pub_raw = priv.public_key().public_bytes(
    encoding=serialization.Encoding.Raw,
    format=serialization.PublicFormat.Raw,
)

pub_b64 = base64.b64encode(pub_raw).decode("ascii")
priv_b64 = base64.b64encode(priv_raw).decode("ascii")

print("\n=== ROBINHOOD PUBLIC KEY (paste into portal) ===\n")
print(pub_b64)
print("\n=== ROBINHOOD PRIVATE KEY (keep secret; for .env) ===\n")
print(priv_b64)

with open("robinhood_public_key.b64.txt", "w", encoding="utf-8") as f:
    f.write(pub_b64 + "\n")
with open("robinhood_private_key.b64.txt", "w", encoding="utf-8") as f:
    f.write(priv_b64 + "\n")

print("\nSaved: robinhood_public_key.b64.txt and robinhood_private_key.b64.txt\n")
