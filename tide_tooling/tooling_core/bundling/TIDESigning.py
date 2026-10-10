"""Optional Ed25519 authentication for canonical TIDE Bundles."""

from importlib import import_module
from pathlib import Path


SIGNATURE_KIND = "tide_bundle_signature"
SIGNATURE_CODEC_VERSION = 1
SIGNATURE_ALGORITHM = "ed25519"


def _require_cryptography():
    """Load the optional signing dependency only when signing is used."""
    try:
        exceptions = import_module("cryptography.exceptions")
        serialization = import_module(
            "cryptography.hazmat.primitives.serialization"
        )
        ed25519 = import_module(
            "cryptography.hazmat.primitives.asymmetric.ed25519"
        )
    except ImportError as error:
        raise ImportError(
            "TIDE Bundle signing requires the optional dependency: "
            "pip install tide-tooling[signing]"
        ) from error

    return exceptions, serialization, ed25519


class TIDESigningKeys:
    """One generated Ed25519 private/public key pair."""

    def __init__(self, private_key, public_key):
        self.private_key = private_key
        self.public_key = public_key

    def save_private(self, path):
        """Save the private key as unencrypted PKCS8 PEM."""
        _, serialization, _ = _require_cryptography()
        path = Path(path)
        path.write_bytes(
            self.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        return path

    def save_public(self, path):
        """Save the public key as SubjectPublicKeyInfo PEM."""
        _, serialization, _ = _require_cryptography()
        path = Path(path)
        path.write_bytes(
            self.public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )
        return path


def generate_signing_keys():
    """Generate one explicit Ed25519 signing identity."""
    _, _, ed25519 = _require_cryptography()
    private_key = ed25519.Ed25519PrivateKey.generate()
    return TIDESigningKeys(private_key, private_key.public_key())


def validate_signature_record(record):
    """Validate the dependency-independent persisted signature schema."""
    if record is None:
        return
    if not isinstance(record, dict):
        raise TypeError("TIDE Bundle signature must be a dict")

    expected = {
        "kind",
        "codec_version",
        "algorithm",
        "signature",
    }
    if set(record) != expected:
        raise ValueError("Invalid TIDE Bundle signature record")
    if record["kind"] != SIGNATURE_KIND:
        raise ValueError("Unsupported TIDE Bundle signature kind")
    if record["codec_version"] != SIGNATURE_CODEC_VERSION:
        raise ValueError("Unsupported TIDE Bundle signature codec version")
    if record["algorithm"] != SIGNATURE_ALGORITHM:
        raise ValueError("Unsupported TIDE Bundle signature algorithm")
    if not isinstance(record["signature"], bytes):
        raise TypeError("TIDE Bundle signature bytes must be bytes")


def sign_bytes(content, private_key):
    """Sign canonical Bundle content with an Ed25519 private key."""
    _, _, ed25519 = _require_cryptography()
    if not isinstance(private_key, ed25519.Ed25519PrivateKey):
        raise TypeError("sign must be an Ed25519 private key")
    return {
        "kind": SIGNATURE_KIND,
        "codec_version": SIGNATURE_CODEC_VERSION,
        "algorithm": SIGNATURE_ALGORITHM,
        "signature": private_key.sign(content),
    }


def verify_bytes(content, record, public_key):
    """Verify canonical Bundle content with an Ed25519 public key."""
    exceptions, _, ed25519 = _require_cryptography()
    validate_signature_record(record)
    if record is None:
        raise ValueError("TIDE Bundle is not signed")
    if not isinstance(public_key, ed25519.Ed25519PublicKey):
        raise TypeError("verify must be an Ed25519 public key")

    try:
        public_key.verify(record["signature"], content)
    except exceptions.InvalidSignature as error:
        raise ValueError("TIDE Bundle signature verification failed") from error
    return True
