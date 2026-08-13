class TIDEOpaqueValue:
    """Portable evidence for a TIDE value that cannot be rebuilt."""

    def __init__(
        self,
        type_name,
        representation,
        reason,
        module=None,
        qualname=None,
        value_hash=None,
        observed_mutable=False
    ):
        self.type_name = type_name
        self.representation = representation
        self.reason = reason
        self.module = module
        self.qualname = qualname
        self.value_hash = value_hash
        self.observed_mutable = observed_mutable

    def __repr__(self):
        value_hash = self.value_hash or "unhashed"
        return f"opaque<{self.type_name}>[{value_hash[:20]}]"

    def to_record(self):
        return {
            "kind": "opaque",
            "serialiser": "opaque",
            "codec": "descriptor",
            "codec_version": 1,
            "type": self.type_name,
            "module": self.module,
            "qualname": self.qualname,
            "repr": self.representation,
            "reason": self.reason,
            "value_hash": self.value_hash,
            "observed_mutable": self.observed_mutable,
        }
