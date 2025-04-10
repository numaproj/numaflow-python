from dataclasses import dataclass, asdict


@dataclass
class _RuntimeErrorEntry:
    """Represents a runtime error entry to be persisted."""

    container: str
    timestamp: int
    code: str
    message: str
    details: str

    def to_dict(self) -> dict:
        """Converts the dataclass instance to a dictionary."""
        return asdict(self)
