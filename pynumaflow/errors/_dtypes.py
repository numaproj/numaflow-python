class _RuntimeErrorEntry:
    """Represents a runtime error entry to be persisted."""

    def __init__(self, container: str, timestamp: int, code: str, message: str, details: str):
        self.container = container
        self.timestamp = timestamp
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict:
        return {
            "container": self.container,
            "timestamp": self.timestamp,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }
