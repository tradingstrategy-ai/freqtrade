import logging
import re


class SensitiveDataFilter(logging.Filter):
    """
    Logging filter to redact sensitive data (private keys, signatures, etc.)
    from log messages while preserving DEBUG logging capability.
    """

    # Patterns to redact: (regex_pattern, replacement)
    # Matches key="value" or key: "value" patterns with hex strings
    PATTERNS: list[tuple[str, str]] = [
        # Private keys (64+ char hex strings)
        (r'("privateKey"\s*:\s*")([0-9a-fA-Fx]{64,})(")', r'\1[REDACTED]\3'),
        (r'("private_key"\s*:\s*")([0-9a-fA-Fx]{64,})(")', r'\1[REDACTED]\3'),
        (r"('privateKey'\s*:\s*')([0-9a-fA-Fx]{64,})(')", r"\1[REDACTED]\3"),
        (r"('private_key'\s*:\s*')([0-9a-fA-Fx]{64,})(')", r"\1[REDACTED]\3"),
        # Signatures (64+ char hex strings)
        (r'("signature"\s*:\s*")([0-9a-fA-Fx]{64,})(")', r'\1[REDACTED]\3'),
        (r"('signature'\s*:\s*')([0-9a-fA-Fx]{64,})(')", r"\1[REDACTED]\3"),
        # API keys and secrets (16+ char strings)
        (r'("apiKey"\s*:\s*")([^"]{16,})(")', r'\1[REDACTED]\3'),
        (r'("secret"\s*:\s*")([^"]{16,})(")', r'\1[REDACTED]\3'),
        (r"('apiKey'\s*:\s*')([^']{16,})(')", r"\1[REDACTED]\3"),
        (r"('secret'\s*:\s*')([^']{16,})(')", r"\1[REDACTED]\3"),
    ]

    def __init__(self, name: str = ""):
        super().__init__(name)
        self._compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), replacement)
            for pattern, replacement in self.PATTERNS
        ]

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and sanitize the log record message."""
        # Sanitize the message
        if record.msg and isinstance(record.msg, str):
            record.msg = self._sanitize(record.msg)

        # Sanitize string arguments
        # record.args can be a tuple or a dict (for %(name)s style formatting)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: self._sanitize(v) if isinstance(v, str) else v
                    for k, v in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    self._sanitize(arg) if isinstance(arg, str) else arg
                    for arg in record.args
                )
        return True  # Always return True to keep the record

    def _sanitize(self, text: str) -> str:
        """Apply all redaction patterns to the text."""
        for pattern, replacement in self._compiled_patterns:
            text = pattern.sub(replacement, text)
        return text
