import logging
import re
from typing import Any


class SensitiveDataFilter(logging.Filter):
    """
    Logging filter to redact sensitive data (private keys, signatures, etc.)
    from log messages while preserving DEBUG logging capability.
    """

    # Patterns to redact: (regex_pattern, replacement)
    # Matches key="value" or key: "value" patterns
    PATTERNS: list[tuple[str, str]] = [
        # API keys (16+ char strings) - Python dict repr format: 'apiKey': 'value'
        (r"'apiKey':\s*'([^']{16,})'", r"'apiKey': '[REDACTED]'"),
        (r'"apiKey":\s*"([^"]{16,})"', r'"apiKey": "[REDACTED]"'),
        # Secrets (16+ char strings)
        (r"'secret':\s*'([^']{16,})'", r"'secret': '[REDACTED]'"),
        (r'"secret":\s*"([^"]{16,})"', r'"secret": "[REDACTED]"'),
        # Passwords (any non-empty value)
        (r"'password':\s*'([^']+)'", r"'password': '[REDACTED]'"),
        (r'"password":\s*"([^"]+)"', r'"password": "[REDACTED]"'),
        # Private keys (64+ char hex strings)
        (r"'privateKey':\s*'(0x[0-9a-fA-F]{64,})'", r"'privateKey': '[REDACTED]'"),
        (r'"privateKey":\s*"(0x[0-9a-fA-F]{64,})"', r'"privateKey": "[REDACTED]"'),
        (r"'private_key':\s*'(0x[0-9a-fA-F]{64,})'", r"'private_key': '[REDACTED]'"),
        (r'"private_key":\s*"(0x[0-9a-fA-F]{64,})"', r'"private_key": "[REDACTED]"'),
        # Account IDs (hex addresses, 40+ chars)
        (r"'accountId':\s*'(0x[0-9a-fA-F]{40,})'", r"'accountId': '[REDACTED]'"),
        (r'"accountId":\s*"(0x[0-9a-fA-F]{40,})"', r'"accountId": "[REDACTED]"'),
        # Wallet addresses (hex, 40+ chars)
        (r"'walletAddress':\s*'(0x[0-9a-fA-F]{40,})'", r"'walletAddress': '[REDACTED]'"),
        (r'"walletAddress":\s*"(0x[0-9a-fA-F]{40,})"', r'"walletAddress": "[REDACTED]"'),
        # Proxy URLs with embedded credentials: http://user:pass@host:port
        (r"'httpsProxy':\s*'(https?://[^:]+:[^@]+@[^']+)'", r"'httpsProxy': '[REDACTED]'"),
        (r'"httpsProxy":\s*"(https?://[^:]+:[^@]+@[^"]+)"', r'"httpsProxy": "[REDACTED]"'),
        (r"'httpProxy':\s*'(https?://[^:]+:[^@]+@[^']+)'", r"'httpProxy': '[REDACTED]'"),
        (r'"httpProxy":\s*"(https?://[^:]+:[^@]+@[^"]+)"', r'"httpProxy": "[REDACTED]"'),
        # Signatures (64+ char hex strings)
        (r"'signature':\s*'(0x[0-9a-fA-F]{64,})'", r"'signature': '[REDACTED]'"),
        (r'"signature":\s*"(0x[0-9a-fA-F]{64,})"', r'"signature": "[REDACTED]"'),
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

        # Sanitize arguments
        # record.args can be a tuple or a dict
        # - tuple: positional args for %s formatting
        # - dict: EITHER named args for %(name)s formatting OR a single dict passed to %s
        #
        # IMPORTANT: Non-string args (dicts, lists) must be converted to strings
        # before sanitizing, because they get converted to strings AFTER the filter
        # by the logging formatter.
        if record.args:
            if isinstance(record.args, dict):
                # Check if this is a single dict arg for %s formatting or named args for %(name)s
                # If msg contains %s but not %(, treat dict as single positional arg
                msg = record.msg if record.msg else ""
                if "%s" in msg and "%(" not in msg:
                    # Single dict arg for %s formatting - convert to sanitized string tuple
                    record.args = (self._sanitize(str(record.args)),)
                else:
                    # Named args for %(name)s formatting - sanitize each value
                    record.args = {
                        k: self._sanitize_any(v) for k, v in record.args.items()
                    }
            elif isinstance(record.args, tuple):
                record.args = tuple(self._sanitize_any(arg) for arg in record.args)
        return True  # Always return True to keep the record

    def _sanitize_any(self, value: Any) -> str | Any:
        """Sanitize any value, converting non-strings to string first if needed."""
        if isinstance(value, str):
            return self._sanitize(value)
        elif isinstance(value, (dict, list)):
            # Convert to string repr, then sanitize
            return self._sanitize(str(value))
        else:
            return value

    def _sanitize(self, text: str) -> str:
        """Apply all redaction patterns to the text."""
        for pattern, replacement in self._compiled_patterns:
            text = pattern.sub(replacement, text)
        return text
