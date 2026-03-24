import logging
import re
import sys
from typing import Any


# Bare private key pattern: optionally 0x-prefixed, exactly 64 hex chars (256-bit key).
# Uses word boundary to avoid matching inside longer hex strings.
_BARE_HEX_KEY_RE = re.compile(r"\b(?:0x)?[0-9a-fA-F]{64}\b")

# PEM private key blocks
_PEM_KEY_RE = re.compile(
    r"-----BEGIN (?:EC |RSA |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----"
    r"[\s\S]*?"
    r"-----END (?:EC |RSA |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----",
    re.MULTILINE,
)

# ed25519 secret format used in this repo (e.g., "ed25519:JC5mjh84Gki...")
_ED25519_SECRET_RE = re.compile(r"ed25519:[A-Za-z0-9+/=]{32,}")


class SensitiveDataFilter(logging.Filter):
    """
    Logging filter to redact sensitive data (private keys, signatures, etc.)
    from log messages while preserving DEBUG logging capability.
    """

    # Patterns to redact: (regex_pattern, replacement)
    # Matches key="value" or key: "value" patterns in Python dict repr / JSON
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
        # Private keys (64+ char hex strings) — keyed format
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

    def _sanitize(self, text: str) -> str:
        """Apply all redaction patterns to the text (instance convenience method)."""
        return sanitize_text(text, self._compiled_patterns)

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and sanitize the log record message."""
        # Sanitize the message
        if record.msg and isinstance(record.msg, str):
            record.msg = sanitize_text(record.msg, self._compiled_patterns)

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
                    record.args = (sanitize_text(str(record.args), self._compiled_patterns),)
                else:
                    # Named args for %(name)s formatting - sanitize each value
                    record.args = {
                        k: _sanitize_any(v, self._compiled_patterns)
                        for k, v in record.args.items()
                    }
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    _sanitize_any(arg, self._compiled_patterns) for arg in record.args
                )

        # Sanitize exception text if present
        if record.exc_text:
            record.exc_text = sanitize_text(record.exc_text, self._compiled_patterns)

        return True  # Always return True to keep the record


# ---------------------------------------------------------------------------
# Public helpers — usable outside of the logging.Filter pathway
# ---------------------------------------------------------------------------

def sanitize_text(text: str, compiled_patterns: list | None = None) -> str:
    """Apply all redaction patterns to a string.

    This is the single sanitization entry point used by log filters,
    notebook patches, git hooks, and save guards.
    """
    if compiled_patterns is None:
        compiled_patterns = _get_default_compiled_patterns()

    for pattern, replacement in compiled_patterns:
        text = pattern.sub(replacement, text)

    # Bare hex private keys (not already caught by keyed patterns)
    text = _BARE_HEX_KEY_RE.sub("[REDACTED-KEY]", text)
    # PEM private key blocks
    text = _PEM_KEY_RE.sub("[REDACTED-PEM-KEY]", text)
    # ed25519 secrets
    text = _ED25519_SECRET_RE.sub("[REDACTED-ED25519]", text)
    return text


def contains_secret(text: str) -> bool:
    """Return True if *text* contains anything that looks like a secret.

    Unlike ``sanitize_text`` this does **not** modify the text — it is a
    pure detection function used by save hooks and git scanners to decide
    whether to block an operation.
    """
    if _BARE_HEX_KEY_RE.search(text):
        return True
    if _PEM_KEY_RE.search(text):
        return True
    if _ED25519_SECRET_RE.search(text):
        return True
    # Check keyed patterns
    for pattern, _ in _get_default_compiled_patterns():
        if pattern.search(text):
            return True
    return False


def sanitize_mime_bundle(data: dict[str, Any]) -> dict[str, Any]:
    """Sanitize a Jupyter MIME bundle dict (e.g. from display_data messages).

    Only string values are filtered; binary payloads (images) are left alone.
    """
    compiled = _get_default_compiled_patterns()
    result = {}
    for mime_type, value in data.items():
        if isinstance(value, str):
            result[mime_type] = sanitize_text(value, compiled)
        elif isinstance(value, list):
            result[mime_type] = [
                sanitize_text(item, compiled) if isinstance(item, str) else item
                for item in value
            ]
        else:
            result[mime_type] = value
    return result


# ---------------------------------------------------------------------------
# Notebook runtime patching
# ---------------------------------------------------------------------------

_NOTEBOOK_PATCHED = False
_ORIGINAL_STDOUT_WRITE = None
_ORIGINAL_STDERR_WRITE = None
_ORIGINAL_WRITE_FORMAT_DATA = None
_ORIGINAL_PUBLISH_DISPLAY_DATA = None
_ORIGINAL_SHOWTRACEBACK = None


def patch_notebook() -> None:
    """Extend sensitive data filtering to all Jupyter/IPython output channels.

    Patches:
    1. sys.stdout.write / sys.stderr.write — catches print()
    2. DisplayHook.write_format_data — catches last-expression auto-display
    3. publish_display_data — catches display() / rich objects
    4. InteractiveShell.showtraceback — catches leaked locals in tracebacks

    Safe to call multiple times. No-op if IPython is not running.
    """
    global _NOTEBOOK_PATCHED
    global _ORIGINAL_STDOUT_WRITE, _ORIGINAL_STDERR_WRITE
    global _ORIGINAL_WRITE_FORMAT_DATA, _ORIGINAL_PUBLISH_DISPLAY_DATA
    global _ORIGINAL_SHOWTRACEBACK

    if _NOTEBOOK_PATCHED:
        return

    try:
        from IPython import get_ipython
        ipython = get_ipython()
        if ipython is None:
            return  # Not running inside IPython/Jupyter
    except ImportError:
        return  # IPython not installed

    compiled = _get_default_compiled_patterns()

    # --- Channel 1: stdout / stderr ---
    _ORIGINAL_STDOUT_WRITE = sys.stdout.write
    _ORIGINAL_STDERR_WRITE = sys.stderr.write

    def _sanitized_stdout_write(text):
        if isinstance(text, str):
            text = sanitize_text(text, compiled)
        return _ORIGINAL_STDOUT_WRITE(text)

    def _sanitized_stderr_write(text):
        if isinstance(text, str):
            text = sanitize_text(text, compiled)
        return _ORIGINAL_STDERR_WRITE(text)

    sys.stdout.write = _sanitized_stdout_write
    sys.stderr.write = _sanitized_stderr_write

    # --- Channel 2: DisplayHook (last-expression auto-display) ---
    if hasattr(ipython, "displayhook") and hasattr(ipython.displayhook, "write_format_data"):
        _ORIGINAL_WRITE_FORMAT_DATA = ipython.displayhook.write_format_data

        def _sanitized_write_format_data(format_dict, md_dict=None):
            format_dict = sanitize_mime_bundle(format_dict)
            return _ORIGINAL_WRITE_FORMAT_DATA(format_dict, md_dict)

        ipython.displayhook.write_format_data = _sanitized_write_format_data

    # --- Channel 3: publish_display_data (display() calls) ---
    try:
        import IPython.core.display_functions as _display_mod

        _ORIGINAL_PUBLISH_DISPLAY_DATA = _display_mod.publish_display_data

        def _sanitized_publish_display_data(data, metadata=None, *, transient=None, **kwargs):
            if isinstance(data, dict):
                data = sanitize_mime_bundle(data)
            return _ORIGINAL_PUBLISH_DISPLAY_DATA(
                data, metadata, transient=transient, **kwargs
            )

        _display_mod.publish_display_data = _sanitized_publish_display_data
    except (ImportError, AttributeError):
        pass

    # --- Channel 4: Traceback display ---
    if hasattr(ipython, "showtraceback"):
        _ORIGINAL_SHOWTRACEBACK = ipython.showtraceback

        def _sanitized_showtraceback(*args, **kwargs):
            # Capture the traceback output by temporarily redirecting stderr
            import io
            buf = io.StringIO()
            old_stderr = sys.stderr
            # Temporarily bypass our stderr patch to avoid double-sanitizing
            sys.stderr = buf
            try:
                _ORIGINAL_SHOWTRACEBACK(*args, **kwargs)
            finally:
                sys.stderr = old_stderr
            sanitized = sanitize_text(buf.getvalue(), compiled)
            if sanitized:
                old_stderr.write(sanitized)

        ipython.showtraceback = _sanitized_showtraceback

    _NOTEBOOK_PATCHED = True


def unpatch_notebook() -> None:
    """Remove all notebook output patches. Mainly useful for testing."""
    global _NOTEBOOK_PATCHED
    global _ORIGINAL_STDOUT_WRITE, _ORIGINAL_STDERR_WRITE
    global _ORIGINAL_WRITE_FORMAT_DATA, _ORIGINAL_PUBLISH_DISPLAY_DATA
    global _ORIGINAL_SHOWTRACEBACK

    if not _NOTEBOOK_PATCHED:
        return

    if _ORIGINAL_STDOUT_WRITE is not None:
        sys.stdout.write = _ORIGINAL_STDOUT_WRITE
    if _ORIGINAL_STDERR_WRITE is not None:
        sys.stderr.write = _ORIGINAL_STDERR_WRITE

    try:
        from IPython import get_ipython
        ipython = get_ipython()
        if ipython is not None:
            if _ORIGINAL_WRITE_FORMAT_DATA is not None:
                ipython.displayhook.write_format_data = _ORIGINAL_WRITE_FORMAT_DATA
            if _ORIGINAL_SHOWTRACEBACK is not None:
                ipython.showtraceback = _ORIGINAL_SHOWTRACEBACK
    except ImportError:
        pass

    if _ORIGINAL_PUBLISH_DISPLAY_DATA is not None:
        try:
            import IPython.core.display_functions as _display_mod
            _display_mod.publish_display_data = _ORIGINAL_PUBLISH_DISPLAY_DATA
        except (ImportError, AttributeError):
            pass

    _NOTEBOOK_PATCHED = False
    _ORIGINAL_STDOUT_WRITE = None
    _ORIGINAL_STDERR_WRITE = None
    _ORIGINAL_WRITE_FORMAT_DATA = None
    _ORIGINAL_PUBLISH_DISPLAY_DATA = None
    _ORIGINAL_SHOWTRACEBACK = None


def is_notebook_patched() -> bool:
    """Check if notebook output channels have been patched."""
    return _NOTEBOOK_PATCHED


# ---------------------------------------------------------------------------
# Logging monkeypatch (mirrors web3-ethereum-defi version)
# ---------------------------------------------------------------------------

_LOGGING_PATCHED = False
_SENSITIVE_FILTER: SensitiveDataFilter | None = None
_ORIGINAL_HANDLER_INIT = None
_ORIGINAL_RECORD_FACTORY = None
_ORIGINAL_FORMAT_EXCEPTION = None


def patch_logging() -> None:
    """Add SensitiveDataFilter to all existing and future log handlers.

    Also installs:
    - A custom LogRecordFactory so msg/args are sanitized before handlers
    - A Formatter.formatException wrapper so exc_text is sanitized when
      generated (exc_text is populated lazily, so the factory can't catch it)

    Safe to call multiple times — will only apply once.
    """
    global _LOGGING_PATCHED, _SENSITIVE_FILTER, _ORIGINAL_HANDLER_INIT
    global _ORIGINAL_RECORD_FACTORY, _ORIGINAL_FORMAT_EXCEPTION

    if _LOGGING_PATCHED:
        return

    _SENSITIVE_FILTER = SensitiveDataFilter()
    compiled = _get_default_compiled_patterns()

    # Add filter to all existing handlers
    for handler in logging.root.handlers:
        handler.addFilter(_SENSITIVE_FILTER)

    # Monkeypatch logging.Handler.__init__ to add filter to future handlers
    _ORIGINAL_HANDLER_INIT = logging.Handler.__init__

    def patched_handler_init(self, level=logging.NOTSET):
        _ORIGINAL_HANDLER_INIT(self, level)
        if _SENSITIVE_FILTER is not None:
            self.addFilter(_SENSITIVE_FILTER)

    logging.Handler.__init__ = patched_handler_init

    # Install a custom LogRecordFactory for pre-handler sanitization of msg/args
    _ORIGINAL_RECORD_FACTORY = logging.getLogRecordFactory()
    _install_sanitizing_record_factory(_ORIGINAL_RECORD_FACTORY)

    # Patch Formatter.formatException so exc_text is sanitized when generated.
    # exc_text is populated lazily during format() — after both the factory
    # and filter have already run — so it must be caught at the source.
    _ORIGINAL_FORMAT_EXCEPTION = logging.Formatter.formatException

    def sanitized_format_exception(self, ei):
        text = _ORIGINAL_FORMAT_EXCEPTION(self, ei)
        return sanitize_text(text, compiled)

    logging.Formatter.formatException = sanitized_format_exception

    _LOGGING_PATCHED = True


def unpatch_logging() -> None:
    """Remove the logging monkeypatch. Mainly useful for testing."""
    global _LOGGING_PATCHED, _SENSITIVE_FILTER, _ORIGINAL_HANDLER_INIT
    global _ORIGINAL_RECORD_FACTORY, _ORIGINAL_FORMAT_EXCEPTION

    if not _LOGGING_PATCHED:
        return

    if _ORIGINAL_HANDLER_INIT is not None:
        logging.Handler.__init__ = _ORIGINAL_HANDLER_INIT

    # Restore original LogRecordFactory
    if _ORIGINAL_RECORD_FACTORY is not None:
        logging.setLogRecordFactory(_ORIGINAL_RECORD_FACTORY)

    # Restore original Formatter.formatException
    if _ORIGINAL_FORMAT_EXCEPTION is not None:
        logging.Formatter.formatException = _ORIGINAL_FORMAT_EXCEPTION

    # Remove filter from existing handlers
    if _SENSITIVE_FILTER is not None:
        for handler in logging.root.handlers:
            handler.removeFilter(_SENSITIVE_FILTER)

    _LOGGING_PATCHED = False
    _SENSITIVE_FILTER = None
    _ORIGINAL_HANDLER_INIT = None
    _ORIGINAL_RECORD_FACTORY = None
    _ORIGINAL_FORMAT_EXCEPTION = None


def is_logging_patched() -> bool:
    """Check if logging has been patched with SensitiveDataFilter."""
    return _LOGGING_PATCHED


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_DEFAULT_COMPILED_PATTERNS: list | None = None


def _get_default_compiled_patterns() -> list:
    """Lazily compile and cache the default redaction patterns."""
    global _DEFAULT_COMPILED_PATTERNS
    if _DEFAULT_COMPILED_PATTERNS is None:
        _DEFAULT_COMPILED_PATTERNS = [
            (re.compile(pattern, re.IGNORECASE), replacement)
            for pattern, replacement in SensitiveDataFilter.PATTERNS
        ]
    return _DEFAULT_COMPILED_PATTERNS


def _sanitize_any(value: Any, compiled_patterns: list) -> str | Any:
    """Sanitize any value, converting non-strings to string first if needed."""
    if isinstance(value, str):
        return sanitize_text(value, compiled_patterns)
    elif isinstance(value, (dict, list)):
        return sanitize_text(str(value), compiled_patterns)
    return value


def _install_sanitizing_record_factory(original_factory) -> None:
    """Wrap the current LogRecordFactory so every record is sanitized at creation.

    This ensures sanitization happens *before* any handler or filter sees
    the record, closing the FTBufferingHandler / RPC blind spot where
    ``exc_text`` and ``message`` are read verbatim from the buffer.

    The factory only sanitizes string values within existing arg structures —
    it never converts a dict to a tuple, which would break ``%(name)s``
    formatting and cause ``KeyError``.
    """
    compiled = _get_default_compiled_patterns()

    def sanitizing_factory(*args, **kwargs):
        record = original_factory(*args, **kwargs)
        if record.msg and isinstance(record.msg, str):
            record.msg = sanitize_text(record.msg, compiled)
        if record.args:
            if isinstance(record.args, dict):
                # Always preserve dict shape — never convert to tuple.
                # This handles both %(name)s named args and single-dict %s args.
                # The handler-level Filter handles the %s-with-dict edge case.
                record.args = {
                    k: _sanitize_any(v, compiled) for k, v in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(_sanitize_any(a, compiled) for a in record.args)
        # Sanitize exception text — this is what _rpc_get_logs reads from
        # bufferHandler.buffer[].exc_text verbatim.
        if record.exc_text and isinstance(record.exc_text, str):
            record.exc_text = sanitize_text(record.exc_text, compiled)
        return record

    logging.setLogRecordFactory(sanitizing_factory)
