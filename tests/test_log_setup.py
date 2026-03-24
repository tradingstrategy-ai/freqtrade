import logging
import logging.handlers
import re
import sys

import pytest

from freqtrade.exceptions import OperationalException
from freqtrade.loggers import (
    FTBufferingHandler,
    FtRichHandler,
    setup_logging,
    setup_logging_pre,
)
from freqtrade.loggers.sensitive_filter import (
    SensitiveDataFilter,
    contains_secret,
    is_logging_patched,
    patch_logging,
    patch_notebook,
    sanitize_text,
    unpatch_logging,
    unpatch_notebook,
)
from freqtrade.loggers.set_log_levels import (
    reduce_verbosity_for_bias_tester,
    restore_verbosity_for_bias_tester,
)


@pytest.mark.usefixtures("keep_log_config_loggers")
def test_set_loggers() -> None:
    # Reset Logging to Debug, otherwise this fails randomly as it's set globally
    logging.getLogger("requests").setLevel(logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)
    logging.getLogger("ccxt.base.exchange").setLevel(logging.DEBUG)
    logging.getLogger("telegram").setLevel(logging.DEBUG)

    previous_value1 = logging.getLogger("requests").level
    previous_value2 = logging.getLogger("ccxt.base.exchange").level
    previous_value3 = logging.getLogger("telegram").level
    config = {
        "verbosity": 1,
        "ft_tests_force_logging": True,
    }
    setup_logging(config)

    value1 = logging.getLogger("requests").level
    assert previous_value1 is not value1
    assert value1 is logging.INFO

    value2 = logging.getLogger("ccxt.base.exchange").level
    assert previous_value2 is not value2
    assert value2 is logging.INFO

    value3 = logging.getLogger("telegram").level
    assert previous_value3 is not value3
    assert value3 is logging.INFO
    config["verbosity"] = 2
    setup_logging(config)

    assert logging.getLogger("requests").level is logging.DEBUG
    assert logging.getLogger("ccxt.base.exchange").level is logging.INFO
    assert logging.getLogger("telegram").level is logging.INFO
    assert logging.getLogger("werkzeug").level is logging.INFO

    config["verbosity"] = 3
    config["api_server"] = {"verbosity": "error"}
    setup_logging(config)

    assert logging.getLogger("requests").level is logging.DEBUG
    assert logging.getLogger("ccxt.base.exchange").level is logging.DEBUG
    assert logging.getLogger("telegram").level is logging.INFO
    assert logging.getLogger("werkzeug").level is logging.ERROR


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@pytest.mark.usefixtures("keep_log_config_loggers")
def test_set_loggers_syslog():
    logger = logging.getLogger()
    orig_handlers = logger.handlers
    logger.handlers = []

    config = {
        "ft_tests_force_logging": True,
        "verbosity": 2,
        "logfile": "syslog:/dev/log",
    }

    setup_logging_pre()
    setup_logging(config)
    assert len(logger.handlers) == 3
    assert [x for x in logger.handlers if isinstance(x, logging.handlers.SysLogHandler)]
    assert [x for x in logger.handlers if isinstance(x, FtRichHandler)]
    assert [x for x in logger.handlers if isinstance(x, FTBufferingHandler)]
    # setting up logging again should NOT cause the loggers to be added a second time.
    setup_logging(config)
    assert len(logger.handlers) == 3
    # reset handlers to not break pytest
    logger.handlers = orig_handlers


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@pytest.mark.usefixtures("keep_log_config_loggers")
def test_set_loggers_Filehandler(tmp_path):
    logger = logging.getLogger()
    orig_handlers = logger.handlers
    logger.handlers = []
    logfile = tmp_path / "logs/ft_logfile.log"
    config = {
        "ft_tests_force_logging": True,
        "verbosity": 2,
        "logfile": str(logfile),
    }

    setup_logging_pre()
    setup_logging(config)
    assert len(logger.handlers) == 3
    assert [x for x in logger.handlers if isinstance(x, logging.handlers.RotatingFileHandler)]
    assert [x for x in logger.handlers if isinstance(x, FtRichHandler)]
    assert [x for x in logger.handlers if isinstance(x, FTBufferingHandler)]
    # setting up logging again should NOT cause the loggers to be added a second time.
    setup_logging(config)
    assert len(logger.handlers) == 3
    # reset handlers to not break pytest
    if logfile.exists:
        logfile.unlink()
    logger.handlers = orig_handlers


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@pytest.mark.usefixtures("keep_log_config_loggers")
def test_set_loggers_Filehandler_without_permission(tmp_path):
    logger = logging.getLogger()
    orig_handlers = logger.handlers
    logger.handlers = []

    try:
        tmp_path.chmod(0o400)
        logfile = tmp_path / "logs/ft_logfile.log"
        config = {
            "ft_tests_force_logging": True,
            "verbosity": 2,
            "logfile": str(logfile),
        }

        setup_logging_pre()
        with pytest.raises(OperationalException):
            setup_logging(config)

        logger.handlers = orig_handlers
    finally:
        tmp_path.chmod(0o700)


@pytest.mark.skip(reason="systemd is not installed on every system, so we're not testing this.")
@pytest.mark.usefixtures("keep_log_config_loggers")
def test_set_loggers_journald():
    logger = logging.getLogger()
    orig_handlers = logger.handlers
    logger.handlers = []

    config = {
        "ft_tests_force_logging": True,
        "verbosity": 2,
        "logfile": "journald",
    }

    setup_logging_pre()
    setup_logging(config)
    assert len(logger.handlers) == 3
    assert [x for x in logger.handlers if type(x).__name__ == "JournaldLogHandler"]
    assert [x for x in logger.handlers if isinstance(x, FtRichHandler)]
    # reset handlers to not break pytest
    logger.handlers = orig_handlers


@pytest.mark.usefixtures("keep_log_config_loggers")
def test_set_loggers_journald_importerror(import_fails):
    logger = logging.getLogger()
    orig_handlers = logger.handlers
    logger.handlers = []

    config = {
        "ft_tests_force_logging": True,
        "verbosity": 2,
        "logfile": "journald",
    }
    with pytest.raises(OperationalException, match=r"You need the cysystemd python package.*"):
        setup_logging(config)
    logger.handlers = orig_handlers


@pytest.mark.usefixtures("keep_log_config_loggers")
def test_set_loggers_json_format(capsys):
    logger = logging.getLogger()
    orig_handlers = logger.handlers
    logger.handlers = []

    config = {
        "ft_tests_force_logging": True,
        "verbosity": 2,
        "log_config": {
            "version": 1,
            "formatters": {
                "json": {
                    "()": "freqtrade.loggers.json_formatter.JsonFormatter",
                    "fmt_dict": {
                        "timestamp": "asctime",
                        "level": "levelname",
                        "logger": "name",
                        "message": "message",
                    },
                }
            },
            "handlers": {
                "json": {
                    "class": "logging.StreamHandler",
                    "formatter": "json",
                }
            },
            "root": {
                "handlers": ["json"],
                "level": "DEBUG",
            },
        },
    }

    setup_logging_pre()
    setup_logging(config)
    assert len(logger.handlers) == 2
    assert [x for x in logger.handlers if type(x).__name__ == "StreamHandler"]
    assert [x for x in logger.handlers if isinstance(x, FTBufferingHandler)]

    logger.info("Test message")

    captured = capsys.readouterr()
    assert re.search(r'{"timestamp": ".*"Test message".*', captured.err)

    # reset handlers to not break pytest
    logger.handlers = orig_handlers


def test_reduce_verbosity():
    setup_logging_pre()
    reduce_verbosity_for_bias_tester()
    prior_level = logging.getLogger("freqtrade").getEffectiveLevel()

    assert logging.getLogger("freqtrade.resolvers").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("freqtrade.strategy.hyper").getEffectiveLevel() == logging.WARNING
    # base level wasn't changed
    assert logging.getLogger("freqtrade").getEffectiveLevel() == prior_level

    restore_verbosity_for_bias_tester()

    assert logging.getLogger("freqtrade.resolvers").getEffectiveLevel() == prior_level
    assert logging.getLogger("freqtrade.strategy.hyper").getEffectiveLevel() == prior_level
    assert logging.getLogger("freqtrade").getEffectiveLevel() == prior_level
    # base level wasn't changed


class TestSensitiveDataFilter:
    """Tests for the SensitiveDataFilter logging filter."""

    def test_redacts_private_key_double_quotes(self):
        f = SensitiveDataFilter()
        text = '{"privateKey": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"}'
        result = f._sanitize(text)
        assert "[REDACTED]" in result
        assert "1234567890abcdef" not in result

    def test_redacts_private_key_single_quotes(self):
        f = SensitiveDataFilter()
        text = "{'privateKey': '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'}"
        result = f._sanitize(text)
        assert "[REDACTED]" in result
        assert "1234567890abcdef" not in result

    def test_redacts_private_key_snake_case(self):
        f = SensitiveDataFilter()
        text = '{"private_key": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"}'
        result = f._sanitize(text)
        assert "[REDACTED]" in result
        assert "abcdef1234567890" not in result

    def test_redacts_signature(self):
        f = SensitiveDataFilter()
        text = '{"signature": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"}'
        result = f._sanitize(text)
        assert "[REDACTED]" in result
        assert "abcdef1234567890" not in result

    def test_redacts_api_key(self):
        f = SensitiveDataFilter()
        text = '{"apiKey": "my-super-secret-api-key-12345"}'
        result = f._sanitize(text)
        assert "[REDACTED]" in result
        assert "my-super-secret-api-key-12345" not in result

    def test_redacts_secret(self):
        f = SensitiveDataFilter()
        text = '{"secret": "very-long-secret-value-here"}'
        result = f._sanitize(text)
        assert "[REDACTED]" in result
        assert "very-long-secret-value-here" not in result

    def test_preserves_normal_content(self):
        f = SensitiveDataFilter()
        text = '{"type": "order", "price": 100.5, "amount": 1.0}'
        result = f._sanitize(text)
        assert result == text  # Unchanged

    def test_preserves_short_values(self):
        f = SensitiveDataFilter()
        # Values shorter than thresholds should not be redacted
        text = '{"apiKey": "short"}'
        result = f._sanitize(text)
        assert result == text  # Unchanged (less than 16 chars)

    def test_filter_method_sanitizes_message(self):
        f = SensitiveDataFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg='Request: {"privateKey": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"}',
            args=(),
            exc_info=None,
        )
        result = f.filter(record)
        assert result is True  # Record should always be kept
        assert "[REDACTED]" in record.msg
        assert "1234567890abcdef" not in record.msg

    def test_filter_method_sanitizes_args(self):
        f = SensitiveDataFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Data: %s",
            args=('{"privateKey": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"}',),
            exc_info=None,
        )
        result = f.filter(record)
        assert result is True
        assert "[REDACTED]" in record.args[0]
        assert "1234567890abcdef" not in record.args[0]

    def test_case_insensitive(self):
        f = SensitiveDataFilter()
        # Test case insensitivity for key names
        text = '{"PRIVATEKEY": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"}'
        result = f._sanitize(text)
        assert "[REDACTED]" in result
        assert "1234567890abcdef" not in result

    def test_filter_method_with_dict_args(self):
        """Test that dict-style args (for %(name)s formatting) are handled correctly."""
        f = SensitiveDataFilter()
        # Set args after construction to avoid Python 3.12 LogRecord.__init__
        # bug with single-key dict args (KeyError: 0 on args[0] check)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Config: %(config)s",
            args=None,
            exc_info=None,
        )
        record.args = {"config": '{"privateKey": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"}'}
        result = f.filter(record)
        assert result is True
        assert isinstance(record.args, dict)
        assert "[REDACTED]" in record.args["config"]

    def test_filter_preserves_non_sensitive_dict_args(self):
        """Test that non-sensitive dict args are converted to string but content preserved."""
        f = SensitiveDataFilter()
        test_dict = {"options": True, "rateLimit": 500}
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Config: %s",
            args=(test_dict,),
            exc_info=None,
        )
        result = f.filter(record)
        assert result is True
        assert isinstance(record.args, tuple)
        # Dict is converted to string for sanitization
        sanitized = record.args[0]
        assert isinstance(sanitized, str)
        assert "500" in sanitized
        assert "True" in sanitized

    def test_redacts_ccxt_config_dict(self):
        """Test that the ACTUAL log format from exchange.py is redacted."""
        f = SensitiveDataFilter()

        # This is the EXACT format logged by freqtrade
        # When logger.info("msg: %s", dict) is called, Python stores dict as args directly
        ccxt_kwargs = {
            "options": {"defaultType": "swap"},
            "enableRateLimit": True,
            "rateLimit": 500,
            "apiKey": "ed25519:GjzHM8W8G6QU1BgZjEqymm8csu3jexsbc7HoqHPaar4f",
            "secret": "ed25519:JC5mjh84GkitnPrzvJLeN8AJ8CGZeKNfrvZmLJ9cBC6w",
            "accountId": "0xc2f9d39accd5e68e79241c105ffaabe52f662ba6e921125268040b46f9ce5f5e",
            "httpsProxy": "http://201678:D8TMz2SjqYSz@80.65.222.147:8800",
        }

        record = logging.LogRecord(
            name="freqtrade.exchange.exchange",
            level=logging.INFO,
            pathname="",
            lineno=403,
            msg="Applying additional ccxt config: %s",
            args=ccxt_kwargs,  # Python stores single dict arg directly, not as tuple
            exc_info=None,
        )

        result = f.filter(record)
        assert result is True

        # Args should now be a tuple with a sanitized STRING
        assert isinstance(record.args, tuple)
        sanitized = record.args[0]
        assert isinstance(sanitized, str)

        # Verify secrets are redacted
        assert "ed25519:GjzHM8W8G6QU1BgZjEqymm8csu3jexsbc7HoqHPaar4f" not in sanitized
        assert "ed25519:JC5mjh84GkitnPrzvJLeN8AJ8CGZeKNfrvZmLJ9cBC6w" not in sanitized
        assert "0xc2f9d39accd5e68e79241c105ffaabe52f662ba6e921125268040b46f9ce5f5e" not in sanitized
        assert "201678:D8TMz2SjqYSz" not in sanitized

        # Verify [REDACTED] appears
        assert "[REDACTED]" in sanitized

        # Verify non-sensitive data preserved
        assert "swap" in sanitized
        assert "500" in sanitized

    def test_redacts_proxy_with_credentials(self):
        """Test proxy URLs with embedded user:pass are redacted."""
        f = SensitiveDataFilter()
        text = "{'httpsProxy': 'http://201678:D8TMz2SjqYSz@80.65.222.147:8800'}"
        result = f._sanitize(text)
        assert "201678:D8TMz2SjqYSz" not in result
        assert "[REDACTED]" in result

    def test_redacts_password(self):
        """Test password fields are redacted."""
        f = SensitiveDataFilter()
        text = "{'password': 'mysecretpassword'}"
        result = f._sanitize(text)
        assert "mysecretpassword" not in result
        assert "[REDACTED]" in result

    def test_redacts_account_id(self):
        """Test accountId hex addresses are redacted."""
        f = SensitiveDataFilter()
        text = "{'accountId': '0xc2f9d39accd5e68e79241c105ffaabe52f662ba6e921125268040b46f9ce5f5e'}"
        result = f._sanitize(text)
        assert "0xc2f9d39accd5e68e79241c105ffaabe52f662ba6e921125268040b46f9ce5f5e" not in result
        assert "[REDACTED]" in result

    def test_redacts_wallet_address(self):
        """Test walletAddress hex addresses are redacted."""
        f = SensitiveDataFilter()
        text = "{'walletAddress': '0x742d35cc6634c0532925a3b844bc454e4438f44e'}"
        result = f._sanitize(text)
        assert "0x742d35cc6634c0532925a3b844bc454e4438f44e" not in result
        assert "[REDACTED]" in result


class TestSanitizeTextHelpers:
    """Tests for the module-level sanitize_text, contains_secret, etc."""

    def test_sanitize_text_bare_hex_key(self):
        """Bare 0x + 64 hex chars are redacted even without a key name."""
        key = "0x" + "ab" * 32  # 64 hex chars
        result = sanitize_text(f"Using key {key} for signing")
        assert key not in result
        assert "[REDACTED-KEY]" in result

    def test_sanitize_text_preserves_short_hex(self):
        """Short hex strings (tx hashes, addresses) are NOT redacted by bare key pattern."""
        tx_hash = "0x" + "ab" * 16  # Only 32 hex chars
        result = sanitize_text(f"TX: {tx_hash}")
        assert tx_hash in result  # Should NOT be redacted

    def test_sanitize_text_pem_key(self):
        """PEM private key blocks are redacted."""
        pem = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBg...\n-----END PRIVATE KEY-----"
        result = sanitize_text(f"Key: {pem}")
        assert "MIIEvQIBADANBg" not in result
        assert "[REDACTED-PEM-KEY]" in result

    def test_sanitize_text_ed25519_secret(self):
        """ed25519 secret format is redacted."""
        secret = "ed25519:JC5mjh84GkitnPrzvJLeN8AJ8CGZeKNfrvZmLJ9cBC6w"
        result = sanitize_text(f"Secret: {secret}")
        assert "JC5mjh84Gki" not in result
        assert "[REDACTED-ED25519]" in result

    def test_contains_secret_positive(self):
        """contains_secret returns True for bare hex keys."""
        key = "0x" + "ab" * 32
        assert contains_secret(f"key={key}") is True

    def test_contains_secret_negative(self):
        """contains_secret returns False for normal text."""
        assert contains_secret("Hello world, price is $100.50") is False

    def test_contains_secret_pem(self):
        """contains_secret detects PEM blocks."""
        pem = "-----BEGIN PRIVATE KEY-----\ndata\n-----END PRIVATE KEY-----"
        assert contains_secret(pem) is True

    def test_contains_secret_keyed_pattern(self):
        """contains_secret detects keyed patterns like apiKey."""
        text = "{'apiKey': 'my-super-secret-api-key-12345'}"
        assert contains_secret(text) is True

    def test_sanitize_text_exc_text_redacted(self):
        """Exception text on LogRecord is also sanitized."""
        f = SensitiveDataFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="error", args=(), exc_info=None,
        )
        key = "0x" + "ab" * 32
        record.exc_text = f"Traceback: privateKey={key}"
        f.filter(record)
        assert key not in record.exc_text
        assert "[REDACTED" in record.exc_text


class TestConfigValidationPrivateKeys:
    """Tests for _validate_no_raw_private_keys."""

    def test_rejects_raw_private_key_top_level(self):
        """Raw private_key at exchange level is rejected."""
        from freqtrade.configuration.config_validation import _validate_no_raw_private_keys
        from freqtrade.exceptions import ConfigurationError

        conf = {"exchange": {"name": "gmx", "private_key": "0x" + "ab" * 32}}
        with pytest.raises(ConfigurationError, match="Raw private key found"):
            _validate_no_raw_private_keys(conf)

    def test_rejects_raw_privateKey_top_level(self):
        """Raw privateKey (camelCase) at exchange level is rejected."""
        from freqtrade.configuration.config_validation import _validate_no_raw_private_keys
        from freqtrade.exceptions import ConfigurationError

        conf = {"exchange": {"name": "gmx", "privateKey": "0x" + "ab" * 32}}
        with pytest.raises(ConfigurationError, match="Raw private key found"):
            _validate_no_raw_private_keys(conf)

    def test_rejects_raw_key_in_ccxt_config(self):
        """Raw privateKey inside ccxt_config is rejected."""
        from freqtrade.configuration.config_validation import _validate_no_raw_private_keys
        from freqtrade.exceptions import ConfigurationError

        conf = {
            "exchange": {
                "name": "gmx",
                "ccxt_config": {"privateKey": "0x" + "ab" * 32},
            }
        }
        with pytest.raises(ConfigurationError, match="ccxt_config"):
            _validate_no_raw_private_keys(conf)

    def test_rejects_raw_key_in_ccxt_sync_config(self):
        """Raw privateKey inside ccxt_sync_config is rejected."""
        from freqtrade.configuration.config_validation import _validate_no_raw_private_keys
        from freqtrade.exceptions import ConfigurationError

        conf = {
            "exchange": {
                "name": "gmx",
                "ccxt_sync_config": {"privateKey": "0x" + "ab" * 32},
            }
        }
        with pytest.raises(ConfigurationError, match="ccxt_sync_config"):
            _validate_no_raw_private_keys(conf)

    def test_accepts_private_key_env(self):
        """private_key_env is accepted (no error raised)."""
        from freqtrade.configuration.config_validation import _validate_no_raw_private_keys

        conf = {"exchange": {"name": "gmx", "private_key_env": "GMX_PRIVATE_KEY"}}
        _validate_no_raw_private_keys(conf)  # Should not raise

    def test_accepts_no_private_key(self):
        """Config without any private key field is accepted."""
        from freqtrade.configuration.config_validation import _validate_no_raw_private_keys

        conf = {"exchange": {"name": "binance", "key": "abc", "secret": "def"}}
        _validate_no_raw_private_keys(conf)  # Should not raise


class TestPatchLogging:
    """Tests for patch_logging / unpatch_logging."""

    def test_patch_logging_idempotent(self):
        """Calling patch_logging multiple times is safe."""
        try:
            patch_logging()
            patch_logging()  # Second call should be no-op
            assert is_logging_patched()
        finally:
            unpatch_logging()

    def test_unpatch_restores_state(self):
        """unpatch_logging restores original state."""
        patch_logging()
        assert is_logging_patched()
        unpatch_logging()
        assert not is_logging_patched()

    def test_unpatch_restores_record_factory(self):
        """unpatch_logging must restore the original LogRecordFactory."""
        original = logging.getLogRecordFactory()
        patch_logging()
        # Factory should be different while patched
        assert logging.getLogRecordFactory() is not original
        unpatch_logging()
        # Factory must be restored
        assert logging.getLogRecordFactory() is original

    def test_exc_text_sanitized_in_buffer(self):
        """Exception text must be sanitized in the log record for RPC buffer safety."""
        try:
            patch_logging()
            logger_test = logging.getLogger("test.exc_text_buffer")
            logger_test.setLevel(logging.DEBUG)

            # Create a handler that captures records (like FTBufferingHandler)
            import io
            handler = logging.handlers.MemoryHandler(capacity=100)
            logger_test.addHandler(handler)

            key = "0x" + "ef" * 32
            try:
                raise ValueError(f"Transaction failed with key {key}")
            except ValueError:
                logger_test.exception("Error occurred")

            # Check the buffered record's exc_text
            assert len(handler.buffer) > 0
            record = handler.buffer[-1]
            # Force exc_text generation if not yet set
            if record.exc_text is None:
                formatter = logging.Formatter()
                formatter.format(record)
            assert key not in (record.exc_text or ""), \
                f"Raw key leaked in exc_text: {record.exc_text}"

            logger_test.removeHandler(handler)
        finally:
            unpatch_logging()

    def test_dict_args_with_named_format_preserved(self):
        """Dict args for %(name)s formatting must stay as dicts, not become tuples."""
        try:
            patch_logging()
            # Construct record with args=None, then set args after to avoid
            # Python 3.12 LogRecord.__init__ bug with single-key dict args
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="Config: %(config)s",
                args=None,
                exc_info=None,
            )
            record.args = {"config": "some_value"}
            # The record should still format correctly
            formatter = logging.Formatter("%(message)s")
            message = formatter.format(record)
            assert "some_value" in message
            # Args must still be a dict
            assert isinstance(record.args, dict)
        finally:
            unpatch_logging()


class TestPatchNotebook:
    """Tests for notebook patching (stdout/stderr channels)."""

    def test_patch_notebook_noop_without_ipython(self):
        """patch_notebook is a no-op when IPython is not running."""
        patch_notebook()
        # Should not raise, and should not be marked as patched
        # (unless we're actually in IPython, which we're not in pytest)
        # Just verify it doesn't crash
        unpatch_notebook()

    def test_patch_notebook_idempotent(self):
        """Calling patch_notebook multiple times is safe."""
        patch_notebook()
        patch_notebook()
        unpatch_notebook()

    def test_sanitize_text_through_stdout_mock(self):
        """Verify sanitize_text works on stdout-like content."""
        key = "0x" + "cd" * 32
        output = f"Debug: wallet key is {key}"
        sanitized = sanitize_text(output)
        assert key not in sanitized
        assert "[REDACTED-KEY]" in sanitized
