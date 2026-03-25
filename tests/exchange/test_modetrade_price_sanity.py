"""Unit tests for ModeTrade price sanity check logic"""

import pytest

from freqtrade.exchange.modetrade import Modetrade


class TestModeTradePriceSanity:
    """Test ModeTrade price sanity check methods"""

    def test_rel_deviation_basic(self):
        """Test relative deviation calculation with basic cases"""
        # Same values should give 0% deviation
        assert Modetrade._rel_deviation(100.0, 100.0) == 0.0

        # 1% deviation
        assert abs(Modetrade._rel_deviation(101.0, 100.0) - 0.01) < 0.0001

        # 3% deviation
        assert abs(Modetrade._rel_deviation(103.0, 100.0) - 0.03) < 0.0001

        # 5% deviation
        assert abs(Modetrade._rel_deviation(105.0, 100.0) - 0.05) < 0.0001

    def test_rel_deviation_negative(self):
        """Test relative deviation with negative deviations"""
        # Deviation should be absolute (positive)
        assert abs(Modetrade._rel_deviation(97.0, 100.0) - 0.03) < 0.0001
        assert abs(Modetrade._rel_deviation(95.0, 100.0) - 0.05) < 0.0001

    def test_rel_deviation_zero_reference(self):
        """Test relative deviation with zero reference"""
        # Should return infinity when reference is zero
        assert Modetrade._rel_deviation(100.0, 0.0) == float("inf")

    def test_rel_deviation_real_prices(self):
        """Test relative deviation with real price examples from logs"""
        # From logs: ob=1.184000 idx=1.184000 -> dev=0.00%
        dev = Modetrade._rel_deviation(1.184000, 1.184000)
        assert dev < 0.0001  # Should be ~0%

        # From logs: ob=76.480000 idx=76.510000 -> dev=0.04%
        dev = Modetrade._rel_deviation(76.480000, 76.510000)
        assert abs(dev - 0.0004) < 0.0001  # Should be ~0.04%

        # From logs: ob=4438.200000 idx=4442.100000 -> dev=0.09%
        dev = Modetrade._rel_deviation(4438.200000, 4442.100000)
        assert abs(dev - 0.0009) < 0.0001  # Should be ~0.09%

    def test_ticker_ref_extraction(self):
        """Test extraction of index/mark prices from ticker"""
        # Valid ticker with both index and mark
        ticker = {
            "info": {
                "index_price": "1.184000",
                "mark_price": "1.183000"
            }
        }
        idx, mark = Modetrade._ticker_ref(ticker)
        assert idx == 1.184000
        assert mark == 1.183000

    def test_ticker_ref_missing_info(self):
        """Test ticker_ref with missing info"""
        # No info field
        ticker = {"other": "data"}
        idx, mark = Modetrade._ticker_ref(ticker)
        assert idx is None
        assert mark is None

        # Not a dict
        idx, mark = Modetrade._ticker_ref(None)
        assert idx is None
        assert mark is None

    def test_ticker_ref_invalid_prices(self):
        """Test ticker_ref with invalid price values"""
        # Non-numeric prices
        ticker = {
            "info": {
                "index_price": "invalid",
                "mark_price": "also_invalid"
            }
        }
        idx, mark = Modetrade._ticker_ref(ticker)
        assert idx is None
        assert mark is None

    def test_ticker_ref_partial_data(self):
        """Test ticker_ref with only one price available"""
        # Only index
        ticker = {
            "info": {
                "index_price": "1.184000"
            }
        }
        idx, mark = Modetrade._ticker_ref(ticker)
        assert idx == 1.184000
        assert mark is None

        # Only mark
        ticker = {
            "info": {
                "mark_price": "1.183000"
            }
        }
        idx, mark = Modetrade._ticker_ref(ticker)
        assert idx is None
        assert mark == 1.183000

    def test_ob_top_extraction(self):
        """Test extraction of top-of-book bid/ask"""
        # Valid order book
        order_book = {
            "bids": [[76.480, 10.5], [76.470, 5.2]],
            "asks": [[76.520, 8.3], [76.530, 12.1]]
        }
        bid, ask = Modetrade._ob_top(order_book)
        assert bid == 76.480
        assert ask == 76.520

    def test_ob_top_empty(self):
        """Test ob_top with empty order book"""
        # Empty bids/asks
        order_book = {"bids": [], "asks": []}
        bid, ask = Modetrade._ob_top(order_book)
        assert bid is None
        assert ask is None

        # No bids/asks fields
        order_book = {}
        bid, ask = Modetrade._ob_top(order_book)
        assert bid is None
        assert ask is None

        # Not a dict
        bid, ask = Modetrade._ob_top(None)
        assert bid is None
        assert ask is None

    def test_ob_top_invalid_data(self):
        """Test ob_top with invalid data structures"""
        # Invalid bid/ask format
        order_book = {
            "bids": [["invalid", "data"]],
            "asks": [["also", "invalid"]]
        }
        bid, ask = Modetrade._ob_top(order_book)
        assert bid is None
        assert ask is None

    def test_price_sanity_cfg_defaults(self):
        """Test price sanity config with defaults"""
        class MockExchange(Modetrade):
            def __init__(self):
                self._config = {}

        exchange = MockExchange()
        cfg = exchange._modetrade_price_sanity_cfg()

        assert cfg["enabled"] is True
        assert cfg["max_deviation_ratio"] == 0.03
        assert cfg["log_level"] == "warning"

    def test_price_sanity_cfg_custom(self):
        """Test price sanity config with custom values"""
        class MockExchange(Modetrade):
            def __init__(self):
                self._config = {
                    "price_sanity_check_settings": {
                        "enabled": False,
                        "max_deviation_ratio": 0.05,
                        "log_level": "info"
                    }
                }

        exchange = MockExchange()
        cfg = exchange._modetrade_price_sanity_cfg()

        assert cfg["enabled"] is False
        assert cfg["max_deviation_ratio"] == 0.05
        assert cfg["log_level"] == "info"

    def test_price_sanity_cfg_backward_compat(self):
        """Test backward compatibility with modetrade_price_sanity key"""
        class MockExchange(Modetrade):
            def __init__(self):
                self._config = {
                    "modetrade_price_sanity": {
                        "enabled": True,
                        "max_deviation_ratio": 0.02,
                        "log_level": "debug"
                    }
                }

        exchange = MockExchange()
        cfg = exchange._modetrade_price_sanity_cfg()

        assert cfg["enabled"] is True
        assert cfg["max_deviation_ratio"] == 0.02
        assert cfg["log_level"] == "debug"

    def test_price_deviation_decision_logic(self):
        """Test the decision logic for price selection"""
        # Scenario from logs: dev=0.00% max=3.0% -> use_orderbook
        ob_rate = 1.184000
        ref_price = 1.184000
        max_dev = 0.03

        deviation = Modetrade._rel_deviation(ob_rate, ref_price)
        should_use_ob = deviation <= max_dev
        assert should_use_ob is True

        # Scenario: dev=0.09% max=3.0% -> use_orderbook (within threshold)
        ob_rate = 4438.200000
        ref_price = 4442.100000
        max_dev = 0.03

        deviation = Modetrade._rel_deviation(ob_rate, ref_price)
        should_use_ob = deviation <= max_dev
        assert should_use_ob is True  # 0.09% < 3.0%

        # Scenario: dev=5% max=3.0% -> use_index (exceeds threshold)
        ob_rate = 100.0
        ref_price = 105.0
        max_dev = 0.03

        deviation = Modetrade._rel_deviation(ob_rate, ref_price)
        should_use_ob = deviation <= max_dev
        assert should_use_ob is False  # 5% > 3.0%

    def test_real_log_examples(self):
        """Test with actual examples from the logs"""
        # Example 1: MORPHO/USDC:USDC
        # ob=1.184000 idx=1.184000 mark=1.183000 → chose=1.184000 | dev=0.00%
        ob = 1.184000
        idx = 1.184000
        mark = 1.183000

        # Should use index as reference (preferred over mark)
        ref = idx if idx is not None else mark
        assert ref == 1.184000

        dev = Modetrade._rel_deviation(ob, ref)
        assert dev < 0.01  # Should be very small
        assert dev <= 0.03  # Within threshold, use OB

        # Example 2: XAG/USDC:USDC
        # ob=76.480000 idx=76.510000 mark=76.480000 → chose=76.480000 | dev=0.04%
        ob = 76.480000
        idx = 76.510000
        mark = 76.480000

        ref = idx if idx is not None else mark
        assert ref == 76.510000

        dev = Modetrade._rel_deviation(ob, ref)
        assert abs(dev - 0.0004) < 0.0001  # ~0.04%
        assert dev <= 0.03  # Within threshold, use OB

        # Example 3: XAU/USDC:USDC
        # ob=4438.200000 idx=4442.100000 mark=4438.100000 → chose=4438.200000 | dev=0.09%
        ob = 4438.200000
        idx = 4442.100000
        mark = 4438.100000

        ref = idx if idx is not None else mark
        assert ref == 4442.100000

        dev = Modetrade._rel_deviation(ob, ref)
        assert abs(dev - 0.0009) < 0.0001  # ~0.09%
        assert dev <= 0.03  # Within threshold, use OB


class TestModeTradeDelistingDetection:
    """Test ModeTrade delisting detection logic"""

    def test_badsymbol_tracking_increments(self):
        """Test that BadSymbol failures are tracked correctly"""
        from unittest.mock import patch

        import ccxt

        from freqtrade.exceptions import DDosProtection, TemporaryError

        # Create mock exchange instance
        modetrade = Modetrade({'name': 'modetrade', 'dry_run': True})
        test_pair = "AVNT/USDC:USDC"

        # Initially should be empty
        assert test_pair not in modetrade._bad_symbol_count
        assert test_pair not in modetrade._delisted_pairs

        # Create a function that returns a new TemporaryError each time (needed for multiple raises)
        def create_temp_error():
            bad_symbol_error = ccxt.BadSymbol(f"modetrade does not have market symbol {test_pair}")
            temp_error = TemporaryError(f"Could not get order book due to BadSymbol. Message: {bad_symbol_error}")
            temp_error.__cause__ = bad_symbol_error
            return temp_error

        # Mock parent's fetch_l2_order_book to raise TemporaryError with BadSymbol cause
        with patch.object(
            Modetrade.__bases__[0], 'fetch_l2_order_book',
            side_effect=lambda pair, limit=None: (_ for _ in ()).throw(create_temp_error())
        ):
            # Attempt 1 - should raise TemporaryError and increment count
            with pytest.raises(TemporaryError):
                modetrade.fetch_l2_order_book(test_pair)
            assert modetrade._bad_symbol_count[test_pair] == 1
            assert test_pair not in modetrade._delisted_pairs

            # Attempt 2 - should raise TemporaryError and increment count
            with pytest.raises(TemporaryError):
                modetrade.fetch_l2_order_book(test_pair)
            assert modetrade._bad_symbol_count[test_pair] == 2
            assert test_pair not in modetrade._delisted_pairs

            # Attempt 3 - should mark as delisted and raise DDosProtection
            with pytest.raises(DDosProtection) as exc_info:
                modetrade.fetch_l2_order_book(test_pair)
            assert modetrade._bad_symbol_count[test_pair] == 3
            assert test_pair in modetrade._delisted_pairs
            assert "delisted" in str(exc_info.value).lower()

    def test_delisted_pair_raises_immediately(self):
        """Test that already-delisted pairs raise DDosProtection immediately"""
        from freqtrade.exceptions import DDosProtection

        modetrade = Modetrade({'name': 'modetrade', 'dry_run': True})
        test_pair = "AVNT/USDC:USDC"

        # Manually mark as delisted
        modetrade._delisted_pairs.add(test_pair)

        # Should raise DDosProtection immediately without calling parent
        with pytest.raises(DDosProtection) as exc_info:
            modetrade.fetch_l2_order_book(test_pair)

        assert "delisted" in str(exc_info.value).lower()
        # Counter should not increment (immediate raise)
        assert test_pair not in modetrade._bad_symbol_count

    def test_successful_fetch_resets_counter(self):
        """Test that successful fetch resets the failure counter"""
        from unittest.mock import patch

        import ccxt

        from freqtrade.exceptions import TemporaryError

        modetrade = Modetrade({'name': 'modetrade', 'dry_run': True})
        test_pair = "BTC/USDC:USDC"

        # Fail once
        with patch.object(
            Modetrade.__bases__[0], 'fetch_l2_order_book',
            side_effect=ccxt.BadSymbol("test")
        ):
            with pytest.raises(TemporaryError):
                modetrade.fetch_l2_order_book(test_pair)
            assert modetrade._bad_symbol_count[test_pair] == 1

        # Success - should reset counter
        mock_order_book = {'bids': [[100.0, 1.0]], 'asks': [[101.0, 1.0]]}
        with patch.object(
            Modetrade.__bases__[0], 'fetch_l2_order_book',
            return_value=mock_order_book
        ):
            result = modetrade.fetch_l2_order_book(test_pair)
            assert result == mock_order_book
            # Counter should be removed
            assert test_pair not in modetrade._bad_symbol_count


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
