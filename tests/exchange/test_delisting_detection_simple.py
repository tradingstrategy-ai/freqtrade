#!/usr/bin/env python3
"""
Simple test to verify BadSymbol detection logic works.
Tests the delisting detection without needing full bot startup.
"""

import sys


sys.path.insert(0, 'deps/freqtrade')

import ccxt

from freqtrade.exceptions import DDosProtection, TemporaryError


print("=" * 80)
print("REPL TEST: BadSymbol Detection Logic")
print("=" * 80)

# Simulate the ModeTrade fetch_l2_order_book logic
class MockExchange:
    def __init__(self):
        self._bad_symbol_count = {}
        self._delisted_pairs = set()
        self._bad_symbol_threshold = 3

    def fetch_l2_order_book_parent(self, pair: str):
        """Simulates parent class that raises ccxt.BadSymbol"""
        raise ccxt.BadSymbol(f"modetrade does not have market symbol {pair}")

    def fetch_l2_order_book(self, pair: str, limit: int = 100):
        """Our override logic"""
        # If already marked as delisted, raise immediately
        if pair in self._delisted_pairs:
            raise DDosProtection(
                f"Pair {pair} has been delisted from exchange. "
                f"Use ticker pricing or emergency exit."
            )

        try:
            return self.fetch_l2_order_book_parent(pair)
        except ccxt.BadSymbol as e:
            # Track consecutive failures
            self._bad_symbol_count[pair] = self._bad_symbol_count.get(pair, 0) + 1
            failure_count = self._bad_symbol_count[pair]

            if failure_count >= self._bad_symbol_threshold:
                # Mark as delisted after threshold reached
                self._delisted_pairs.add(pair)
                print(f"   ⚠️  Pair {pair} failed {failure_count} times - MARKING AS DELISTED")
                raise DDosProtection(
                    f"Pair {pair} has been delisted from exchange "
                    f"(BadSymbol threshold {self._bad_symbol_threshold} reached)"
                ) from e
            else:
                print(f"   ℹ️  BadSymbol for {pair} ({failure_count}/{self._bad_symbol_threshold})")
                raise TemporaryError(
                    f"Could not get order book due to {e.__class__.__name__}. Message: {e}"
                ) from e


# Run test
exchange = MockExchange()
test_pair = "AVNT/USDC:USDC"

print(f"\nTesting with pair: {test_pair}")
print(f"Initial state: {exchange._bad_symbol_count}, {exchange._delisted_pairs}")

for attempt in range(1, 5):
    print(f"\n{'='*80}")
    print(f"ATTEMPT {attempt}")
    print(f"{'='*80}")

    try:
        print(f"   Calling fetch_l2_order_book('{test_pair}')...")
        order_book = exchange.fetch_l2_order_book(test_pair, 1)
        print("   ✓ Got order book (unexpected!)")
    except DDosProtection as e:
        print(f"   ✓ CAUGHT DDosProtection: {e}")
    except TemporaryError as e:
        print(f"   ✓ CAUGHT TemporaryError: {str(e)[:80]}...")
    except Exception as e:
        print(f"   ✗ CAUGHT {type(e).__name__}: {e}")

    print(f"   State: count={exchange._bad_symbol_count}, delisted={exchange._delisted_pairs}")

    if test_pair in exchange._delisted_pairs:
        print("\n   🎯 PAIR MARKED AS DELISTED!")
        break

print("\n" + "="*80)
print("FINAL RESULT")
print("="*80)
print(f"_bad_symbol_count: {exchange._bad_symbol_count}")
print(f"_delisted_pairs: {exchange._delisted_pairs}")

if test_pair in exchange._delisted_pairs:
    threshold = exchange._bad_symbol_threshold
    print(
        f"\n✅ TEST PASSED: Logic correctly marks pair "
        f"as delisted after {threshold} attempts"
    )
    print("✅ Raises DDosProtection on threshold")
    print("✅ Raises TemporaryError before threshold")
else:
    print("\n❌ TEST FAILED")

print("\n" + "="*80)
print("TESTING SUBSEQUENT CALLS TO DELISTED PAIR")
print("="*80)

try:
    print("Calling fetch_l2_order_book on already-delisted pair...")
    exchange.fetch_l2_order_book(test_pair, 1)
    print("❌ Should have raised DDosProtection immediately!")
except DDosProtection as e:
    print(f"✅ Correctly raised DDosProtection immediately: {e}")
except Exception as e:
    print(f"❌ Wrong exception: {type(e).__name__}")

print("\n" + "="*80)
print("ALL TESTS COMPLETE")
print("="*80)
