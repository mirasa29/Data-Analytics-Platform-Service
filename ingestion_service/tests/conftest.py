#!/usr/bin/env python3

"""
Make sure fixtures are available for all tests
"""

pytest_plugins = [
    "tests.fixtures.test_fixtures"
]
