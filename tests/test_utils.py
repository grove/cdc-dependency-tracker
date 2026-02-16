"""Tests for utility functions."""

import pytest
from cdc_dependency_tracker.utils import derive_id_column_name, derive_tracking_table_name


class TestDeriveIdColumnName:
    """Test ID column name derivation from table names."""

    def test_simple_plural(self):
        """Test simple plural → singular conversion."""
        assert derive_id_column_name("customers") == "customer_id"
        assert derive_id_column_name("users") == "user_id"
        assert derive_id_column_name("orders") == "order_id"
        assert derive_id_column_name("products") == "product_id"

    def test_irregular_plurals(self):
        """Test irregular plural forms."""
        assert derive_id_column_name("people") == "person_id"
        assert derive_id_column_name("children") == "child_id"
        assert derive_id_column_name("geese") == "goose_id"
        assert derive_id_column_name("teeth") == "tooth_id"

    def test_singular_names(self):
        """Test already singular table names."""
        assert derive_id_column_name("user") == "user_id"
        assert derive_id_column_name("account") == "account_id"
        assert derive_id_column_name("profile") == "profile_id"

    def test_compound_names(self):
        """Test compound table names with underscores."""
        assert derive_id_column_name("order_lines") == "order_line_id"
        assert derive_id_column_name("user_profiles") == "user_profile_id"
        assert derive_id_column_name("product_categories") == "product_category_id"

    def test_names_ending_in_ies(self):
        """Test names ending in 'ies' (categories → category)."""
        assert derive_id_column_name("categories") == "category_id"
        assert derive_id_column_name("companies") == "company_id"
        assert derive_id_column_name("entries") == "entry_id"

    def test_names_ending_in_ss(self):
        """Test names ending in 'ss' (likely singular)."""
        assert derive_id_column_name("address") == "address_id"
        assert derive_id_column_name("class") == "class_id"
        assert derive_id_column_name("progress") == "progress_id"

    def test_names_ending_in_us(self):
        """Test names ending in 'us' (likely singular)."""
        assert derive_id_column_name("status") == "status_id"
        assert derive_id_column_name("bonus") == "bonus_id"

    def test_empty_name_raises_error(self):
        """Test that empty table name raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            derive_id_column_name("")

    def test_single_character(self):
        """Test single character table name."""
        assert derive_id_column_name("x") == "x_id"
        assert derive_id_column_name("a") == "a_id"


class TestDeriveTrackingTableName:
    """Test tracking table name derivation."""

    def test_default_suffix(self):
        """Test with default 'to_reprocess' suffix."""
        assert derive_tracking_table_name("customers") == "customers_to_reprocess"
        assert derive_tracking_table_name("users") == "users_to_reprocess"
        assert derive_tracking_table_name("orders") == "orders_to_reprocess"

    def test_custom_suffix(self):
        """Test with custom suffix."""
        assert derive_tracking_table_name("customers", "pending") == "customers_pending"
        assert derive_tracking_table_name("users", "queue") == "users_queue"
        assert derive_tracking_table_name("orders", "dirty") == "orders_dirty"

    def test_empty_base_table_raises_error(self):
        """Test that empty base table raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            derive_tracking_table_name("")

    def test_compound_names(self):
        """Test with compound table names."""
        assert derive_tracking_table_name("order_lines") == "order_lines_to_reprocess"
        assert derive_tracking_table_name("user_profiles", "sync") == "user_profiles_sync"
