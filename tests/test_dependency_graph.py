"""Tests for dependency graph."""

from cdc_dependency_tracker.dependency_graph import DependencyGraph


def test_dependency_graph_depths():
    """Test depth calculation from base table."""
    # Mock join graph: customers -> orders -> order_lines -> products
    join_graph = {
        "customers": [("orders", "_id", "cust_id")],
        "orders": [("customers", "cust_id", "_id"), ("order_lines", "_id", "order_id")],  # reverse
        "order_lines": [
            ("orders", "order_id", "_id"),  # reverse
            ("products", "product_id", "_id"),
        ],
        "products": [("order_lines", "_id", "product_id")],  # reverse
    }

    graph = DependencyGraph("customers", join_graph)

    assert graph.get_depth("customers") == 0
    assert graph.get_depth("orders") == 1
    assert graph.get_depth("order_lines") == 2
    assert graph.get_depth("products") == 3


def test_get_join_keys():
    """Test extraction of join key columns."""
    join_graph = {
        "customers": [("orders", "_id", "cust_id")],
        "orders": [("customers", "cust_id", "_id"), ("order_lines", "_id", "order_id")],
    }

    graph = DependencyGraph("customers", join_graph)

    # Orders has 'cust_id' and '_id' as join keys
    join_keys = graph.get_join_keys_for_table("orders")
    assert "cust_id" in join_keys
    assert "_id" in join_keys


def test_get_parent_join():
    """Test finding parent join (one hop closer to base)."""
    join_graph = {
        "customers": [("orders", "_id", "cust_id")],
        "orders": [("customers", "cust_id", "_id"), ("order_lines", "_id", "order_id")],
        "order_lines": [("orders", "order_id", "_id")],
    }

    graph = DependencyGraph("customers", join_graph)

    # Order lines -> orders
    parent = graph.get_parent_join("order_lines")
    assert parent is not None
    assert parent[0] == "orders"  # parent table

    # Orders -> customers
    parent = graph.get_parent_join("orders")
    assert parent is not None
    assert parent[0] == "customers"

    # Customers has no parent
    parent = graph.get_parent_join("customers")
    assert parent is None


def test_is_join_key():
    """Test checking if column is a join key."""
    join_graph = {
        "orders": [("customers", "cust_id", "_id"), ("order_lines", "_id", "order_id")],
    }

    graph = DependencyGraph("customers", join_graph)

    assert graph.is_join_key("orders", "cust_id")
    assert graph.is_join_key("orders", "_id")
    assert not graph.is_join_key("orders", "order_date")
