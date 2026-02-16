"""Tests for SQL parser."""

from cdc_dependency_tracker.sql_parser import SQLParser


def test_sql_parser_basic():
    """Test basic SQL parsing with joins."""
    sql = """
        SELECT 
            c._id as customer_id,
            o._id as order_id
        FROM customers c
        JOIN orders o ON c._id = o.cust_id
        WHERE c._deleted = FALSE
    """

    parser = SQLParser(sql)
    joins = parser.get_joins()

    assert len(joins) > 0
    assert "customers" in parser.get_tables()
    assert "orders" in parser.get_tables()


def test_sql_parser_multi_join():
    """Test parsing with multiple joins."""
    sql = """
        SELECT 
            c._id as customer_id,
            o._id as order_id,
            ol._id as order_line_id,
            p._id as product_id
        FROM customers c
        JOIN orders o ON c._id = o.cust_id
        JOIN order_lines ol ON o._id = ol.order_id
        JOIN products p ON ol.product_id = p._id
        WHERE c._deleted = FALSE
    """

    parser = SQLParser(sql)
    joins = parser.get_joins()
    tables = parser.get_tables()
    join_graph = parser.get_join_graph()

    assert len(joins) >= 3
    assert len(tables) == 4
    assert "customers" in join_graph
    assert "orders" in join_graph
    assert "order_lines" in join_graph
    assert "products" in join_graph


def test_extract_base_id_column_with_alias():
    """Test extracting base table ID column when aliased in SELECT."""
    sql = """
        SELECT 
            c._id as customer_id,
            o._id as order_id
        FROM customers c
        JOIN orders o ON c._id = o.cust_id
    """

    parser = SQLParser(sql, base_table="customers")
    base_id_column = parser.get_base_id_column()

    assert base_id_column == "customer_id"


def test_extract_base_id_column_no_alias():
    """Test extracting base table ID column when not aliased."""
    sql = """
        SELECT 
            c._id,
            o._id as order_id
        FROM customers c
        JOIN orders o ON c._id = o.cust_id
    """

    parser = SQLParser(sql, base_table="customers")
    base_id_column = parser.get_base_id_column()

    assert base_id_column == "_id"


def test_extract_base_id_column_not_in_select():
    """Test when base table ID is not in SELECT clause."""
    sql = """
        SELECT 
            o._id as order_id,
            o.total
        FROM orders o
        JOIN customers c ON c._id = o.cust_id
    """

    parser = SQLParser(sql, base_table="customers")
    base_id_column = parser.get_base_id_column()

    assert base_id_column is None


def test_extract_base_id_column_no_base_table():
    """Test when no base_table is specified."""
    sql = """
        SELECT c._id as customer_id
        FROM customers c
    """

    parser = SQLParser(sql)  # No base_table
    base_id_column = parser.get_base_id_column()

    assert base_id_column is None


def test_extract_base_id_column_irregular_table_name():
    """Test with irregular table name where heuristic would fail."""
    sql = """
        SELECT 
            p._id as person_id,
            a._id as address_id
        FROM people p
        JOIN addresses a ON p._id = a.person_id
    """

    parser = SQLParser(sql, base_table="people")
    base_id_column = parser.get_base_id_column()

    # Should extract "person_id" from SQL regardless of table name irregularity
    assert base_id_column == "person_id"


def test_extract_base_id_column_with_id_not_underscore_id():
    """Test extracting when column is 'id' not '_id'."""
    sql = """
        SELECT 
            u.id as user_id,
            s.id as session_id
        FROM users u
        JOIN sessions s ON u.id = s.user_id
    """

    parser = SQLParser(sql, base_table="users")
    base_id_column = parser.get_base_id_column()

    assert base_id_column == "user_id"
