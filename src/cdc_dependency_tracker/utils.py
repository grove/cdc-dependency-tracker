"""Utility functions for CDC dependency tracker."""

# Mapping for irregular plural forms
IRREGULAR_PLURALS = {
    "people": "person",
    "children": "child",
    "geese": "goose",
    "teeth": "tooth",
    "feet": "foot",
    "mice": "mouse",
    "men": "man",
    "women": "woman",
}


def derive_id_column_name(table_name: str) -> str:
    """
    Derive ID column name from table name using common conventions.

    Handles various naming patterns:
    - Plural ending in 's': customers → customer_id
    - Irregular plurals: people → person_id
    - Already singular: user → user_id
    - Ends with 's' but singular: address → address_id (kept as-is)

    Args:
        table_name: Name of the database table

    Returns:
        Derived ID column name (e.g., "customer_id")

    Examples:
        >>> derive_id_column_name("customers")
        'customer_id'
        >>> derive_id_column_name("users")
        'user_id'
        >>> derive_id_column_name("people")
        'person_id'
        >>> derive_id_column_name("address")
        'address_id'
        >>> derive_id_column_name("order_lines")
        'order_line_id'
    """
    if not table_name:
        raise ValueError("table_name cannot be empty")

    # Check for irregular plurals first
    if table_name.lower() in IRREGULAR_PLURALS:
        singular = IRREGULAR_PLURALS[table_name.lower()]
        return f"{singular}_id"

    # Handle compound names with underscores (e.g., order_lines)
    if "_" in table_name:
        parts = table_name.split("_")
        # Singularize last part
        last_part = parts[-1]
        if last_part.endswith("ies") and len(last_part) > 3:
            # Handle 'ies' ending (categories → category)
            parts[-1] = last_part[:-3] + "y"
        elif last_part.endswith("s") and len(last_part) > 1:
            # Check if it's likely plural (not ending in 'ss', 'us', etc.)
            if not last_part.endswith(("ss", "us", "is")):
                parts[-1] = last_part[:-1]
        return "_".join(parts) + "_id"

    # Simple plural: ends with 's' and longer than 1 char
    if table_name.endswith("s") and len(table_name) > 1:
        # Don't remove 's' from words ending in 'ss', 'us', 'is' (likely singular)
        if not table_name.endswith(("ss", "us", "is")):
            # Handle special case: ends with 'ies' (e.g., categories → category)
            if table_name.endswith("ies") and len(table_name) > 3:
                singular = table_name[:-3] + "y"
                return f"{singular}_id"
            # Standard plural: just remove 's'
            singular = table_name[:-1]
            return f"{singular}_id"

    # Already singular or unknown pattern: use as-is
    return f"{table_name}_id"


def derive_tracking_table_name(base_table: str, suffix: str = "to_reprocess") -> str:
    """
    Derive tracking table name from base table name.

    Args:
        base_table: Name of the base table
        suffix: Suffix to add (default: "to_reprocess")

    Returns:
        Tracking table name (e.g., "customers_to_reprocess")

    Examples:
        >>> derive_tracking_table_name("customers")
        'customers_to_reprocess'
        >>> derive_tracking_table_name("users", "pending")
        'users_pending'
    """
    if not base_table:
        raise ValueError("base_table cannot be empty")

    return f"{base_table}_{suffix}"
