"""SQL parser to extract JOIN relationships from queries."""

from typing import Dict, List, Tuple, Set, Optional
import sqlglot
from sqlglot import exp


class JoinCriteria:
    """Represents a join between two tables."""

    def __init__(self, from_table: str, to_table: str, from_col: str, to_col: str):
        self.from_table = from_table
        self.to_table = to_table
        self.from_col = from_col
        self.to_col = to_col

    def __repr__(self) -> str:
        return f"{self.from_table}.{self.from_col} = {self.to_table}.{self.to_col}"


class SQLParser:
    """Parse SQL queries to extract join relationships."""

    def __init__(self, sql_query: str, base_table: Optional[str] = None):
        self.sql_query = sql_query
        self.base_table = base_table
        self.parsed = sqlglot.parse_one(sql_query, dialect="postgres")
        self.table_aliases: Dict[str, str] = {}
        self.joins: List[JoinCriteria] = []
        self.base_id_column: Optional[str] = None
        self._parse()

    def _parse(self) -> None:
        """Parse the SQL query and extract joins."""
        # Extract table aliases
        self._extract_table_aliases()

        # Extract join conditions
        self._extract_joins()

        # Extract base table ID column if base_table specified
        if self.base_table:
            self.base_id_column = self._extract_base_id_column()

    def _extract_table_aliases(self) -> None:
        """Build mapping of alias -> table name."""
        for table in self.parsed.find_all(exp.Table):
            table_name = table.name
            alias = table.alias_or_name
            self.table_aliases[alias] = table_name

    def _extract_joins(self) -> None:
        """Extract join conditions from ON clauses."""
        # Extract explicit JOINs - ON condition is in args['on']
        for join in self.parsed.find_all(exp.Join):
            if "on" in join.args and join.args["on"]:
                self._process_join_condition(join.args["on"])

    def _process_join_condition(self, condition: exp.Expression) -> None:
        """Process a single join condition."""
        # Handle equality conditions: table1.col1 = table2.col2
        if isinstance(condition, exp.EQ):
            # sqlglot uses 'this' for left and 'expression' for right in EQ
            left = condition.this if hasattr(condition, "this") else condition.left
            right = condition.expression if hasattr(condition, "expression") else condition.right

            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                left_table = self._resolve_table_alias(str(left.table))
                left_col = str(left.name)
                right_table = self._resolve_table_alias(str(right.table))
                right_col = str(right.name)

                if left_table and right_table:
                    self.joins.append(JoinCriteria(left_table, right_table, left_col, right_col))

        # Handle AND conditions recursively
        elif isinstance(condition, exp.And):
            self._process_join_condition(condition.left)
            self._process_join_condition(condition.right)

    def _resolve_table_alias(self, alias: str) -> str:
        """Resolve table alias to actual table name."""
        return self.table_aliases.get(alias, alias)

    def _extract_base_id_column(self) -> Optional[str]:
        """Extract the ID column alias for base table from SELECT clause."""
        if not self.base_table:
            return None

        # Find the alias used for base table
        base_alias = None
        for alias, table_name in self.table_aliases.items():
            if table_name == self.base_table:
                base_alias = alias
                break

        if not base_alias:
            return None

        # Find SELECT column from base table that looks like an ID
        # Look for patterns: _id, id (most common primary key column names)
        for select in self.parsed.find_all(exp.Select):
            for projection in select.expressions:
                # Check if this is an aliased column (e.g., c._id as base_id)
                if isinstance(projection, exp.Alias):
                    column = projection.this
                    if isinstance(column, exp.Column):
                        if str(column.table) == base_alias and str(column.name) in ("_id", "id"):
                            return str(projection.alias)
                # Check if this is an unaliased column (e.g., c._id)
                elif isinstance(projection, exp.Column):
                    if str(projection.table) == base_alias and str(projection.name) in (
                        "_id",
                        "id",
                    ):
                        return str(projection.name)

        return None

    def get_joins(self) -> List[JoinCriteria]:
        """Get all extracted join criteria."""
        return self.joins

    def get_tables(self) -> Set[str]:
        """Get all tables involved in the query."""
        return set(self.table_aliases.values())

    def get_base_id_column(self) -> Optional[str]:
        """Get the extracted base table ID column name."""
        return self.base_id_column

    def get_join_graph(self) -> Dict[str, List[Tuple[str, str, str]]]:
        """
        Build a directed graph of join relationships.

        Returns:
            Dict mapping table -> [(to_table, from_col, to_col), ...]
        """
        graph: Dict[str, List[Tuple[str, str, str]]] = {}

        for join in self.joins:
            # Forward edge
            if join.from_table not in graph:
                graph[join.from_table] = []
            graph[join.from_table].append((join.to_table, join.from_col, join.to_col))

            # Reverse edge (for backward traversal)
            if join.to_table not in graph:
                graph[join.to_table] = []
            graph[join.to_table].append((join.from_table, join.to_col, join.from_col))

        return graph
