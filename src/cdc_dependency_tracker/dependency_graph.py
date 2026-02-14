"""Dependency graph for tracking table relationships."""

from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass


@dataclass
class JoinPath:
    """Represents a join relationship between two tables."""
    
    to_table: str
    from_col: str  # Column in the current table
    to_col: str    # Column in the target table
    
    def __repr__(self) -> str:
        return f"→ {self.to_table} ON {self.from_col}={self.to_table}.{self.to_col}"


class DependencyGraph:
    """Graph representing table dependencies and join paths."""
    
    def __init__(self, base_table: str, join_graph: Dict[str, List[Tuple[str, str, str]]]):
        self.base_table = base_table
        self.graph = join_graph
        self._depths: Dict[str, int] = {}
        self._join_keys_cache: Dict[str, Set[str]] = {}
        self._compute_depths()
    
    def _compute_depths(self) -> None:
        """Compute depth (distance) of each table from base table."""
        self._depths[self.base_table] = 0
        queue = [(self.base_table, 0)]
        visited = {self.base_table}
        
        while queue:
            current, depth = queue.pop(0)
            
            if current in self.graph:
                for to_table, _, _ in self.graph[current]:
                    if to_table not in visited:
                        visited.add(to_table)
                        self._depths[to_table] = depth + 1
                        queue.append((to_table, depth + 1))
    
    def get_depth(self, table: str) -> int:
        """Get the depth (hop distance) of a table from base table."""
        return self._depths.get(table, -1)
    
    def get_join_keys_for_table(self, table: str) -> Set[str]:
        """Get all columns in a table that are used in joins."""
        if table in self._join_keys_cache:
            return self._join_keys_cache[table]
        
        join_keys = set()
        
        if table in self.graph:
            for _, from_col, _ in self.graph[table]:
                join_keys.add(from_col)
        
        self._join_keys_cache[table] = join_keys
        return join_keys
    
    def get_parent_join(self, table: str) -> Optional[Tuple[str, str, str]]:
        """
        Get the parent join (one hop closer to base table).
        
        Returns:
            (parent_table, join_col_in_table, join_col_in_parent) or None
        """
        current_depth = self.get_depth(table)
        if current_depth <= 0:
            return None
        
        target_depth = current_depth - 1
        
        if table in self.graph:
            for to_table, from_col, to_col in self.graph[table]:
                if self.get_depth(to_table) == target_depth:
                    return (to_table, from_col, to_col)
        
        return None
    
    def get_path_to_base(self, from_table: str) -> List[JoinPath]:
        """
        Get the join path from a table back to the base table.
        
        Returns:
            List of JoinPath objects describing the route
        """
        if from_table == self.base_table:
            return []
        
        path = []
        current = from_table
        
        while current != self.base_table:
            parent_info = self.get_parent_join(current)
            if not parent_info:
                raise ValueError(f"No path from {from_table} to {self.base_table}")
            
            parent_table, from_col, to_col = parent_info
            path.append(JoinPath(parent_table, from_col, to_col))
            current = parent_table
        
        return path
    
    def is_join_key(self, table: str, column: str) -> bool:
        """Check if a column is used in any join for this table."""
        return column in self.get_join_keys_for_table(table)
    
    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the dependency graph."""
        return list(self._depths.keys())
    
    def __repr__(self) -> str:
        return f"DependencyGraph(base={self.base_table}, tables={list(self._depths.keys())})"
