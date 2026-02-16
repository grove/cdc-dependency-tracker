"""CDC event handler for representing change data capture events."""

from typing import Dict, Optional, Any
from dataclasses import dataclass


@dataclass
class CDCEvent:
    """Represents a CDC (Change Data Capture) event from PostgreSQL logical replication."""

    table: str
    schema: str
    operation: str  # 'c', 'u', 'd', 'r', 't'
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    source: Dict[str, Any]
    ts_ms: int

    @property
    def is_create(self) -> bool:
        return self.operation in ("c", "r")

    @property
    def is_update(self) -> bool:
        return self.operation == "u"

    @property
    def is_delete(self) -> bool:
        return self.operation == "d"

    @property
    def is_truncate(self) -> bool:
        return self.operation == "t"

    def get_id_field(self, id_field: str = "_id") -> Optional[str]:
        """Get the ID from the appropriate before/after state."""
        if self.is_delete and self.before:
            return self.before.get(id_field)
        elif self.after:
            return self.after.get(id_field)
        return None

    def __repr__(self) -> str:
        op_name = {"c": "CREATE", "u": "UPDATE", "d": "DELETE", "r": "READ", "t": "TRUNCATE"}.get(
            self.operation, self.operation
        )

        id_val = self.get_id_field()
        return f"CDCEvent({self.schema}.{self.table}.{op_name} id={id_val})"
