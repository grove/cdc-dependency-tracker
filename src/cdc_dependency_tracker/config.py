"""Configuration management using Pydantic."""

from typing import Optional
from pydantic import BaseModel, Field, field_validator
import yaml
from pathlib import Path


class DatabaseConfig(BaseModel):
    """Database connection configuration."""
    
    host: str = Field(default="localhost")
    port: int = Field(default=5432, ge=1, le=65535)
    dbname: str
    user: str
    password: str
    
    def to_connection_params(self) -> dict:
        """Convert to psycopg2 connection parameters."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }


class TrackerConfig(BaseModel):
    """Tracker configuration for dependency resolution."""
    
    base_table: str
    tracking_table: str
    immediate_fanout_threshold: int = Field(default=100, ge=1)
    percolation_interval_seconds: int = Field(default=30, ge=1)
    percolation_batch_size: int = Field(default=1000, ge=1)
    sql_query: str
    
    @field_validator("sql_query")
    @classmethod
    def validate_sql_query(cls, v: str) -> str:
        """Ensure SQL query is not empty."""
        if not v or not v.strip():
            raise ValueError("sql_query cannot be empty")
        return v.strip()


class ReplicationConfig(BaseModel):
    """PostgreSQL logical replication configuration."""
    
    enabled: bool = Field(default=False)
    slot_name: str = Field(default="cdc_slot")
    plugin: str = Field(default="pgoutput")
    
    # Separate connection for replication (requires REPLICATION privilege)
    host: str = Field(default="localhost")
    port: int = Field(default=5432, ge=1, le=65535)
    dbname: str
    user: str
    password: str
    
    # Auto-creation settings
    auto_create_slot: bool = Field(default=True)
    auto_create_publication: bool = Field(default=True)
    publication_name: str = Field(default="cdc_pub")
    
    # Processing settings
    ack_interval_seconds: int = Field(default=10, ge=1)
    max_batch_size: int = Field(default=100, ge=1)
    
    def to_connection_params(self) -> dict:
        """Convert to psycopg2 connection parameters."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }


class Config(BaseModel):
    """Main configuration model."""
    
    database: DatabaseConfig
    tracker: TrackerConfig
    replication: Optional[ReplicationConfig] = None
    
    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        """Load configuration from YAML file."""
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)
        
        return cls(**data)
    
    def __repr__(self) -> str:
        """Safe representation without password."""
        repl_status = "enabled" if self.replication and self.replication.enabled else "disabled"
        return (
            f"Config(database={self.database.host}:{self.database.port}/"
            f"{self.database.dbname}, base_table={self.tracker.base_table}, "
            f"replication={repl_status})"
        )
