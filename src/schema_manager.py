#!/usr/bin/env python3
"""
Shared Schema Manager for ML Processes
======================================

This module provides a centralized way to initialize database schemas
for all ML processes (IPR, Declination, Cartas Dinagraficas).

All schemas are maintained in BP010-data-pipelines as the single source of truth.
ML processes should use this module instead of duplicating schema files.

Usage:
    from src.schema_manager import SchemaManager
    
    manager = SchemaManager()
    manager.init_schemas(engine)
    manager.init_stage_tables(engine)
    manager.init_universal_tables(engine)
"""

import logging
from pathlib import Path
from typing import Optional
from sqlalchemy import create_engine, Engine, text

logger = logging.getLogger(__name__)


class SchemaManager:
    """Centralized schema manager for database initialization."""
    
    def __init__(self, schema_base_path: Optional[Path] = None):
        """
        Initialize SchemaManager.
        
        Args:
            schema_base_path: Base path to schema SQL files. If None, auto-detects
                             from BP010-data-pipelines/src/sql/schema/
        """
        if schema_base_path is None:
            # Auto-detect BP010-data-pipelines location
            # This file is in BP010-data-pipelines/src/
            current_file = Path(__file__)
            self.schema_base_path = current_file.parent / "sql" / "schema"
        else:
            self.schema_base_path = Path(schema_base_path)
        
        if not self.schema_base_path.exists():
            raise ValueError(
                f"Schema directory not found: {self.schema_base_path}\n"
                f"Ensure BP010-data-pipelines/src/sql/schema/ exists."
            )
    
    def _read_sql_file(self, file_path: Path) -> str:
        """Read SQL file content."""
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def _execute_sql_statements(self, engine: Engine, sql_content: str, description: str = "SQL statements"):
        """
        Execute SQL statements safely, handling errors gracefully.
        
        Args:
            engine: SQLAlchemy engine
            sql_content: SQL content (may contain multiple statements)
            description: Description for logging
        """
        # Split by semicolon, but preserve multi-line statements
        statements = []
        current_statement = []
        
        for line in sql_content.split('\n'):
            stripped = line.strip()
            # Skip empty lines and comments
            if not stripped or stripped.startswith('--'):
                continue
            
            current_statement.append(line)
            
            # If line ends with semicolon, it's the end of a statement
            if stripped.endswith(';'):
                statement = '\n'.join(current_statement).strip()
                if statement:
                    statements.append(statement)
                current_statement = []
        
        # Add any remaining statement
        if current_statement:
            statement = '\n'.join(current_statement).strip()
            if statement:
                statements.append(statement)
        
        executed = 0
        failed = 0
        
        for i, statement in enumerate(statements, 1):
            if not statement or statement.startswith('--'):
                continue
                
            try:
                with engine.begin() as conn:
                    conn.execute(text(statement))
                executed += 1
                # Log CREATE TABLE statements for visibility
                if 'CREATE TABLE' in statement.upper():
                    # Extract table name from CREATE TABLE statement
                    parts = statement.upper().split('CREATE TABLE')
                    if len(parts) > 1:
                        table_part = parts[1].strip().split()[0]
                        logger.info(f"  ✓ Created table: {table_part}")
            except Exception as e:
                error_str = str(e).lower()
                # Ignore expected errors (already exists, doesn't exist, etc.)
                if any(phrase in error_str for phrase in [
                    "already exists",
                    "does not exist",
                    "cannot drop",
                ]):
                    logger.debug(f"Skipping statement {i} (expected error): {statement[:80]}...")
                elif "current transaction is aborted" in error_str or "in failed sql transaction" in error_str:
                    logger.warning(f"Transaction aborted on statement {i}, rolling back...")
                    failed += 1
                    # Try to continue with next statement in new transaction
                else:
                    logger.error(f"❌ Error executing statement {i} ({description}): {e}")
                    logger.error(f"Failed statement: {statement[:200]}...")
                    failed += 1
        
        if failed > 0:
            logger.warning(f"⚠️  {failed} statement(s) failed out of {len(statements)} total")
        logger.debug(f"Executed {executed} statement(s) successfully")
    
    def init_schemas(self, engine: Engine, schemas: list[str] = None):
        """
        Initialize database schemas.
        
        Args:
            engine: SQLAlchemy engine
            schemas: List of schema names to create. Defaults to ['stage', 'universal']
        """
        if schemas is None:
            schemas = ['stage', 'universal']
        
        logger.info(f"Creating schemas: {', '.join(schemas)}...")
        
        with engine.begin() as conn:
            for schema_name in schemas:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
                logger.info(f"✅ Created '{schema_name}' schema")
    
    def init_stage_tables(self, engine: Engine):
        """
        Initialize stage tables from V2__stage_schema.sql.
        
        Args:
            engine: SQLAlchemy engine
        """
        stage_sql_path = self.schema_base_path / "V2__stage_schema.sql"
        
        if not stage_sql_path.exists():
            raise FileNotFoundError(
                f"Stage schema file not found: {stage_sql_path}\n"
                f"Ensure BP010-data-pipelines/src/sql/schema/V2__stage_schema.sql exists."
            )
        
        logger.info("Creating stage tables...")
        sql_content = self._read_sql_file(stage_sql_path)
        self._execute_sql_statements(engine, sql_content, "stage tables")
        logger.info("✅ Stage tables created")
    
    def init_universal_tables(self, engine: Engine):
        """
        Initialize universal tables from V1__universal_schema.sql.
        
        Args:
            engine: SQLAlchemy engine
        """
        universal_sql_path = self.schema_base_path / "V1__universal_schema.sql"
        
        if not universal_sql_path.exists():
            raise FileNotFoundError(
                f"Universal schema file not found: {universal_sql_path}\n"
                f"Ensure BP010-data-pipelines/src/sql/schema/V1__universal_schema.sql exists."
            )
        
        logger.info("Creating universal tables...")
        sql_content = self._read_sql_file(universal_sql_path)
        self._execute_sql_statements(engine, sql_content, "universal tables")
        logger.info("✅ Universal tables created")
    
    def init_reporting_tables(self, engine: Engine):
        """
        Initialize reporting tables from V1__reporting_schema.sql.
        
        Args:
            engine: SQLAlchemy engine
        """
        reporting_sql_path = self.schema_base_path / "V1__reporting_schema.sql"
        
        if not reporting_sql_path.exists():
            raise FileNotFoundError(
                f"Reporting schema file not found: {reporting_sql_path}\n"
                f"Ensure BP010-data-pipelines/src/sql/schema/V1__reporting_schema.sql exists."
            )
        
        logger.info("Creating reporting tables...")
        sql_content = self._read_sql_file(reporting_sql_path)
        self._execute_sql_statements(engine, sql_content, "reporting tables")
        logger.info("✅ Reporting tables created")
    
    def init_all(self, engine: Engine, include_reporting: bool = False):
        """
        Initialize all schemas and tables.
        
        Args:
            engine: SQLAlchemy engine
            include_reporting: Whether to include reporting schema (default: False)
        """
        logger.info("=" * 60)
        logger.info("Database Schema Initialization")
        logger.info("=" * 60)
        
        try:
            # Initialize schemas
            schemas = ['stage', 'universal']
            if include_reporting:
                schemas.append('reporting')
            self.init_schemas(engine, schemas)
            
            # Initialize tables
            self.init_stage_tables(engine)
            self.init_universal_tables(engine)
            
            if include_reporting:
                self.init_reporting_tables(engine)
            
            logger.info("\n" + "=" * 60)
            logger.info("✅ Database initialization completed successfully!")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}", exc_info=True)
            raise


def get_schema_manager(base_path: Optional[Path] = None) -> SchemaManager:
    """
    Factory function to get SchemaManager instance.
    
    This function handles path resolution when called from different ML processes.
    
    Args:
        base_path: Optional base path. If None, tries to auto-detect BP010 location.
    
    Returns:
        SchemaManager instance
    """
    if base_path is None:
        # Strategy 1: If this file is in BP010-data-pipelines, use it directly
        current_file = Path(__file__).resolve()
        if "BP010-data-pipelines" in current_file.parts:
            bp010_index = current_file.parts.index("BP010-data-pipelines")
            bp010_path = Path(*current_file.parts[:bp010_index + 1])
            schema_path = bp010_path / "src" / "sql" / "schema"
            if schema_path.exists():
                return SchemaManager(schema_path)
        
        # Strategy 2: Try to find BP010-data-pipelines from current working directory
        current_path = Path.cwd().resolve()
        
        # Check if we're in BP010-data-pipelines
        if current_path.name == "BP010-data-pipelines":
            schema_path = current_path / "src" / "sql" / "schema"
            if schema_path.exists():
                return SchemaManager(schema_path)
        
        # Check if we're in a subdirectory of BP010-data-pipelines
        if "BP010-data-pipelines" in current_path.parts:
            bp010_index = current_path.parts.index("BP010-data-pipelines")
            bp010_path = Path(*current_path.parts[:bp010_index + 1])
            schema_path = bp010_path / "src" / "sql" / "schema"
            if schema_path.exists():
                return SchemaManager(schema_path)
        
        # Check if we're in Process root (parent of all BP projects)
        if "Process" in current_path.parts:
            process_index = current_path.parts.index("Process")
            process_path = Path(*current_path.parts[:process_index + 1])
            schema_path = process_path / "BP010-data-pipelines" / "src" / "sql" / "schema"
            if schema_path.exists():
                return SchemaManager(schema_path)
        
        # Strategy 3: Search upward from current file location
        search_path = current_file.parent
        for _ in range(10):  # Search up to 10 levels
            if search_path.name == "BP010-data-pipelines":
                schema_path = search_path / "src" / "sql" / "schema"
                if schema_path.exists():
                    return SchemaManager(schema_path)
            # Check if Process is in path and BP010 is sibling
            if "Process" in search_path.parts:
                process_idx = search_path.parts.index("Process")
                process_path = Path(*search_path.parts[:process_idx + 1])
                schema_path = process_path / "BP010-data-pipelines" / "src" / "sql" / "schema"
                if schema_path.exists():
                    return SchemaManager(schema_path)
            search_path = search_path.parent
            if search_path == search_path.parent:  # Reached root
                break
        
        # Fallback: assume relative to current file
        schema_path = Path(__file__).parent / "sql" / "schema"
        return SchemaManager(schema_path)
    else:
        return SchemaManager(base_path)

