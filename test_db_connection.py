#!/usr/bin/env python3
"""
Test database connectivity for audit environment.
"""
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get database URL
DATABASE_URL = os.getenv("DATABASE_URL")

print("=" * 60)
print("Database Connectivity Test")
print("=" * 60)
print(f"Connecting to: {DATABASE_URL.replace(os.getenv('DEV_DB_PASSWORD', ''), '****')}")

try:
    # Create engine
    engine = create_engine(DATABASE_URL)
    
    # Test connection
    with engine.connect() as connection:
        result = connection.execute(text("SELECT version();"))
        version = result.scalar()
        print(f"\n✅ Connection successful!")
        print(f"PostgreSQL version: {version}")
        
        # Check existing schemas
        result = connection.execute(text("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name;
        """))
        schemas = [row[0] for row in result]
        print(f"\nExisting schemas: {schemas if schemas else 'None (empty database)'}")
        
    print("\n" + "=" * 60)
    print("✅ Database is ready for audit!")
    print("=" * 60)
    sys.exit(0)
    
except Exception as e:
    print(f"\n❌ Connection failed: {e}")
    print("\n" + "=" * 60)
    print("Please check:")
    print("1. Docker container is running: docker ps")
    print("2. .env file has correct credentials")
    print("3. Port 5433 is available")
    print("=" * 60)
    sys.exit(1)
