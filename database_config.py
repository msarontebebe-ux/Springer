"""
Database configuration for cloud deployment.
Supports PostgreSQL (Neon, RDS, Supabase) and local SQLite fallback.
Credentials are read from environment variables — never hardcoded.
"""

import os
from urllib.parse import quote_plus

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


class DatabaseConfig:
    """Builds SQLAlchemy engine from environment variables."""

    @staticmethod
    def get_connection_string() -> str:
        """
        Priority order:
        1. Full connection string  (AWS_RDS_CONNECTION_STRING or AZURE_SQL_CONNECTION_STRING)
        2. Individual components   (DB_TYPE + DB_HOST + DB_NAME + DB_USER + DB_PASSWORD)
        3. Local SQLite fallback   (DB_TYPE=sqlite or nothing set)
        """
        # Full connection string — Neon / RDS / Supabase paste it directly
        full = (
            os.getenv('AWS_RDS_CONNECTION_STRING')
            or os.getenv('AZURE_SQL_CONNECTION_STRING')
            or os.getenv('DATABASE_URL')
        )
        if full:
            return full

        db_type = os.getenv('DB_TYPE', 'sqlite')

        if db_type == 'sqlite':
            db_path = os.getenv('DB_PATH', 'data/springer_publications.db')
            return f'sqlite:///{db_path}'

        # Build from components
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        name = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')

        if not all([host, name, user, password]):
            raise ValueError(
                'Missing database config. Set DB_HOST, DB_NAME, DB_USER, DB_PASSWORD '
                '(or paste AWS_RDS_CONNECTION_STRING / DATABASE_URL from Neon).'
            )

        pw = quote_plus(password)

        if db_type == 'postgresql':
            port = port or '5432'
            # Add SSL mode for cloud databases (Neon, AWS RDS, etc.)
            ssl_param = '?sslmode=require' if 'neon.tech' in host or 'rds.amazonaws.com' in host else ''
            return f'postgresql://{user}:{pw}@{host}:{port}/{name}{ssl_param}'
        elif db_type == 'mysql':
            port = port or '3306'
            return f'mysql+pymysql://{user}:{pw}@{host}:{port}/{name}'
        elif db_type == 'sqlserver':
            port = port or '1433'
            driver = 'ODBC+Driver+17+for+SQL+Server'
            return f'mssql+pyodbc://{user}:{pw}@{host}:{port}/{name}?driver={driver}'
        else:
            raise ValueError(f'Unsupported DB_TYPE: {db_type}')

    @staticmethod
    def create_engine_from_env():
        """Create a SQLAlchemy engine from the current environment."""
        conn_str = DatabaseConfig.get_connection_string()

        kwargs = {
            'pool_pre_ping': True,
            'echo': os.getenv('DB_ECHO', 'false').lower() == 'true',
        }

        # Neon / serverless — use NullPool to avoid stale connections
        if os.getenv('DB_SERVERLESS', 'false').lower() == 'true' or 'neon.tech' in conn_str:
            kwargs['poolclass'] = NullPool
        else:
            kwargs['pool_size'] = int(os.getenv('DB_POOL_SIZE', '5'))
            kwargs['max_overflow'] = int(os.getenv('DB_MAX_OVERFLOW', '10'))
            kwargs['pool_recycle'] = 3600

        return create_engine(conn_str, **kwargs)

    @staticmethod
    def get_session():
        """Return a new SQLAlchemy session."""
        engine = DatabaseConfig.create_engine_from_env()
        Session = sessionmaker(bind=engine)
        return Session()

    @staticmethod
    def ping():
        """Test the connection. Prints result. Returns True on success."""
        try:
            engine = DatabaseConfig.create_engine_from_env()
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            print(f'✓ Database connected: {engine.url}')
            return True
        except Exception as e:
            print(f'✗ Database connection failed: {e}')
            return False


if __name__ == '__main__':
    DatabaseConfig.ping()
