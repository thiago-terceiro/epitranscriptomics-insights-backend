import os
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

# Allow override with DATABASE_URL env var (for SQLite local testing)
database_url = os.getenv("DATABASE_URL")

if database_url and database_url.startswith("sqlite"):
    # Use aiosqlite for async SQLite
    DATABASE_URL = database_url.replace("sqlite:///", "sqlite+aiosqlite:///")
else:
    # PostgreSQL configuration
    DATABASE_URL = database_url or (
        f"postgresql+psycopg_async://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST','postgres')}:{os.getenv('POSTGRES_PORT','5432')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

# For SQLite, don't use pool settings
if DATABASE_URL.startswith("sqlite"):
    engine = create_async_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_async_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )
SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)
Base = declarative_base()

async def get_session():
    async with SessionLocal() as session:
        yield session

async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)