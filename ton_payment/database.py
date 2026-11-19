from collections.abc import AsyncIterator

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

from ton_payment.config import settings

METADATA = MetaData(
    naming_convention={
        "all_column_names": lambda constraint, _: "_".join(
            [column.name for column in constraint.columns.values()],
        ),
        "pk": "pk__%(table_name)s",
        "ix": "ix__%(table_name)s__%(all_column_names)s",
        "fk": "fk__%(table_name)s__%(all_column_names)s__"
        "%(referred_table_name)s",
        "uq": "uq__%(table_name)s__%(all_column_names)s",
        "ck": "ck__%(table_name)s__%(constraint_name)s",
    },
)


async def get_async_session() -> AsyncIterator[AsyncSession]:
    async with async_session_maker() as session:
        yield session


engine: AsyncEngine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    echo=settings.postgres_echo,
    echo_pool=settings.postgres_echo,
)

async_session_maker = async_sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

Base = declarative_base(metadata=METADATA)
