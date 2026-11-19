"""
add msg_hash_norm.

Revision ID: 3f1b7c2d9e4a
Revises: ac5c968f642e
Create Date: 2026-10-08 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3f1b7c2d9e4a"
down_revision: str | None = "ac5c968f642e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "transfers",
        sa.Column("msg_hash_norm", sa.String(length=255), nullable=True),
    )
    op.create_unique_constraint(
        op.f("uq__transfers__msg_hash_norm"),
        "transfers",
        ["msg_hash_norm"],
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("uq__transfers__msg_hash_norm"),
        "transfers",
        type_="unique",
    )
    op.drop_column("transfers", "msg_hash_norm")
