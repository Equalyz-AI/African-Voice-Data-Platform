"""fix category spelling

Revision ID: c9df2ef2640f
Revises: 6b383f1a727c
Create Date: 2025-09-15 06:35:40.438576

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c9df2ef2640f'
down_revision: Union[str, Sequence[str], None] = '6b383f1a727c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
