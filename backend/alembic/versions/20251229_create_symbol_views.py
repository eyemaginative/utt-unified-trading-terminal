"""create symbol_views table

Revision ID: 20251229_create_symbol_views
Revises: <PUT_PREVIOUS_REVISION_ID_HERE>
Create Date: 2025-12-29
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251229_create_symbol_views"
down_revision = "<PUT_PREVIOUS_REVISION_ID_HERE>"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "symbol_views",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("view_key", sa.String(), nullable=False),
        sa.Column("viewed_confirmed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("viewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("idx_symbol_views_view_key", "symbol_views", ["view_key"], unique=True)
    op.create_index("idx_symbol_views_viewed_at", "symbol_views", ["viewed_at"], unique=False)


def downgrade():
    op.drop_index("idx_symbol_views_viewed_at", table_name="symbol_views")
    op.drop_index("idx_symbol_views_view_key", table_name="symbol_views")
    op.drop_table("symbol_views")
