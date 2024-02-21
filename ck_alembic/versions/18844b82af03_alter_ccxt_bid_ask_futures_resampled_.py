"""Alter ccxt_bid_ask_futures_resampled_1min table

Revision ID: 18844b82af03
Revises: 59e3f93ea2ca
Create Date: 2024-02-07 13:19:50.773029+00:00

"""
import os
import alembic

# revision identifiers, used by Alembic.
revision = '18844b82af03'
down_revision = '59e3f93ea2ca'
branch_labels = None
depends_on = None

dir_path = os.path.dirname(os.path.realpath(__file__))
init_sql_file = os.path.join(
    dir_path, "18844b82af03_alter_ccxt_bid_ask_futures_resampled_.sql"
)


def upgrade() -> None:
    # op.execute is an alembic method to call sql
    # TODO(Juraj): specify the path to the revision file with relative to
    # root of the repo (container)
    with open(init_sql_file) as file:
        alembic.op.execute(file.read())


def downgrade() -> None:
    pass
