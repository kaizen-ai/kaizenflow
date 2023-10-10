"""Add spot identifier to tables and trades data table

Revision ID: 59e3f93ea2ca
Revises: 411c7363a33d
Create Date: 2023-03-13 17:37:05.603636+00:00

"""
import os

import alembic

# revision identifiers, used by Alembic.
revision = "59e3f93ea2ca"
down_revision = "411c7363a33d"
branch_labels = None
depends_on = None

dir_path = os.path.dirname(os.path.realpath(__file__))
init_sql_file = os.path.join(
    dir_path, "59e3f93ea2ca_add_spot_identifier_to_tables_and_.sql"
)


def upgrade() -> None:
    # op.execute is an alembic method to call sql
    # TODO(Juraj): specify the path to the revision file with relative to
    # root of the repo (container)
    with open(init_sql_file) as file:
        alembic.op.execute(file.read())


def downgrade() -> None:
    pass
