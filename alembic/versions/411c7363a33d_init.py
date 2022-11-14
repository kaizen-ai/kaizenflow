"""Init

Revision ID: 411c7363a33d
Revises: 
Create Date: 2022-11-14 15:04:45.509703+00:00

"""
import alembic
import os
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '411c7363a33d'
down_revision = None
branch_labels = None
depends_on = None

dir_path = os.path.dirname(os.path.realpath(__file__))
init_sql_file = os.path.join(dir_path, "411c7363a33d_init.sql")
# op.execute is an alembic method to call sql
# TODO(Juraj): specify the path to the revision file with relative to
# root of the repo (container)
with open(init_sql_file) as file:
    alembic.op.execute(file.read())

def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
