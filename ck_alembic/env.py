"""
Import as:

import alembic.env as alenv
"""

import logging

import sqlalchemy

import helpers.hsql as hsql
from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    logging.config.fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

# This is a custom added argument to differentiate between db_stages.
cmd_kwargs = context.get_x_argument(as_dictionary=True)
if "stage" not in cmd_kwargs:
    raise Exception(
        "Argument `stage` not found in the CLI arguments. "
        "Please verify `alembic` was run with `-x stage=db_stage` "
        "(e.g. `alembic -x stage=dev upgrade head`)"
    )
stage = cmd_kwargs["stage"]


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    In this scenario we need to create an Engine and associate a
    connection with the context.
    """
    # In order to reuse our API we need to pass this as callable
    #  into sqlalchemy.
    conn_creator = lambda _: hsql.get_connection_from_env_file(stage)
    connectable = sqlalchemy.create_engine(
        "postgresql+psycopg2://", creator=conn_creator
    )
    with connectable.connect() as conn:
        context.configure(
            connection=conn,
            target_metadata=target_metadata,
            version_table_schema="public",
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()