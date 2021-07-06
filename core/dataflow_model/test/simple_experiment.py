import logging

import core.config as cconfig

_LOG = logging.getLogger(__name__)


def run_experiment(config: cconfig.Config) -> None:
    """
    This experiment fails/succeeds depending on the param `fail` stored inside
    the config.
    """
    _LOG.info("config=\n%s", config)
    if config is None:
        raise ValueError("No config provided")
    if config["fail"]:
        raise ValueError("Failure")
    _LOG.info("Success")
