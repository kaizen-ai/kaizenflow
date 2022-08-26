"""
Import as:

import im_v2.common.secrets.secret_identifier as imvcsseid
"""

from dataclasses import dataclass

import helpers.hdbg as hdbg

# TODO(Juraj): possibly extend the behavior to fetch the secret values
# themselves from AWS instead of acting as a pure dataclass.
@dataclass(frozen=True)
class SecretIdentifier:
    """
    Wrapper class to store categorized secret identifier.
    """

    exchange_id: str
    stage: str
    account_type: str
    _id: int

    def __post_init__(self) -> None:
        hdbg.dassert_in(self.stage, ["local", "preprod", "prod"])
        hdbg.dassert_in(self.account_type, ["sandbox", "trading"])

    def __str__(self) -> str:
        return (
            f"{self.exchange_id}.{self.stage}.{self.account_type}.{str(self._id)}"
        )
