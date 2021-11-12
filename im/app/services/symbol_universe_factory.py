# TODO(gp): CmampTask413, remove
"""
Produce SymbolUniverse objects.

Import as: import im.app.services.symbol_universe_factory as iassym
"""

from typing import Any

import im.common.metadata.symbols as icmsym


class SymbolUniverseFactory:
    @classmethod
    def get_symbol_universe(
        cls, provider: str, **kwargs: Any
    ) -> icmsym.SymbolUniverse:
        """
        Get the universe of supported symbols for a given provider.

        :param provider: provider (e.g., "kibot", "ib", ...)
        :raises ValueError: if SymbolUniverse is not implemented for provider
        """
        symbol_universe: icmsym.SymbolUniverse
        # TODO(plyq): Implement for kibot.
        if provider == "ib":
            import im.ib.metadata.ib_symbols as iimibs

            symbol_universe = iimibs.IbSymbolUniverse(**kwargs)
        else:
            raise ValueError(
                "Symbol universe for provider '%s' is not implemented" % provider
            )
        return symbol_universe
