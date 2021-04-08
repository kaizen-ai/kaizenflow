"""
Produce SymbolUniverse objects.

Import as: import instrument_master.app.services.symbol_universe_factory as iassym
"""

import instrument_master.common.metadata.symbols as icmsym


class SymbolUniverseFactory:
    @classmethod
    def get_symbol_universe(cls, provider: str) -> icmsym.SymbolUniverse:
        """
        Get symbol universe for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if SymbolUniverse is not implemented for provider
        """
        symbol_universe: icmsym.SymbolUniverse
        # TODO(plyq): Implement for kibot.
        if provider == "ib":
            import instrument_master.ib.metadata.ib_symbols as iimibs

            symbol_universe = iimibs.IbSymbolUniverse()
        else:
            raise ValueError(
                "S3 to SQL transformer for provider '%s' is not implemented"
                % provider
            )
        return symbol_universe
