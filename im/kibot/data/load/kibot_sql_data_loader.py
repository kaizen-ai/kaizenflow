import helpers.dbg as dbg
import im.common.data.load.abstract_data_loader as icdlab
import im.common.data.types as icdtyp


class KibotSqlDataLoader(icdlab.AbstractSqlDataLoader):
    @staticmethod
    def _get_table_name_by_frequency(frequency: icdtyp.Frequency) -> str:
        """
        Get table name by predefined frequency.

        :param frequency: a predefined frequency
        :return: table name in DB
        """
        table_name = ""
        if frequency == icdtyp.Frequency.Minutely:
            table_name = "KibotMinuteData"
        elif frequency == icdtyp.Frequency.Daily:
            table_name = "KibotDailyData"
        elif frequency == icdtyp.Frequency.Tick:
            table_name = "KibotTickData"
        dbg.dassert(table_name, f"Unknown frequency {frequency}")
        return table_name
