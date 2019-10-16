from vendors.eurostat.base_classes import EurostatFileFiller
from data import data_config

EUROSTAT_FILLERS = {
    'TASK302_EUROSTAT_RAW_DATA': {
        'class': EurostatFileFiller,
        'settings': {
            'dst_dir': data_config.TASK302_RAW_DATA,
            'paranoid': True,
        },
        'optional': {
            'root_url': 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=data&sort=1&sort=2&start=all',
            }
    },

}


IMPORT_FILLER = {
    'human_name': 'EUROSTAT fillers',
    'fillers': EUROSTAT_FILLERS
}
