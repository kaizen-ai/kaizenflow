import json
from typing import Generator, List

from tqdm import tqdm

from twitter_data.filler_pipeline_01.filler_classes import TweetsJsonFiller


class TelegramJsonFiller(TweetsJsonFiller):
    """
    converts field 'created_at';
    reads a single file
    """

    def __init__(self,
                 src_data: str,
                 filler_version: str,
                 paranoid: bool = True,
                 ):
        super().__init__(src_data=src_data,
                         filler_version=filler_version,
                         paranoid=paranoid)

    def convert_fields(self):
        for datefield in ['date','fwd_from.date']:
            print('converting ' + datefield + ' field.')
            self.connector.collection.aggregate([{"$addFields": {
                datefield: {"$dateFromString": {
                    "dateString": "$" + datefield
                }}}},
                {"$out": self.dst_collection_name}]
                )

    def data_reader(self) -> Generator[List[dict], None, None]:
        print('processing data')
        with open(self.src_data) as f:
            rows = f.readlines()
        if not rows:
            print(f"empty file: {self.src_data}")
        yield [json.loads(i) for i in rows]


    def fill(self, **kwargs):
        for data in tqdm(self.data_reader()):
            self.fill_data(data)
        self.convert_fields()