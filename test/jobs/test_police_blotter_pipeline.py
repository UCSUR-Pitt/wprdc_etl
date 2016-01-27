import os

from jobs.police_blotter import PoliceBlotterSchema
import pipeline as pl

from .base import TestBase, HERE

class TestODPipeline(TestBase):
    def test_blotter_pipeline(self):
        pl.Pipeline(
            'fatal_od_pipeline', 'Fatal OD Pipeline',
            server=self.default_server,
            settings_file=self.settings_file,
            conn=self.conn
        ) \
            .connect(pl.LocalFileConnector, os.path.join(HERE, '../mock/arrest_blotter_mock.csv')) \
            .extract(pl.CSVExtractor, firstline_headers=True) \
            .schema(PoliceBlotterSchema) \
            .load(self.Loader) \
            .run()
        status = self.cur.execute('select * from status').fetchall()
        self.assertEquals(len(status), 1)
        self.assertEquals(status[0][-2], 'success')
        self.assertEquals(status[0][-1], 10)
