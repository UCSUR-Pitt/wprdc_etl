import os

from jobs.jail_census import JailCensusSchema
import pipeline as pl

from .base import TestBase, HERE

class TestJailPipeline(TestBase):
    def test_jail_pipeline(self):
        pl.Pipeline(
            'jail_census_combined_pipeline', 'Jail Census Combined Pipeline',
            settings_file=self.settings_file,
            conn=self.conn
        ) \
            .connect(pl.FileConnector, os.path.join(HERE, '../mock/jail_census_mock.csv')) \
            .extract(pl.CSVExtractor, firstline_headers=True) \
            .schema(JailCensusSchema) \
            .load(self.Loader) \
            .run()
        status = self.cur.execute('select * from status').fetchall()

        self.assertEquals(len(status), 1)
        self.assertEquals(status[0][-2], 'success')
        self.assertEquals(status[0][-1], 17)
