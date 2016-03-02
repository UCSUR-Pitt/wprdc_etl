import os

from jobs.jail_census import JailCensusSchema
import pipeline as pl

from collections import OrderedDict

from .base import TestBase, HERE


class TestJailPipeline(TestBase):
    def test_jail_pipeline(self):
        jail_test_pipeline = pl.Pipeline(
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

        self.assertEquals(
            jail_test_pipeline.data[0],
            OrderedDict(
                (('Date', '2016-01-03'),
                 ('Gender', 'M'),
                 ('Race', 'B'),
                 ('Age at Booking', '24'),
                 ('Current Age', '26'))
            )
        )
        self.assertEquals(len(status), 1)
        self.assertEquals(status[0][-2], 'success')
        self.assertEquals(status[0][-1], 17)
