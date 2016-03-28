import os

from jobs.kane_census import KaneCensusSchema
import pipeline as pl

from collections import OrderedDict

from .base import TestBase, HERE


class TestKanePipeline(TestBase):
    def test_kane_pipeline(self):
        kane_test_pipeline = pl.Pipeline(
            'kane_census_combined_pipeline', 'Kane Census Combined Pipeline',
            settings_file=self.settings_file,
            conn=self.conn
        ) \
            .connect(pl.FileConnector, os.path.join(HERE, '../mock/kane_census_mock.xlsx'), encoding=None) \
            .extract(pl.ExcelExtractor, firstline_headers=True) \
            .schema(KaneCensusSchema) \
            .load(self.Loader) \
            .run()
        status = self.cur.execute('select * from status').fetchall()

        self.assertEquals(
            kane_test_pipeline.data[0],
            OrderedDict(
                [('date', '2015-10-01'), ('gh_male', 81), ('gh_female', 109), ('gh_white', 116), ('gh_black', None),
                 ('gh_other', 1), ('scott_male', 93), ('scott_female', 180), ('scott_white', 262), ('scott_black', 10),
                 ('scott_other', 1), ('mck_male', 100), ('mck_female', 184), ('mck_white', 222), ('mck_black', 60),
                 ('mck_other', 2), ('ross_male', 63), ('ross_female', 166), ('ross_white', 213), ('ross_black', 14),
                 ('ross_other', 2)]
            )
        )
        self.assertEquals(len(status), 1)
        self.assertEquals(status[0][-2], 'success')
        self.assertEquals(status[0][-1], 23)
