import os

from jobs.asbestos_permits import AsbestosPermitSchema
import pipeline as pl

from .base import TestBase, HERE

class TestPermitsPipeline(TestBase):
    def test_permits_pipeline(self):
        pl.Pipeline(
            'asbestos_permits_pipeline', 'Asbestos Permit Pipeline',
            settings_file=self.settings_file,
            conn=self.conn
        ) \
            .connect(pl.FileConnector, os.path.join(HERE, '../mock/asbestos_mock.xlsx'), encoding=None) \
            .extract(pl.ExcelExtractor, firstline_headers=True) \
            .schema(AsbestosPermitSchema) \
            .load(self.Loader) \
            .run()
        status = self.cur.execute('select * from status').fetchall()
        self.assertEquals(len(status), 1)
        self.assertEquals(status[0][-2], 'success')
        self.assertEquals(status[0][-1], 1)
