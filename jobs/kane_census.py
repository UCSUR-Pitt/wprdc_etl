import datetime
from marshmallow import fields, pre_dump, pre_load
import pipeline as pl

class KaneCensusSchema(pl.BaseSchema):
    date = fields.Date()

    gh_male = fields.Integer(missing=None)
    gh_female = fields.Integer(missing=None)
    gh_white = fields.Integer(missing=None)
    gh_black = fields.Integer(missing=None)
    gh_other = fields.Integer(missing=None)

    scott_male = fields.Integer(missing=None)
    scott_female = fields.Integer(missing=None)
    scott_white = fields.Integer(missing=None)
    scott_black = fields.Integer(missing=None)
    scott_other = fields.Integer(missing=None)

    mck_male = fields.Integer(missing=None)
    mck_female = fields.Integer(missing=None)
    mck_white = fields.Integer(missing=None)
    mck_black = fields.Integer(missing=None)
    mck_other = fields.Integer(missing=None)

    ross_male = fields.Integer(missing=None)
    ross_female = fields.Integer(missing=None)
    ross_white = fields.Integer(missing=None)
    ross_black = fields.Integer(missing=None)
    ross_other = fields.Integer(missing=None)


    class Meta:
        ordered = True

    @pre_load()
    def make_iso_date(self, in_data):
        in_data['date'] = datetime.datetime.strptime(in_data['date'], '%m/%d/%Y').isoformat()

package_id = '79f7236b-76fb-4a41-9011-e5594e01c57c'
resource_name = 'Kane Monthly Census Data'

today = datetime.date.today() - datetime.timedelta(days=1)

target = 'Kane_Daily_Census/Census Demographics-Kane {}.xlsx'.format(today.strftime('%m%d%Y'))

kane_census_pipeline = pl.Pipeline('kane_census_pipeline', 'Kane Census  Pipeline', log_status=True) \
    .connect(pl.SFTPConnector, target, config_string='sftp.county_sftp', encoding=None) \
    .extract(pl.ExcelExtractor, firstline_headers=True) \
    .schema(KaneCensusSchema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=KaneCensusSchema().serialize_to_ckan_fields(),
          key_fields=['date'],
          package_id=package_id,
          resource_name=resource_name,
          method='upsert')
