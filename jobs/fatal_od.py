import os
import datetime
from marshmallow import fields, post_load

import pipeline as pl

HERE = os.path.abspath(os.path.dirname(__file__))

class FatalODSchema(pl.BaseSchema):
    death_date = fields.DateTime(format='%m/%d/%Y')
    death_time = fields.DateTime(format='%I:%M %p')
    death_date_and_time = fields.DateTime(dump_only=True)
    manner_of_death = fields.String()
    age = fields.Integer()
    sex = fields.String()
    race = fields.String()
    case_dispo = fields.String()
    combined_od1 = fields.String(allow_none=True)
    combined_od2 = fields.String(allow_none=True)
    combined_od3 = fields.String(allow_none=True)
    combined_od4 = fields.String(allow_none=True)
    combined_od5 = fields.String(allow_none=True)
    combined_od6 = fields.String(allow_none=True)
    combined_od7 = fields.String(allow_none=True)
    incident_zip = fields.Integer()
    decedent_zip = fields.Integer()
    case_year = fields.Integer()

    @post_load
    def combine_date_and_time(self, in_data):
        death_date, death_time = in_data['death_date'], in_data['death_time']
        today = datetime.datetime.today()

        in_data['death_date_and_time'] = datetime.datetime(
            death_date.year, death_date.month, death_date.day,
            death_time.hour, death_time.minute, death_time.second
        )
        in_data['death_time'].replace(
            year=today.year, month=today.month, day=today.day
        )
        return

    @post_load
    def stringify_datetimes(self, in_data):
        in_data['death_date'] = str(in_data['death_date'])
        in_data['death_time'] = str(in_data['death_time'])
        in_data['death_date_and_time'] = str(in_data['death_date_and_time'])

package_id = '945f9505-f33b-46e1-9c43-6c3315b4b0cd'
resource_name = 'Fatal Accidental OD 2014'

target = 'accidental_fatal_overdoses/fatal_od_mock.csv'

# with open(HERE + '/../settings.json') as f:
#     sftp_config = json.load(f)['county_sftp']

# .connect(pl.SFTPConnector, target, **sftp_config, encoding=None) \

fatal_od_pipeline = pl.Pipeline('fatal_od_pipeline', 'Fatal OD Pipeline', log_status=False) \
    .connect(pl.SFTPConnector, target, encoding=None) \
    .extract(pl.CSVExtractor, firstline_headers=True) \
    .schema(FatalODSchema) \
    .load(pl.CKANDatastoreLoader,
          fields=FatalODSchema().serialize_to_ckan_fields(capitalize=False),
          package_id=package_id,
          resource_name=resource_name,
          method='insert')
