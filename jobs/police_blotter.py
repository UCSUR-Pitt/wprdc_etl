import datetime
from marshmallow import fields, post_load, pre_load
import pipeline as pl

yesterday = (datetime.datetime.now() - datetime.timedelta(days=1))
url = 'http://apps.pittsburghpa.gov/police/arrest_blotter/arrest_blotter_%s.csv' % (yesterday.strftime("%A"))

class PoliceBlotterSchema(pl.BaseSchema):
    report_name = fields.String(dump_to='REPORT_NAME')
    ccr = fields.Integer(dump_to='CCR')
    section = fields.String(dump_to='SECTION')
    description = fields.String(dump_to='DESCRIPTION')
    arrest_date = fields.DateTime(format='%m/%d/%Y', load_only=True)
    arrest_time = fields.DateTime(format='%H:%M',load_only=True)
    arrest_datetime = fields.DateTime(format='%m/%d/%YT%H:%M', dump_to='ARREST_TIME')
    address = fields.String(allow_none=True, dump_to='ADDRESS')
    neighborhood = fields.String(allow_none=True, dump_to='NEIGHBORHOOD')
    zone = fields.Integer(allow_none=True, dump_to='ZONE')
    age = fields.Integer(allow_none=True, dump_to='AGE')
    gender = fields.String(allow_none=True, dump_to='GENDER')

    @pre_load
    def process_na_zone(self, data):
        zone = data.get('zone')
        if zone.lower() in ['n/a', 'osc']:
            data['zone'] = None
        return data

    @post_load
    def combine_date_and_time(self, in_data):
        in_data['arrest_datetime'] = (datetime.datetime(
            in_data['arrest_date'].year, in_data['arrest_date'].month,
            in_data['arrest_date'].day, in_data['arrest_time'].hour,
            in_data['arrest_time'].minute, in_data['arrest_time'].second
        ))

package_id = '83ba85c6-9fd5-4603-bd98-cc9002e206dc'
resource_name = 'Incidents'

police_blotter_pipeline = pl.Pipeline('police_blotter_pipeline', 'Police Blotter Pipeline', log_status=False) \
    .connect(pl.RemoteFileConnector, url) \
    .extract(pl.CSVExtractor) \
    .schema(PoliceBlotterSchema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=PoliceBlotterSchema().serialize_to_ckan_fields(capitalize=True),
          package_id=package_id,
          resource_name=resource_name,
          method='insert')
