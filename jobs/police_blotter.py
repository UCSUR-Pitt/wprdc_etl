import datetime
from marshmallow import fields, post_load
import pipeline as pl


yesterday = (datetime.datetime.now() - datetime.timedelta(days=1))
url = 'http://apps.pittsburghpa.gov/police/arrest_blotter/arrest_blotter_%s.csv' % (yesterday.strftime("%A"))

class PoliceBlotterSchema(pl.BaseSchema):
    report_name = fields.String()
    ccr = fields.Integer()
    section = fields.String()
    description = fields.String()
    arrest_date = fields.DateTime(format='%m/%d/%Y')
    arrest_time = fields.DateTime(format='%H:%M')
    address = fields.String()
    neighborhood = fields.String()
    zone = fields.Integer()
    age = fields.Integer(allow_none=True)
    gender = fields.String(allow_none=True)

    @post_load
    def combine_date_and_time(self, in_data):
        in_data['arrest_time'] = str(datetime.datetime(
            in_data['arrest_date'].year, in_data['arrest_date'].month,
            in_data['arrest_date'].day, in_data['arrest_time'].hour,
            in_data['arrest_date'].minute, in_data['arrest_date'].second
        ))
        in_data.pop('arrest_date')


fields = [
            {
                "type": "text",
                "id": "REPORT_NAME"
            },
            {
                "type": "numeric",
                "id": "CCR"
            },
            {
                "type": "text",
                "id": "SECTION"
            },
            {
                "type": "text",
                "id": "DESCRIPTION"
            },
            {
                "type": "timestamp",
                "id": "ARREST_TIME"
            },
            {
                "type": "text",
                "id": "ADDRESS"
            },
            {
                "type": "text",
                "id": "NEIGHBORHOOD"
            },
            {
                "type": "numeric",
                "id": "ZONE"
            },
            {
                "type": "numeric",
                "id": "AGE"
            },
            {
                "type": "text",
                "id": "GENDER"
            }
        ]
package_id = '83ba85c6-9fd5-4603-bd98-cc9002e206dc'
resource_name = 'Incidents'

# Hack fix for fields capitalization issue
for field in fields:
    field['id'] = field['id'].lower()



police_blotter_pipeline = pl.Pipeline('police_blotter_pipeline', 'Police Blotter Pipeline') \
    .connect(pl.RemoteFileConnector, url) \
    .extract(pl.CSVExtractor) \
    .schema(PoliceBlotterSchema) \
    .load(pl.CKANDatastoreLoader,
          fields=fields,
          package_id=package_id,
          resource_name=resource_name,
          method='insert')