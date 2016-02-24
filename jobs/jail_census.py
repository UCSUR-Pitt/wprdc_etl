import datetime
from marshmallow import fields, pre_dump, pre_load
import pipeline as pl

class JailCensusSchema(pl.BaseSchema):
    date = fields.Date(dump_to='Date')
    gender = fields.String(dump_to='Gender')
    race = fields.String(dump_to='Race')
    age_at_booking = fields.String(dump_to='Age at Booking')
    current_age = fields.String(dump_to='Current Age')

    class Meta:
        ordered = True

    @pre_load()
    def format_date(self, data):
        data['date'] = datetime.date(
            int(data['date'][0:4]),
            int(data['date'][4:6]),
            int(data['date'][6:])).isoformat()


yesterday = datetime.date.today() - datetime.timedelta(days=1)

package_id = '79f7236b-76fb-4a41-9011-e5594e01c57c'
combined_resource_name = 'ACJ Daily Census Data (Combined)'
monthly_resource_name = 'ACJ Daily Census Data - {}/{}'.format(yesterday.month, yesterday.year)
target = 'jail_census_data/acj_daily_population_{}.csv'.format(yesterday.strftime('%Y%m%d'))

# Combined Data
jail_census_combined_pipeline = pl.Pipeline('jail_census_combined_pipeline', 'Jail Census Combined Pipeline', log_status=False) \
    .connect(pl.SFTPConnector, target, config_string='sftp.county_sftp', encoding='utf-8') \
    .extract(pl.CSVExtractor, firstline_headers=True) \
    .schema(JailCensusSchema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=JailCensusSchema().serialize_to_ckan_fields(),
          package_id=package_id,
          resource_name=combined_resource_name,
          method='insert')

# Monthly Data
jail_census_monthly_pipeline = pl.Pipeline('jail_census_monthly_pipeline', 'Jail Census Monthly Pipeline', log_status=False) \
    .connect(pl.SFTPConnector, target, config_string='sftp.county_sftp', encoding='utf-8') \
    .extract(pl.CSVExtractor, firstline_headers=True, ) \
    .schema(JailCensusSchema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=JailCensusSchema().serialize_to_ckan_fields(),
          package_id=package_id,
          resource_name=monthly_resource_name,
          method='insert')

jail_census_combined_pipeline.run()
jail_census_monthly_pipeline.run()
