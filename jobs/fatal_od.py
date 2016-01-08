import datetime
from marshmallow import fields, post_load

import pipeline as pl

class FatalODSchema(pl.BaseSchema):
    death_date = fields.DateTime(format='%m/%d/%Y')
    death_time = fields.DateTime(format='%I:%M %p')
    manner_of_death = fields.String()
    age = fields.Integer()
    sex = fields.String()
    race = fields.String()
    case_dispo = fields.String()
    combined_od1 = fields.String()
    combined_od2 = fields.String()
    combined_od3 = fields.String()
    combined_od4 = fields.String()
    combined_od5 = fields.String()
    combined_od6 = fields.String()
    combined_od7 = fields.String()
    incident_zip = fields.Integer()
    decedent_zip = fields.Integer()
    case_year = fields.Integer()

    @post_load
    def combine_date_and_time(self, in_data):
        death_date, death_time = in_data['death_date'], in_data['death_time']
        in_data['death_date_and_time'] = datetime.datetime(
            death_date.year, death_date.month, death_date.day,
            death_time.hour, death_time.minute, death_time.second
        )

    @post_load
    def replace_death_time_date(self, in_data):
        today = datetime.datetime.today()
        in_data['death_time'].replace(
            year=today.year, month=today.month, day=today.day
        )

fatal_od_pipeline = pl.Pipeline('fatal_od_pipeline', 'Fatal OD Pipeline') \
    .extract(pl.CSVExtractor, firstline_headers=True) \
    .schema(FatalODSchema) \
    .load(pl.Datapusher)
