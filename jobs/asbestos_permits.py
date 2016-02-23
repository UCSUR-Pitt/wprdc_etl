import os
import datetime
from marshmallow import fields, pre_load, post_load

import pipeline as pl

HERE = os.path.abspath(os.path.dirname(__file__))

class AsbestosPermitSchema(pl.BaseSchema):
    permit_number = fields.String()
    s_name = fields.String()
    s_address = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    z_code = fields.Integer(allow_none=True)
    p_fee = fields.Float(allow_none=True)
    c_number = fields.Integer(allow_none=True)
    contractor = fields.Integer(allow_none=True)
    permit_specifications = fields.String(allow_none=True)
    square_feet = fields.Integer(allow_none=True)
    is_the_building_occupied = fields.Boolean(allow_none=True)
    i_date = fields.DateTime(format='%m/%d/%Y', allow_none=True)
    e_date = fields.DateTime(format='%m/%d/%Y', allow_none=True)
    achd_inspector = fields.String(allow_none=True)
    county_executive = fields.String(allow_none=True)
    job_complete = fields.Boolean(allow_none=True)
    extention_date = fields.Boolean(allow_none=True)
    permit_o_e_date = fields.DateTime(format='%m/%d/%Y', allow_none=True)
    project_type = fields.String(allow_none=True)

    class Meta:
        ordered = True


package_id = '649d8d69-9901-483f-9989-3f112ba23cc4'
resource_name = 'Asbestos Permits'
target = 'Asbestos Permits/Site_Info.xlsx'

asbestos_permit_pipeline = pl.Pipeline('asbestos_permit_pipeline', 'Asbestos Permit Pipeline', log_status=False) \
    .connect(pl.SFTPConnector, target, config_string='sftp.county_sftp', encoding=None) \
    .extract(pl.ExcelExtractor, firstline_headers=True) \
    .schema(AsbestosPermitSchema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=AsbestosPermitSchema().serialize_to_ckan_fields(capitalize=False),
          key_fields=['permit_number'],
          package_id=package_id,
          resource_name=resource_name,
          method='upsert')
