import requests
import json
import datetime

from pipeline.exceptions import CKANException

class Loader(object):
    def __init__(self, config, *args, **kwargs):
        self.config = config

    def load(self, data):
        raise NotImplementedError

class CKANLoader(Loader):
    """Connection to ckan datastore"""

    def __init__(self, config, *args, **kwargs):
        super(CKANLoader, self).__init__(config, *args, **kwargs)
        self.ckan_url = self.config['root_url'].rstrip('/') + '/api/3/'
        self.dump_url = self.config['root_url'].rstrip('/') + '/datastore/dump/'
        self.key = self.config['api_key']
        self.resource_id = kwargs.get('resource_id')
        self.package_id = kwargs.get('package_id')
        self.resource_name = kwargs.get('resource_name')

    def resource_exists(self, package_id, resource_name):
        """
        Searches for resource on ckan instance
        Params:
            package_id: id of resources parent dataset
            resource_name: name of the resource
        Returns:
            ``True`` if the resource is found within the package,
            ``False`` otherwise
        """

        check_resource = requests.post(
            self.ckan_url + 'action/package_show',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': package_id
            })
        )

        response = check_resource.json()
        return resource_name in set(i['name'] for i in response['result']['resources'])

    def create_resource(self, package_id, resource_name):
        '''
        Creates new resource in ckan instance
        Params:
            package_id: dataset under which to add new resource
            resource_name: name of new resource
        Returns:
            id of newly created resource if successful, None otherwise
        '''

        # Make api call
        response = requests.post(
            self.ckan_url + 'action/resource_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'package_id': package_id,
                'url': '#',
                'name': resource_name,
                'url_type': 'datapusher',
                'format': 'CSV'
            })
        )

        response_json = response.json()

        if not response_json.get('success', False):
            raise CKANException('An error occured: {}'.format(response_json['error']['name'][0]))

        return response_json['result']['id']

    def create_datastore(self, resource_id, fields):
        """
        Creates new datastore for specified resource
        Params:
            resource_id: resource id for which new datastore is being made
            fields: header fields for csv file
        Returns:
            resource_id for the new datastore if successful
        """

        # Make API call
        create_datastore = requests.post(
            self.ckan_url + 'action/datastore_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'force': True,
                'fields': fields
            })
        )

        create_datastore = create_datastore.json()

        if not create_datastore.get('success', False):
            raise CKANException('An error occured: {}'.format(create_datastore['error']['name'][0]))

        return create_datastore['result']['resource_id']

    def generate_datastore(self, fields):
        if self.resource_id is None:
            self.resource_id = self.create_resource(self.package_id, self.resource_name)
            self.create_datastore(self.resource_id, fields)

        return self.resource_id

    def delete_datastore(self, resource_id):
        """
        Deletes datastore table for resource
        Params:
            resource: resource_id to remove table from
        Returns:
            Status code from the request
        """
        delete = requests.post(
            self.ckan_url + 'action/datastore_delete',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'force': True
            })
        )
        return delete.status_code

    def upsert(self, resource_id, data, method='upsert'):
        """
        Upsert data into datastore
        Params:
            resource_id: resource_id to which data will be inserted
            data: data to be upserted
        Returns:
            request status
        """
        insert = requests.post(
            self.ckan_url + 'action/datastore_upsert',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'method': method,
                'force': True,
                'records': data
            })
        )
        return insert.status_code

    def update_metadata(self, resource_id):
        """
        TODO: Make this versatile
        Params:
            resource_id: resource whose metadata willbe modified
        Returns:
            request status
        """
        update = requests.post(
            self.ckan_url + 'action/resource_patch',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': resource_id,
                'url': self.dump_url + str(resource_id),
                'url_type': 'datapusher',
                'last_modified': datetime.datetime.now().isoformat(),
            })
        )
        return update.status_code

    def load(self, data):
        raise NotImplementedError

class CKANUpsertLoader(CKANLoader):
    def __init__(self, config, *args, **kwargs):
        super(CKANUpsertLoader, self).__init__(config, *args, **kwargs)
        self.fields = kwargs.get('fields', None)
        self.method = kwargs.get('method', 'upsert')

        if self.fields is None:
            raise RuntimeError('Fields must be specified')

    def load(self, data):
        self.generate_datastore(self.fields)
        self.upsert(self.resource_id, data, self.method)
        self.update_metadata(self.resource_id)

class CKANBinaryLoader(CKANLoader):
    def __init__(self, config, *args, **kwargs):
        super(CKANBinaryLoader, self).__init__(config, *args, **kwargs)
        self.fields = kwargs.get('fields')

    def load(self, data):
        self.generate_datastore(self.fields)
