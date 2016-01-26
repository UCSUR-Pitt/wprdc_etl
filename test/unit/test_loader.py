import unittest
import os

from unittest.mock import Mock, patch, PropertyMock

import pipeline as pl
from pipeline.loaders import CKANLoader
from pipeline.exceptions import CKANException

HERE = os.path.abspath(os.path.dirname(__file__))

class TestCKANDatastoreBase(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.Pipeline(
            'test', 'Test', server='testing',
            settings_file=os.path.join(HERE, '../mock/test_settings.json'),
            log_status=False
        )

class TestCKANDatastore(TestCKANDatastoreBase):
    def setUp(self):
        super(TestCKANDatastore, self).setUp()
        self.ckan_loader = CKANLoader(self.pipeline.get_config())

    def test_datapusher_init(self):
        self.assertIsNotNone(self.ckan_loader)
        self.assertEquals(self.ckan_loader.ckan_url, 'localhost:9000/api/3/')
        self.assertEquals(self.ckan_loader.dump_url, 'localhost:9000/datastore/dump/')

    @patch('requests.post')
    def test_resource_exists(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'result': {'resources': []}},
            {'result': {'resources': [{'name': 'NOT EXIST'}]}},
            {'result': {'resources': [{'name': 'exists'}]}},
        ]
        post.return_value = mock_post

        self.assertFalse(self.ckan_loader.resource_exists(None, 'exists'))
        self.assertFalse(self.ckan_loader.resource_exists(None, 'exists'))
        self.assertTrue(self.ckan_loader.resource_exists(None, 'exists'))

    @patch('requests.post')
    def test_create_resource(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'error': {'name': ['not successful']}}
        ]
        post.return_value = mock_post

        self.assertEquals(self.ckan_loader.create_resource(None, None), 1)
        with self.assertRaises(CKANException):
            self.ckan_loader.create_resource(None, None)

    @patch('requests.post')
    def test_create_datastore(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'resource_id': 1}},
            {'error': {'name': ['not successful']}}
        ]
        post.return_value = mock_post

        self.assertEquals(self.ckan_loader.create_datastore(None, []), 1)
        with self.assertRaises(CKANException):
            self.ckan_loader.create_datastore(None, [])

    @patch('requests.post')
    def test_generate_datastore(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post

        self.assertEquals(self.ckan_loader.generate_datastore([]), 1)

    @patch('requests.post')
    def test_delete_datastore(self, post):
        type(post.return_value).status_code = PropertyMock(return_value=204)
        self.assertEquals(self.ckan_loader.delete_datastore(None), 204)

    @patch('requests.post')
    def test_upsert(self, post):
        type(post.return_value).status_code = PropertyMock(return_value=200)
        self.assertEquals(self.ckan_loader.upsert(None, None), 200)

    @patch('requests.post')
    def test_update_metadata(self, post):
        type(post.return_value).status_code = PropertyMock(return_value=200)
        self.assertEquals(self.ckan_loader.update_metadata(None), 200)

class TestCKANUpsertLoader(TestCKANDatastoreBase):
    def setUp(self):
        super(TestCKANUpsertLoader, self).setUp()
        self.upsert_loader = pl.CKANUpsertLoader(
            self.pipeline.get_config(), fields=[]
        )

    def test_upsert_loader_no_fields(self):
        with self.assertRaises(RuntimeError):
            pl.CKANUpsertLoader(self.pipeline.get_config())

    @patch('requests.post')
    def test_upsert_load_successful(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post
        self.upsert_loader.load([])

    @patch('requests.post')
    def test_upsert_load_upsert_failed(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post
        type(post.return_value).status_code = PropertyMock(side_effect=[500, 200])

        with self.assertRaises(RuntimeError):
            self.upsert_loader.load([])

    @patch('requests.post')
    def test_upsert_load_update_metadata_failed(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post
        type(post.return_value).status_code = PropertyMock(side_effect=[200, 500])
        with self.assertRaises(RuntimeError):
            self.upsert_loader.load([])

