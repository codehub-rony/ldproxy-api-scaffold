import unittest
from unittest.mock import MagicMock, patch
import os
import tempfile
from sqlalchemy.engine import Engine
from ldproxy_config_generator.config.sql_provider import SQLProvider

class TestSQLProvider(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.service_id = "test_service"
        self.force_axis_order = "LON_LAT"
        self.table_config = {
            "db_schema": "public",
            "tables": [
                {
                    "tablename": "test_table",
                    "columns": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "geom", "type": "GEOMETRY"},
                        {"name": "name", "type": "VARCHAR"},
                        {"name": "created_at", "type": "TIMESTAMP"}
                    ]
                }
            ]
        }
        self.db_config = {
            "DB_SCHEMA": "public",
            "DATABASE": "test_db",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password"
        }
        self.engine = MagicMock(spec=Engine)
        self.provider = SQLProvider(
            self.service_id,
            self.force_axis_order,
            self.table_config,
            self.engine,
            self.db_config
        )

    def test_initialization(self):
        """Test SQLProvider initialization."""
        self.assertEqual(self.provider.service_id, self.service_id)
        self.assertEqual(self.provider.force_axis_order, self.force_axis_order)
        self.assertEqual(self.provider.table_config, self.table_config)
        self.assertEqual(self.provider.db_config, self.db_config)
        self.assertFalse(self.provider.docker)

    def test_create_connection_info(self):
        """Test connection info creation."""
        connection_info = self.provider._create_connection_info()
        
        self.assertEqual(connection_info['dialect'], 'PGIS')
        self.assertEqual(connection_info['database'], self.db_config['DATABASE'])
        self.assertEqual(connection_info['host'], self.db_config['DB_HOST'])
        self.assertEqual(connection_info['user'], self.db_config['DB_USER'])
        self.assertEqual(connection_info['schemas'], [self.db_config['DB_SCHEMA']])
        
        # Test password encoding
        import base64
        decoded_password = base64.b64decode(connection_info['password']).decode()
        self.assertEqual(decoded_password, self.db_config['DB_PASSWORD'])

    def test_create_connection_info_docker(self):
        """Test connection info creation with docker flag."""
        provider = SQLProvider(
            self.service_id,
            self.force_axis_order,
            self.table_config,
            self.engine,
            self.db_config,
            docker=True
        )
        connection_info = provider._create_connection_info()
        self.assertEqual(connection_info['host'], 'host.docker.internal')

    @patch('ldproxy_config_generator.config.sql_provider.text')
    def test_get_geometry_type(self, mock_text):
        """Test geometry type retrieval."""
        # Mock the database response
        mock_result = MagicMock()
        mock_result.fetchone.return_value = ('MULTIPOLYGON',)
        self.engine.connect.return_value.__enter__().execute.return_value = mock_result

        geometry_type = self.provider._get_geometry_type('test_table')
        self.assertEqual(geometry_type, 'MULTI_POLYGON')

        # Test with ANY geometry type
        mock_result.fetchone.return_value = ('GEOMETRY',)
        geometry_type = self.provider._get_geometry_type('test_table')
        self.assertEqual(geometry_type, 'ANY')

    def test_create_table_properties(self):
        """Test table properties creation."""
        columns = [
            {"name": "id", "type": "INTEGER"},
            {"name": "geom", "type": "GEOMETRY"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "created_at", "type": "TIMESTAMP"}
        ]
        
        properties = self.provider._create_table_properties(columns, 'test_table')
        
        # Test ID property
        self.assertEqual(properties['id']['role'], 'ID')
        self.assertEqual(properties['id']['excludedScopes'], ['RECEIVABLE'])
        
        # Test geometry property
        self.assertEqual(properties['geom']['type'], 'GEOMETRY')
        self.assertEqual(properties['geom']['role'], 'PRIMARY_GEOMETRY')
        
        # Test datetime property
        self.assertEqual(properties['created_at']['role'], 'PRIMARY_INSTANT')
        
        # Test regular property
        self.assertEqual(properties['name']['type'], 'STRING')

    def test_create_types(self):
        """Test types creation."""
        self.provider._create_types()
        
        # Check if types were created correctly
        self.assertIn('test_table', self.provider.config['types'])
        table_config = self.provider.config['types']['test_table']
        
        self.assertEqual(table_config['sourcePath'], '/test_table')
        self.assertIn('properties', table_config)
        self.assertIn('id', table_config['properties'])
        self.assertIn('geom', table_config['properties'])

    @patch('ldproxy_config_generator.config.sql_provider.generate')
    def test_create_yaml(self, mock_generate):
        """Test YAML file creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            self.provider.create_yaml(output_dir=temp_dir)
            
            # Check if generate was called with correct arguments
            mock_generate.assert_called_once()
            
            # Check if output directory was created
            providers_dir = os.path.join(temp_dir, 'providers')
            self.assertTrue(os.path.exists(providers_dir))

if __name__ == '__main__':
    unittest.main() 