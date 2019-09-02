import mock
import pytest
from io import BytesIO, StringIO

import run

CONFIG = {
    'log_level': 'INFO',
    'project_id': 'Neuroscience',
    'gcp_token_id': '3Dsg94Af17',
    'gcp_project': 'healthcare-api',
    'dataset': 'test_dataset',
    'table': 'test_table',
    'view_spec_json': None,
    'saved_view_id': None
}

INPUT = {
    'csv_input_file': 'csv',
    'view_spec_json': None
}

@mock.patch('run.ViewsApi')
@mock.patch('run.bigquery')
@mock.patch('run.requests')
@mock.patch('run.io.BytesIO')
def test_main(mock_bytesio, mock_requests, mock_bigquery, MockViewsApi):
    mock_context = mock.Mock()
    mock_context.configure_mock(config=CONFIG)
    mock_context.get_input.return_value = {'key': 'docker.local.flywheel.io'}
    mock_context.client.lookup.return_value = {'_id': '000000000000000000000000'}
    mock_context.get_input_path = INPUT.get

    mock_requests.get.json.return_value = {'access_token': 'token'}

    test_data = 'header_1\nvalue1'
    mock_file = StringIO(test_data)
    mock_context.open_input.return_value.__enter__ = lambda *args: mock_file
    mock_context.open_input.return_value.__exit__ = lambda *args: None

    expected_load_job_config = mock_bigquery.LoadJobConfig()
    expected_load_job_config.autodetect=True
    expected_load_job_config.write_disposition='WRITE_TRUNCATE'
    expected_load_job_config.maxBadRecords=0
    expected_load_job_config.ignore_unknown_values=False
    expected_load_job_config.source_format='CSV'

    mock_bigquery.SourceFormat.CSV = 'CSV'
    mock_bq_client = mock_bigquery.Client.return_value = mock.Mock()
    mock_bq_client.list_datasets.return_value = []


    mock_bytesio.return_value = mock.Mock()

    with mock.patch('builtins.open', mock.mock_open(), create=True) as mock_builtin_open:
        run.main(mock_context)

    expected_export_data = b'num,header_1\n0,value1\n'
    mock_bq_client.create_dataset.assert_called_once_with('test_dataset')
    mock_bq_client.dataset.assert_called_once_with('test_dataset')
    mock_bq_client.dataset.return_value.table.assert_called_once_with('test_table')
    mock_bytesio.assert_called_once_with(expected_export_data)
    mock_bq_client.load_table_from_file.assert_called_once_with(mock_bytesio(),
                                                                mock_bq_client.dataset.return_value.table.return_value,
                                                                job_config=expected_load_job_config, rewind=True)
