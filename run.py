#!/usr/bin/env python3
import base64
import json
import logging
import pprint
import sys
from collections import OrderedDict

import flywheel
from flywheel.api import ViewsApi
from requests_toolbelt import MultipartEncoder


log = logging.getLogger('flywheel:bq-export')
logging.basicConfig(
    format='%(asctime)s %(name)15.15s %(levelname)4.4s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)

class MultipartRelatedEncoder(MultipartEncoder):
    """A multipart/related encoder"""

    @property
    def content_type(self):
        return str(
            'multipart/related; boundary={0}'.format(self.boundary_value)
        )

    def _iter_fields(self):
        # change content-disposition from form-data to attachment
        for field in super(MultipartRelatedEncoder, self)._iter_fields():
            # content_type = field.headers['Content-Type']
            field.headers['Content-Disposition'] = None
            if field.headers['Content-Type'] == 'text/csv':
                field.headers['Content-Transfer-Encoding'] = 'base64'
            yield field


def main(context):
    api_key = inputs['key']['key']
    container_id = context.config.get('container_id')
    google_project_id = context.config.get('project_id')
    dataset = context.config.get('dataset')
    table = context.config.get('table')
    json_input = context.config.get('json')
    view_id = context.config.get('view_id')

    views_api = ViewsApi(context.client.api_client)

    if json_input:
        data = views_api.evaluate_view_adhoc(container_id, body=json.loads(json_input),
                                             format='csv', _preload_content=False,
                                             _return_http_data_only=True).content.decode('utf-8')
    elif view_id:
        data = views_api.evaluate_view(view_id, container_id,
                                       format='csv', _preload_content=False,
                                       _return_http_data_only=True).content.decode('utf-8')
    else:
        exit(1)
        return

    # TODO: get api key in a better way
    # try to get current user Google oauth2 access token, if user is not logged in use core's
    # default credentials
    try:
        resp = fw.api_client.call_api('/gcp/auth/token', 'GET', **{
            'auth_settings': ['ApiKey'],
            '_return_http_data_only': True,
            '_preload_content': False
        })
        bq_token = resp.json()['access_token']
        log.info("Use user's google credentials")
    except flywheel.ApiException:
        bq_token = fw.api_client.call_api('/gcp/token', 'GET', **{
            'auth_settings': ['ApiKey'],
            '_return_http_data_only': True,
            '_preload_content': False
        }).json()['token']
        log.info("Use core's google service account")

    # TODO: gcp removed, use official bigquery api client lib
    bq = gcp.BigQuery(google_project_id, bq_token)

    if dataset not in bq.list_datasets():
        log.debug('creating bq dataset %s', dataset)
        bq.create_dataset(dataset)

    load_config = {
        'configuration': {
            'load': {
                'autodetect': True,
                'destinationTable': {
                    'projectId': google_project_id,
                    'datasetId': dataset,
                    'tableId': table
                },
                'writeDisposition': 'WRITE_TRUNCATE',
                'ignoreUnknownValues': False,
                'maxBadRecords': 0,
                'sourceFormat': 'CSV'
            }
        }
    }

    # numbering rows
    numbered_data = []
    for i, row in enumerate(data.split('\n')):
        if i == 0:
            numbered_data.append('num,' + row)
        elif row:
            numbered_data.append('{},{}'.format(i-1, row))
    data = '\n'.join(numbered_data)
    data = data + '\n'

    m = MultipartRelatedEncoder(
        fields=OrderedDict({
            'a': (None, json.dumps(load_config), 'application/json; charset=UTF-8'),
            'b': ('test.csv', base64.b64encode(data.encode('utf-8')), 'text/csv'),
        })
    )

    gcp_session = gcp.GCPSession('https://www.googleapis.com/upload/bigquery/v2/projects/healthcare-api-214323',
                                 bq_token)
    resp = gcp_session.post('/jobs?uploadType=multipart', data=m, headers={'Content-Type': m.content_type})
    bq.get_job(resp.json()['jobReference']['jobId'])


if __name__ == '__main__':
    with flywheel.GearContext() as context:
        context.init_logging()
        context.log_config()
        main(context)
