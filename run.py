#!/usr/bin/env python3
import logging
import os
import io
import json

import google.cloud.bigquery as bigquery
import google.oauth2.credentials
import requests

import flywheel
from flywheel.api import ViewsApi


log = logging.getLogger('flywheel:bq-export')


def main(context):
    log.setLevel(getattr(logging, context.config['log_level']))
    google_project_id = context.config['gcp_project']
    dataset_id = context.config['dataset']
    table_id = context.config['table']

    api_key = context.get_input('key')['key']
    api_uri = api_key.rsplit(':', 1)[0]
    if not api_uri.startswith('http'):
        api_uri = 'https://' + api_uri

    views_api = ViewsApi(context.client.api_client)
    container_id = context.config.get('container_id')
    view_id = context.config.get('saved_view_id')
    json_input = context.get_input_path('view_spec_json')
    csv_input = context.get_input_path('csv_input_file')
    container_id = context.client.lookup(container_id)['_id']

    if json_input:
        data = views_api.evaluate_view_adhoc(container_id, body=json.loads(json_input),
                                             format='csv', _preload_content=False,
                                             _return_http_data_only=True).content.decode('utf-8')
    elif view_id:
        data = views_api.evaluate_view(view_id, container_id,
                                       format='csv', _preload_content=False,
                                       _return_http_data_only=True).content.decode('utf-8')
    elif csv_input:
        with context.open_input('csv_input_file', 'r') as source_file:
            data = source_file.read()
            # job = bq.load_table_from_file(source_file, table, job_config=job_config)
    else:
        print('Nothing to do. Exiting ...')
        exit(0)

    # get google access token from core and create bigquery client with it
    # (using requests for non-sdk-accessible core endpoint)
    response = requests.get(
        api_uri + '/api/users/self/tokens/' + context.config['gcp_token_id'],
        headers={'Authorization': 'scitran-user ' + api_key})
    response.raise_for_status()
    gcp_access_token = response.json()['access_token']
    credentials = google.oauth2.credentials.Credentials(gcp_access_token)
    bq = bigquery.Client(google_project_id, credentials)

    # create new bigquery dataset if it doesn't exist yet
    if dataset_id not in [ds.dataset_id for ds in bq.list_datasets()]:
        log.debug('creating bigquery dataset %s', dataset_id)
        bq.create_dataset(dataset_id)

    # get table and create job config
    table = bq.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        ignore_unknown_values=False,
        maxBadRecords=0,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition='WRITE_TRUNCATE',
    )

    # TODO consider CSV encoding/separator detection
    # TODO finalize Zsolt's code below...
    # with context.open_input('input_file', 'rb') as csv:
    #    lines = [line.decode() for line in csv]
    #    if 'num' not in lines[0]:
    #        lines[0] = '{},{}'.format('num,', lines[0])
    #        for num, line in enumerate(lines[1:], 1):
    #            lines[num] = '{},{}'.format(num, line)

    numbered_data = []
    for i, row in enumerate(data.split('\n')):
        if i == 0:
            numbered_data.append('num,' + row)
        elif row:
            numbered_data.append('{},{}'.format(i-1, row))
    data = '\n'.join(numbered_data)
    data = data + '\n'

    job = bq.load_table_from_file(io.BytesIO(data.encode('utf-8')),
                                            table,
                                            job_config=job_config,
                                            rewind=True)

    job.result()
    log.info('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))


def enable_docker_local_access(context):
    """Enable accessing docker.local.flywheel.io within a gear (ie. in development)"""
    if 'docker.local.flywheel.io' in context.get_input('key').get('key', ''):
        if os.path.exists('docker_host'):
            docker_host = open('docker_host').read().strip()
            with open('/etc/hosts', 'a') as hosts:
                hosts.write(docker_host + '\tdocker.local.flywheel.io\n')
        else:
            cmd = "ip -o route get to 8.8.8.8 | sed 's/^.*src \([^ ]*\).*$/\1/;q' > docker_host"
            log.warning('cannot patch /etc/hosts with docker.local.flywheel.io - docker_host file not found. '
                        "Run the following command to create the file in your gear's root dir: \n%s", cmd)


if __name__ == '__main__':
    with flywheel.GearContext() as context:
        enable_docker_local_access(context)
        context.init_logging()
        context.log_config()
        main(context)
