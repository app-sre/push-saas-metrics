#!/usr/bin/env

import base64
import logging
import os

import toml
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from prometheus_client.exposition import basic_auth_handler

from git_metrics import SaasGitMetrics
from gql import GqlApi
import vault_client


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

# TODO: For some reason it's faster if pool size is 0
POOL_SIZE = os.getenv('POOL_SIZE', 0)

# Clone the repos to this path
CACHE_DIR = os.getenv('CACHE_DIR', '.cache')


def get_saas_repos(config):
    gql = GqlApi(config['graphql']['server'], config['graphql']['token'])

    query = """
        {
            apps: apps_v1 {
                codeComponents {
                name
                resource
                url
                }
            }
        }
    """
    apps = gql.query(query)['apps']

    return [
        c['url']
        for app in apps
        for c in (app.get('codeComponents', {}) or {})
        if c['resource'] == "saasrepo"
    ]


def init_vault_client(config):
    v_server = config['vault']['server']
    v_role_id = config['vault']['role_id']
    v_secret_id = config['vault']['secret_id']
    vault_client.init(v_server, v_role_id, v_secret_id)


def pgw_auth_handler(pgw_config):
    def my_auth_handler(url, method, timeout, headers, data):
        return basic_auth_handler(url,
                                  method,
                                  timeout,
                                  headers,
                                  data,
                                  pgw_config['username'],
                                  pgw_config['password'])

    return my_auth_handler


if __name__ == "__main__":
    config = toml.loads(base64.b64decode(os.environ['CONFIG_TOML']))
    init_vault_client(config)
    pgw_config = vault_client.read_all(config['pushgateway']['secret_path'])

    registry = CollectorRegistry()

    labels = ['context', 'service']

    g_upstream_commits = Gauge('saas_upstream_commits',
                               'number of commits in the upstream repo',
                               labels, registry=registry)

    g_commit_index = Gauge('saas_commit_index',
                           'commit number in upstream of the last promoted to '
                           'prod commit',
                           labels, registry=registry)

    g_commit_ts = Gauge('saas_commit_ts',
                        'timestamp of the last promoted to prod commit '
                        '(in upstream)',
                        labels, registry=registry)

    for saas_repo in get_saas_repos(config):
        logging.info(['processing', saas_repo])

        sgm_repo = SaasGitMetrics(saas_repo, POOL_SIZE, cache=CACHE_DIR)
        services = sgm_repo.services_hash_history()

        for s in services:
            context = s['context']
            name = s['name']

            g_upstream_commits.labels(
                context=context,
                service=name
            ).set(s['upstream_commits'])

            g_commit_index.labels(
                context=context,
                service=name
            ).set(s['upstream_saas_commit_index'])

            g_commit_ts.labels(
                context=context,
                service=name
            ).set(s['commit_ts'])

    push_to_gateway(config['pushgateway']['server'],
                    job='saas_metrics',
                    registry=registry,
                    handler=pgw_auth_handler(pgw_config))
