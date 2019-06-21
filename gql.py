import json

from graphqlclient import GraphQLClient


class GqlApiError(Exception):
    pass


class GqlApi(object):
    def __init__(self, url, token=None):
        self.url = url
        self.token = token

        self.client = GraphQLClient(self.url)
        self.client.inject_token(token)

    def query(self, query, variables=None):
        result_json = self.client.execute(query, variables)
        result = json.loads(result_json)

        if 'errors' in result:
            raise GqlApiError(result['errors'])

        if 'data' not in result:
            raise GqlApiError((
                "`data` field missing from GraphQL"
                "server response."))

        return result['data']


