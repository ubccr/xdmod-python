class _Descriptors:
    def __init__(self, http_requester):
        self.__http_requester = http_requester
        self.__aggregate = None
        self.__raw = None

    def _get_aggregate(self):
        if self.__aggregate is None:
            self.__aggregate = self.__request_aggregate()
        return self.__aggregate

    def _get_raw(self):
        if self.__raw is None:
            self.__raw = self.__request_raw()
        return self.__raw

    def __request_aggregate(self):
        response = self.__http_requester._request_json(
            '/controllers/metric_explorer.php',
            {'operation': 'get_dw_descripter'},
        )
        if response['totalCount'] != 1:
            raise RuntimeError(
                'Descriptor received with unexpected structure.'
            )
        return self.__deserialize_aggregate(response['data'][0]['realms'])

    def __request_raw(self):
        response = self.__http_requester._request_json(
            '/rest/v1/warehouse/export/realms'
        )
        return self.__deserialize_raw(response['data'])

    def __deserialize_aggregate(self, serialized_descriptor):
        result = {}
        for realm in serialized_descriptor:
            result[realm] = {'label': serialized_descriptor[realm]['category']}
            for m_or_d in ('metrics', 'dimensions'):
                m_or_d_descriptor = serialized_descriptor[realm][m_or_d]
                result[realm][m_or_d] = {}
                for id_ in m_or_d_descriptor:
                    result[realm][m_or_d][id_] = {
                        'label': m_or_d_descriptor[id_]['text'],
                        'description': m_or_d_descriptor[id_]['info'],
                    }
        return result

    def __deserialize_raw(self, serialized_descriptor):
        result = {}
        for realm in serialized_descriptor:
            realm_id = realm['id']
            result[realm_id] = {'label': realm['name']}
            result[realm_id]['fields'] = {}
            fields = realm['fields']
            for field in fields:
                result[realm_id]['fields'][field['alias']] = {
                    'label': field['display'],
                    'description': field['documentation'],
                }
        return result
