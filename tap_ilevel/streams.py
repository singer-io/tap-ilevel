# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
#   path: API endpoint relative path, when added to the base URL, creates the full path
#   key_properties: Primary key field(s) for the object endpoint
#   replication_method: FULL_TABLE or INCREMENTAL
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#        and setting the state
#   params: Query, sort, and other endpoint specific parameters
#   data_key: JSON element containing the records for the endpoint
#   bookmark_query_field: Typically a date-time field used for filtering the query
#   bookmark_type: Data type for bookmark, integer or datetime
#   children: A collection of child endpoints (where the endpoint path includes the parent id)
#   parent: On each of the children, the singular stream name for parent element
STREAMS = {

    'asset_to_asset_relations': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },

    'assets': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },

    'data_items': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },

    'fund_to_asset_relations': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },

    'fund_to_fund_relations': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },

    'funds': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },

    'investment_transactions': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified']
    },

    'investments': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },

    'scenarios': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },

    'securities': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },

    'periodic_data_standardized': {
        'key_properties': ['hash_key'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['reported_date_value']
    },

    'periodic_data_calculated': {
        'key_properties': ['hash_key'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['reported_date_value']
    }
}
