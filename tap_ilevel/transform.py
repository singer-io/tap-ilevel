import re
import hashlib
import humps


#  camelCase to snake_case for fieldname keys
def transform_json(records):
    transformed_json = humps.decamelize(records)
    return transformed_json


# Create MD5 hash key for data element
def hash_data(data):
    # Prepare the project id hash
    hash_id = hashlib.md5()
    hash_id.update(repr(data).encode('utf-8'))
    return hash_id.hexdigest()
