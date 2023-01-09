import os

SAME_THING_SERVICE_URL = os.environ.get('SAME_THING_SERVICE_URL', 'https://smart.hum.uva.nl/same-thing/lookup/')
SOLR_URL = os.environ.get('SOLR_URL', 'http://localhost:8983/solr')
SOLR_COLLECTION = os.environ.get('SOLR_COLLECTION', 'opentapioca-1')
