import requests
from tenacity import retry, wait_exponential

from nifconverter.settings import SOLR_URL, SOLR_COLLECTION
from nifconverter.uriconverter import URIConverter


class IndexSameAsUriConverter(URIConverter):

    def __init__(self, target_prefix):
        super(IndexSameAsUriConverter, self).__init__(target_prefix)

    def is_convertible(self, uri):
        """
        Is this URI convertible by this converter?
        """
        return True

    @retry(wait=wait_exponential(multiplier=1, min=2, max=30))
    def convert_one(self, uri):
        """
        Convert one URI (assumed to be convertible).
        Returns the converted URI or None if the concept does
        not exist in the target domain.
        """
        url = "{SOLR_URL}/{SOLR_COLLECTION}/select".format(SOLR_URL=SOLR_URL, SOLR_COLLECTION=SOLR_COLLECTION)
        params = {'q': 'same_as_ss:"{}"'.format(uri)}
        r = requests.get(url, params)

        if 200 != r.status_code or 1 != r.json().get('response', {}).get('numFound', 0):
            return None
        else:
            return self.target_prefix + r.json().get('response').get('docs')[0].get('id')

    def convert(self, uris):
        """
        Converts a list of URIs, whose length should
        not be greater than batch_size.

        Returns a map of results (from the original uri to the target uri).
        """
        return {
            uri: self.convert_one(uri)
            for uri in uris
        }
