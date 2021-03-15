# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest

import pytest
import requests_mock
from requests import HTTPError

from nifconverter.dbpedia_samething import SameThingConverter
from nifconverter.index_sameas_uri_converter import IndexSameAsUriConverter
from nifconverter.settings import SAME_THING_SERVICE_URL
from nifconverter.tests.dbpedia_bases import DBpediaTestBase


class IndexSameAsUriConverterTest(DBpediaTestBase.FromDBpediaBase):
    @classmethod
    def setUpClass(cls):
        cls.converter = IndexSameAsUriConverter('http://www.wikidata.org/entity/')

    def test_single_redirect(self):
        target_uri = self.converter.convert_one('http://dbpedia.org/resource/Apple')
        assert 'http://www.wikidata.org/entity/Q89' == target_uri

