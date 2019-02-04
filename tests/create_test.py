import unittest
import logging
from ppdb_nba import ppdb_NBA

class CreateTestCase(unittest.TestCase):

    source = 'specimen'
    config = {
        'elastic': {
            'host': 'elasticsearch'
        },
        'paths': {
            'incoming': '/shared-data/incoming',
            'processed': '/shared-data/processed',
            'jobs': '/shared-data/jobs',
            'failed': '/shared-data/failed',
            'done': '/shared-data/done',
            'delta': '/shared-data/incremental'
        },
        'sources':
            {
                'specimen':
                    {
                        'table': 'xenocantospecimen',
                        'id': 'id',
                        'enrich': True,
                        'code': 'XC',
                        'incremental': False,
                        'path': '/shared-data/test'
                    }
            },
        'postgres':
            {
                'host': 'postgres',
                'user': 'postgres',
                'pass': 'postgres',
                'db': 'ppdb'
            }
    }

    def __init__(self, *args, **kwargs):
        super(CreateTestCase, self).__init__(*args, **kwargs)
        logger = logging.getLogger('ppdb_nba')
        logger.setLevel(logging.ERROR)
        self.pp = ppdb_NBA(config=self.config)
        self.pp.set_source(self.source)
        try:
            self.pp.generate_mapping(create_tables=True)
        except:
            pass

    def test_create_delete_record(self):
        recordID = "test123"
        status = 'REJECTED'
        deleteRecord = self.pp.create_delete_record(self.source, recordID, status)

        self.assertIsInstance(deleteRecord, dict)
        self.assertIsNotNone(deleteRecord.get('sourceSystemCode'))
        self.assertEqual(deleteRecord.get('unitID'), recordID)
        self.assertEqual(deleteRecord.get('status'), status)

    def test_create_name_summary(self):
        vernacularName = {
            'test': False,
            'name': 'vernacularName',
            'language': 'NL',
            'other': 'not important',
            'forgetit': 'removed'
        }

        nameSummary = self.pp.create_name_summary(vernacularName)
        self.assertIsInstance(nameSummary, dict)
        self.assertEqual(nameSummary.get('name'), vernacularName.get('name'))
        self.assertEqual(nameSummary.get('language'), vernacularName.get('language'))
        self.assertIsNone(nameSummary.get('other'))

    def test_create_scientific_summary(self):
        scientificName = {
            'test': False,
            'other': 'not important',
            'forgetit': 'removed',
            'fullScientificName': 'fullScientificName',
            'taxonomicStatus': 'taxonomicStatus',
            'genusOrMonomial': 'genusOrMonomial',
            'subgenus': 'subgenus',
            'specificEpithet': 'specificEpithet',
            'infraspecificEpithet': 'infraspecificEpithet',
            'authorshipVerbatim': 'authorshipVerbatim'
        }

        scientificSummary = self.pp.create_scientific_summary(scientificName)
        self.assertIsInstance(scientificSummary, dict)
        self.assertEqual(scientificSummary.get('fullScientificName'), scientificName.get('fullScientificName'))
        self.assertEqual(scientificSummary.get('subgenus'), scientificName.get('subgenus'))
        self.assertIsNone(scientificSummary.get('other'))

    def test_create_enrichment(self):
        vernacularName = {
            'test': False,
            'name': 'vernacularName',
            'language': 'NL',
            'other': 'not important',
            'forgetit': 'removed'
        }
        scientificName = {
            'test': False,
            'other': 'not important',
            'forgetit': 'removed',
            'fullScientificName': 'fullScientificName',
            'taxonomicStatus': 'taxonomicStatus',
            'genusOrMonomial': 'genusOrMonomial',
            'subgenus': 'subgenus',
            'specificEpithet': 'specificEpithet',
            'infraspecificEpithet': 'infraspecificEpithet',
            'authorshipVerbatim': 'authorshipVerbatim'
        }
        rec = {
            'id': 'TEST123',
            'sourceSystem': {
                'code': 'TEST'
            },
            'acceptedName': {
                'scientificNameGroup': 'scientificNameGroup'
            },
            'defaultClassification': 'test',
            'vernacularNames': [vernacularName],
            'synonyms': [scientificName]
        }
        enrichment = self.pp.create_enrichment(rec, 'test')

        self.assertIsInstance(enrichment, dict)
        self.assertIsNotNone(enrichment.get('taxonId'))
        self.assertIsNotNone(enrichment.get('synonyms'))
        self.assertIsNotNone(enrichment.get('sourceSystem'))
        self.assertIsNotNone(enrichment.get('sourceSystem').get('code'))
        self.assertIsNone(enrichment.get('defaultClassification'))

    def test_create_col_enrichment(self):
        vernacularName = {
            'test': False,
            'name': 'vernacularName',
            'language': 'NL',
            'other': 'not important',
            'forgetit': 'removed'
        }
        scientificName = {
            'test': False,
            'other': 'not important',
            'forgetit': 'removed',
            'fullScientificName': 'fullScientificName',
            'taxonomicStatus': 'taxonomicStatus',
            'genusOrMonomial': 'genusOrMonomial',
            'subgenus': 'subgenus',
            'specificEpithet': 'specificEpithet',
            'infraspecificEpithet': 'infraspecificEpithet',
            'authorshipVerbatim': 'authorshipVerbatim'
        }
        rec = {
            'id': 'TEST123',
            'sourceSystem': {
                'code': 'COL'
            },
            'acceptedName': {
                'scientificNameGroup': 'scientificNameGroup'
            },
            'defaultClassification': 'test',
            'vernacularNames': [vernacularName],
            'synonyms': [scientificName]
        }
        enrichment = self.pp.create_enrichment(rec, 'test')
        self.assertIsNotNone(enrichment.get('defaultClassification'))

    def test_cache_taxon(self):
        systemCode = 'XC'
        rec = {
            'acceptedName': {
                'scientificNameGroup': 'TEST'
            },
            'id': 'test123'
        }
        self.pp.cache_taxon_record(rec, systemCode)

        taxon = self.pp.get_taxon(self.source, 'TEST')

        print(taxon)
