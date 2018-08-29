import unittest
from time import sleep
from ppdb_nba import *

class BrahmsspecimenTestCase(unittest.TestCase):

    source = 'brahms-specimen'
    config = {'table': 'brahmsspecimen',
     'id': 'assemblageID',
     'index': 'specimen_test',
     'doctype': 'Specimen',
     'enrich': True,
     'elastic': True,
     'incremental': False,
     'path': '/data/brahms-specimen/'}

    def __init__(self, *args, **kwargs):
        super(BrahmsspecimenTestCase, self).__init__(*args, **kwargs)
        logger = logging.getLogger('ppdb_nba')
        logger.setLevel(logging.ERROR)

    def setUp(self):
        clear_data(table=self.config.get('table') + "_current")
        kill_index(self.config)

        # Vul de basis tabel
        import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/1-base.json')

        # Importeer in elastic search
        changes = handle_changes(self.config)

    def test_same(self):
        # Vul de basis tabel
        import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/2-same.json')
        changes = handle_changes(self.config)

        # Test of er geen verschillen zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

        sleep(2)
        res = es.search(index=self.config.get('index'), body={"query" : {"match_all" : {}}})
        self.assertEqual(res['hits']['total'], 100)

    def test_new(self):
        import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/3-new.json')
        changes = handle_changes(self.config)

        # Test of er m aantal nieuwe records zijn
        self.assertEqual(len(changes['new']), 89)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

        sleep(2)
        res = es.search(index=self.config.get('index'), body={"query" : {"match_all" : {}}})
        self.assertEqual(res['hits']['total'], 189)

    def test_updates(self):
        import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/4-updates.json')
        changes = handle_changes(self.config)

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 10)

        sleep(2)
        res = es.search(index=self.config.get('index'), body={"query" : {"match_all" : {}}})
        self.assertEqual(res['hits']['total'], 100)

    def test_updatesnew(self):
        import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/5-updatesnew.json')
        changes = handle_changes(self.config)

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 5)
        self.assertEqual(len(changes['update']), 10)

        sleep(2)
        res = es.search(index=self.config.get('index'), body={"query" : {"match_all" : {}}})
        self.assertEqual(res['hits']['total'], 105)

    def test_deletes(self):
        import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/6-deletes.json')
        changes = handle_changes(self.config)

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 10)
        self.assertEqual(len(changes['update']), 0)

        sleep(2)
        res = es.search(index=self.config.get('index'), body={"query" : {"match_all" : {}}})
        self.assertEqual(res['hits']['total'], 90)


if __name__ == '__main__':
    unittest.main()

