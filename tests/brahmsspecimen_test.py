import unittest
from ppdb_nba import ppdbNBA
import logging

class BrahmsspecimenTestCase(unittest.TestCase):

    source = 'brahms-specimen'
    config = {
                  'deltapath' : '/data/incremental',
                  'sources' :
                  {
                      'brahms-specimen' :
                      {
                         'table': 'brahmsspecimen',
                         'id': 'assemblageID',
                         'enrich': True,
                         'incremental': True,
                         'path': '/data/brahms-specimen/'
                      }
                  },
                  'postgres' :
                  {
                    'host' : 'postgres',
                    'user' : 'postgres',
                    'pass' : 'postgres',
                    'db'   : 'ppdb'
                  }
              }


    def __init__(self, *args, **kwargs):
        super(BrahmsspecimenTestCase, self).__init__(*args, **kwargs)
        logger = logging.getLogger('ppdb_nba')
        logger.setLevel(logging.ERROR)
        self.pp = ppdbNBA(config=self.config, source=self.source)

    def setUp(self):
        self.pp.clear_data(table=self.config.get('table') + "_current")

        # Vul de basis tabel
        self.pp.import_data(table=self.config.get('table') + "_current", datafile=self.config.get('path') + '/1-base.json')

        changes = self.pp.list_changes()

        self.pp.handle_changes()

    def test_same(self):
        # Vul de basis tabel
        self.pp.import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/2-same.json')

        changes = self.pp.list_changes()

        self.pp.handle_changes()

        # Test of er geen verschillen zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)


    def test_new(self):
        self.pp.import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/3-new.json')
        changes = self.pp.list_changes()

        self.pp.handle_changes()

        # Test of er m aantal nieuwe records zijn
        self.assertEqual(len(changes['new']), 89)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

    def test_updates(self):
        self.pp.import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/4-updates.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 10)

        self.pp.handle_changes()


    def test_updatesnew(self):
        self.pp.import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/5-updatesnew.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 5)
        self.assertEqual(len(changes['update']), 10)

        self.pp.handle_changes()


    def test_deletes(self):
        import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/6-deletes.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 10)
        self.assertEqual(len(changes['update']), 0)

        changes = self.pp.handle_changes()



if __name__ == '__main__':
    unittest.main()

