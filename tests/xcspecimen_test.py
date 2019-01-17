import unittest
from ppdb_nba import ppdb_NBA
import logging

class XcspecimenTestCase(unittest.TestCase):

    source = 'xenocanto-specimen'
    config = {
        'deltapath' : '/data/incremental',
        'sources' :
        {
            'xenocanto-specimen' :
            {
                 'table': 'xenocantospecimen',
                 'id': 'id',
                 'enrich': True,
                 'incremental': False,
                 'path': '/data/xenocanto-specimen'
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
        super(XcspecimenTestCase, self).__init__(*args, **kwargs)
        logger = logging.getLogger('ppdb_nba')
        logger.setLevel(logging.ERROR)
        self.pp = ppdb_NBA(config=self.config, source=self.source)

    def setUp(self):
        self.clear_data(table=self.config.get('table') + "_current")

        # Vul de basis tabel
        self.import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/1-base.json')

        changes = self.pp.list_changes()

        self.pp.handle_changes()


    def test_same(self):
        # Vul de basis tabel
        self.pp.import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/2-same.json')

        changes = self.pp.list_changes()

        # Test of er geen verschillen zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes()

    def test_new(self):
        self.pp.import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/3-new.json')

        changes = self.pp.list_changes()

        # Test of er m aantal nieuwe records zijn
        self.assertEqual(len(changes['new']), 100)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes()

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
        self.assertEqual(len(changes['new']), 10)
        self.assertEqual(len(changes['update']), 10)

        self.pp.handle_changes()

    def test_deletes(self):
        self.pp.import_data(table=self.config.get('table') + "_import", datafile=self.config.get('path') + '/6-deletes.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 10)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes(self.config)


if __name__ == '__main__':
    unittest.main()

