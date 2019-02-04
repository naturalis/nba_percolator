import unittest
from ppdb_nba import ppdb_NBA
import logging

class SpecimenTestCase(unittest.TestCase):

    source = 'xc-specimen'
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
            'xc-specimen':
            {
                 'table': 'xenocantospecimen',
                 'id': 'id',
                 'enrich': True,
                 'code': 'XC',
                 'incremental': False
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
        super(SpecimenTestCase, self).__init__(*args, **kwargs)
        logger = logging.getLogger('ppdb_nba')
        logger.setLevel(logging.ERROR)
        self.pp = ppdb_NBA(config=self.config)

    def setUp(self):
        source_config = self.config.get('sources').get(self.source)
        self.pp.clear_data(table=source_config.get('table') + "_current")

        # Vul de basis tabel
        self.pp.import_data(table=source_config.get('table') + "_import", datafile=source_config.get('path') + '/1-base.json')

        changes = self.pp.list_changes()

        self.pp.handle_changes()


    def test_same(self):
        source_config = self.config.get('sources').get(self.source)
        # Vul de basis tabel
        self.pp.import_data(table=source_config.get('table') + "_import", datafile=source_config.get('path') + '/2-same.json')

        changes = self.pp.list_changes()

        # Test of er geen verschillen zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes()

    def test_new(self):
        source_config = self.config.get('sources').get(self.source)
        self.pp.import_data(table=source_config.get('table') + "_import", datafile=source_config.get('path') + '/3-new.json')

        changes = self.pp.list_changes()

        # Test of er m aantal nieuwe records zijn
        self.assertEqual(len(changes['new']), 100)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes()

    def test_updates(self):
        source_config = self.config.get('sources').get(self.source)
        self.pp.import_data(table=source_config.get('table') + "_import", datafile=source_config.get('path') + '/4-updates.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 10)

        self.pp.handle_changes()

    def test_updatesnew(self):
        source_config = self.config.get('sources').get(self.source)
        self.pp.import_data(table=source_config.get('table') + "_import", datafile=source_config.get('path') + '/5-updatesnew.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 10)
        self.assertEqual(len(changes['update']), 10)

        self.pp.handle_changes()

    def test_deletes(self):
        source_config = self.config.get('sources').get(self.source)
        self.pp.import_data(table=source_config.get('table') + "_import", datafile=source_config.get('path') + '/6-deletes.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 10)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes(self.config)


if __name__ == '__main__':
    unittest.main()
