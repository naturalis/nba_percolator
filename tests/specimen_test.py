import unittest
from ppdb_nba import ppdb_NBA
from pony.orm.core import MappingError
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
    mapping = False

    def __init__(self, *args, **kwargs):
        super(SpecimenTestCase, self).__init__(*args, **kwargs)
        logger = logging.getLogger('ppdb_nba')
        logger.setLevel(logging.ERROR)
        self.pp = ppdb_NBA(config=self.config)
        self.pp.set_source(self.source)
        try:
            self.pp.generate_mapping(create_tables=True)
        except MappingError:
            pass

    def setUp(self):
        self.pp.clear_data(table=self.pp.sourceConfig.get('table') + "_current")

        # Vul de basis tabel
        self.pp.import_data(table=self.pp.sourceConfig.get('table') + "_import", datafile=self.pp.sourceConfig.get('path') + '/1-base.json')

        changes = self.pp.list_changes()

        self.pp.handle_changes()

    def test_same(self):
        # Vul de basis tabel
        self.pp.import_data(table=self.pp.sourceConfig.get('table') + "_import", datafile=self.pp.sourceConfig.get('path') + '/2-same.json')

        changes = self.pp.list_changes()

        # Test of er geen verschillen zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes()

    def test_new(self):
        self.pp.import_data(table=self.pp.sourceConfig.get('table') + "_import", datafile=self.pp.sourceConfig.get('path') + '/3-new.json')

        changes = self.pp.list_changes()

        # Test of er m aantal nieuwe records zijn
        self.assertEqual(len(changes['new']), 100)
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes()

    def test_updates(self):
        self.pp.import_data(table=self.pp.sourceConfig.get('table') + "_import", datafile=self.pp.sourceConfig.get('path') + '/4-updates.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['delete']), 0)
        self.assertEqual(len(changes['update']), 10)

        self.pp.handle_changes()

    def test_updatesnew(self):
        self.pp.import_data(table=self.pp.sourceConfig.get('table') + "_import", datafile=self.pp.sourceConfig.get('path') + '/5-updatesnew.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 10)
        self.assertEqual(len(changes['update']), 10)

        self.pp.handle_changes()

    def test_deletes(self):
        self.pp.import_data(table=self.pp.sourceConfig.get('table') + "_import", datafile=self.pp.sourceConfig.get('path') + '/6-deletes.json')

        changes = self.pp.list_changes()

        # Test of er n aantal records zijn
        self.assertEqual(len(changes['new']), 0)
        self.assertEqual(len(changes['delete']), 10)
        self.assertEqual(len(changes['update']), 0)

        self.pp.handle_changes()


if __name__ == '__main__':
    unittest.main()
