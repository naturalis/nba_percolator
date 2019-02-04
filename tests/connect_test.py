import unittest
import logging
import os
from ppdb_nba import ppdb_NBA

class ConnectTestCase(unittest.TestCase):

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
        super(ConnectTestCase, self).__init__(*args, **kwargs)
        logger = logging.getLogger('ppdb_nba')
        logger.setLevel(logging.ERROR)
        self.pp = ppdb_NBA(config=self.config)
        self.pp.set_source(self.source)
        try:
            self.pp.generate_mapping(create_tables=True)
        except:
            pass

    def test_lock(self):
        self.pp.lock('test')

        jobsPath = self.pp.config.get('paths').get('jobs')
        exists = os.path.isfile(os.path.join(jobsPath, '.lock'), 'r')
        self.assertTrue(exists)

        self.pp.unlock()
        jobsPath = self.pp.config.get('paths').get('jobs')
        exists = os.path.isfile(os.path.join(jobsPath, '.lock'), 'r')
        self.assertFalse(exists)



if __name__ == '__main__':
    unittest.main()
