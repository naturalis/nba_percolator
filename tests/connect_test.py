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
        exists = os.path.isfile(os.path.join(jobsPath, '.lock'))
        self.assertTrue(exists)

        locked = self.pp.is_locked()
        self.assertTrue(locked)

        self.pp.unlock()
        jobsPath = self.pp.config.get('paths').get('jobs')
        exists = os.path.isfile(os.path.join(jobsPath, '.lock'))
        self.assertFalse(exists)

    def test_lockdatafile(self):
        lock = self.pp.lock_datafile('test')
        self.assertTrue(lock)
        lock = self.pp.lock_datafile('test')
        self.assertFalse(lock)
        lock = self.pp.unlock_datafile('test')
        self.assertTrue(lock)
        lock = self.pp.unlock_datafile('test')
        self.assertTrue(lock)

    def test_incremental(self):
        self.assertFalse(self.pp.is_incremental())

    def test_parsejob(self):
        json = "{}"
        files = self.pp.parse_job(json)
        self.assertEquals(len(files.keys()), 0)

        json = '{"id":"1234","data_supplier":"XC","validator":{"results":{"outfiles":{"valid":"test"}}}}'
        files = self.pp.parse_job(json)
        self.assertEquals(len(files.keys()), 1)




if __name__ == '__main__':
    unittest.main()
