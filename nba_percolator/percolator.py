"""NBA percolator - the NBA pre processing database

This module contains all the database dependencies and functions used
for importing new and updated data into the NBA document store.
"""
import json
import logging
import os
import glob
import shutil
import sys
import requests
import time
import yaml
from timeit import default_timer as timer
from elasticsearch import Elasticsearch, ElasticsearchException, ConnectionError, TransportError
from pony.orm import db_session
from dateutil import parser
from diskcache import Cache
from .schema import *

logger = logging.getLogger('nba_percolator')

# Caching on disk (diskcache) using sqlite, it should be fast
cache = Cache('/tmp/percolator_cache')
cache.clear()


# noinspection SqlNoDataSourceInspection,SqlResolve,PyTypeChecker,PyUnresolvedReferences,SpellCheckingInspection
class Percolator:
    """
    Preprocessor class containing all functions needed for
    importing the data and create incremental insert, update
    or delete files.
    """

    def __init__(self, config):
        """
        Reading the config.yml file where all sources, configuration
        and specifics are listed
        """
        if isinstance(config, str):
            # config is string, read the config file
            try:
                with open(file=config, mode='r') as ymlFile:
                    self.config = yaml.load(ymlFile, Loader=yaml.BaseLoader)
            except Exception:
                msg = '"config.yml" is missing?'
                logger.fatal(msg)
                sys.exit(msg)
        elif isinstance(config, dict):
            # config is dictionary
            self.config = config
        else:
            # do not accept any other type
            msg = 'Config parameter should be dictionary or filename'
            logger.fatal(msg)
            sys.exit(msg)

        if not self.config.get('sources', False):
            msg = 'Sources part missing in config'
            logger.fatal(msg)
            sys.exit(msg)

        self.es = self.connect_to_elastic()
        self.connect_to_database()

        self.jobDate = datetime.now()

        self.job = False

        self.percolatorMeta = {}

        self.tabulaRasa = False

        self.jobId = ''
        self.source = ''
        self.supplier = ''
        self.filename = ''
        self.deltafiles = []
        self.noslack = False
        self.elastic_logging = True

        self.paths = self.config.get('paths')
        self.sourceConfig = {}

        self.delta_writable_test()

    def set_nologging(self):
        self.elastic_logging = False

    def set_source(self, source):
        """
        Setting the data source of the import (and it's source config)

        :param source:
        """

        self.source = source
        if self.config.get('sources').get(source):
            self.sourceConfig = self.config.get('sources').get(source)
        else:
            msg = 'Source "%s" does not exist in config file' % (source)
            sys.exit(msg)

    def set_metainfo(self, key='', value='', source=False, filename=False):
        if not source:
            source = self.source
        if not filename:
            filename = self.filename

        if not self.percolatorMeta.get(source, False):
            self.percolatorMeta[source] = {}

        if not self.percolatorMeta[source].get(filename, False):
            self.percolatorMeta[source][filename] = {}

        self.percolatorMeta[source][filename][key] = value

    def get_metainfo(self, key='', source=False, filename=False):
        if not source:
            source = self.source
        if not filename:
            filename = self.filename

        if self.percolatorMeta.get(source, False):
            if self.percolatorMeta[source].get(filename, False):
                return self.percolatorMeta[source][filename].get(key)

        return False

    def get_path(self, pathkey='', filename=''):
        path = self.paths.get(pathkey, False)
        if path:
            return os.path.join(path, filename)
        return False

    def add_deltafile(self, filepath):
        try:
            self.deltafiles.index(filepath)
        except ValueError:
            self.deltafiles.append(filepath)

    def connect_to_elastic(self):
        """
        Connect to elastic search for logging
        """
        host = os.environ.get('LOGGING_HOST')
        logger.debug('Connecting to elastic: {host}'.format(host=host))
        try:
            es = Elasticsearch(
                hosts=os.environ.get('LOGGING_HOST'),
                sniff_on_start=True,
                sniff_on_connection_fail=True,
                sniffer_timeout=30,
                retry_on_timeout=True,
                timeout=30,
                max_retries=10
            )
            logger.debug('Connected to elastic: {host}'.format(host=host))
            return es
        except ElasticsearchException:
            msg = 'Cannot connect to elastic search server (needed for logging)'
            logger.fatal(msg)
            self.slack('*Percolator* failed: {msg}'.format(msg=msg))
            sys.exit(msg)

    def connect_to_database(self):
        """
        Connects to postgres database
        """
        logger.debug('Connecting to database')

        global db

        self.db = db

        user = os.environ.get('DATABASE_USER')
        password = os.environ.get('DATABASE_PASSWORD')
        host = os.environ.get('DATABASE_HOST')
        database = os.environ.get('DATABASE_DB')
        if self.config.get('postgres'):
            user = self.config.get('postgres').get('user')
            password = self.config.get('postgres').get('pass')
            database = self.config.get('postgres').get('db')
            host = self.config.get('postgres').get('host')

        try:
            self.db.bind(
                provider='postgres',
                user=user,
                password=password,
                host=host,
                database=database
            )
        except TypeError:
            return
        except Exception:
            msg = 'Cannot connect to postgres database'
            logger.fatal(msg)
            self.slack('*Percolator* failed: {msg}'.format(msg=msg))
            sys.exit(msg)

        logger.debug('Connected to database: {database}'.format(
            database=database
        ))

    def generate_mapping(self, create_tables=False):
        """
        Generates mapping of the database
        """
        try:
            self.db.generate_mapping(create_tables=create_tables)
        except Exception:
            msg = 'Creating tables needed for preprocessing failed'
            logger.fatal(msg)
            self.slack('*Percolator* failed: {msg}'.format(msg=msg))
            sys.exit(msg)

    def is_incremental(self):
        return self.sourceConfig.get('incremental', 'yes') == 'yes'

    def lock(self, jobFile):
        """
        Generates a locking file
        """
        lockFilePath = self.get_path('jobs', '.lock')
        with open(lockFilePath, 'w') as lockFile:
            lockRecord = {
                'job': jobFile,
                'pid': os.getpid()
            }
            json.dump(lockRecord, lockFile)

    def unlock(self):
        """
        Removes the locking file
        """
        lockFilePath = self.get_path('jobs', '.lock')

        locks = glob.glob(lockFilePath)
        lock = locks.pop()
        if lock:
            os.remove(lock)

    def is_locked(self):
        """
        Checks if there's a lockfile and if so, parse it. A
        lockfile is a json record with PID as well as job
        filepath. If the process is still running then the
        process is still locked. If not it has failed.

        If no lock file exists it is no longer locked.

        :return:  True = still locked / False = no longer locked
        """
        lockFilePath = self.get_path('jobs', '.lock')

        locks = glob.glob(lockFilePath)
        if len(locks) > 0:
            lockFile = locks.pop()
            with open(file=lockFile, mode='r') as f:
                lockinfo = json.load(f)

            # check of the process in the lockfile is still running, kill signal=0
            # this does not kill the process, just checks if the process is there
            try:
                os.kill(lockinfo['pid'], 0)
                logger.info(
                    'Preprocessor still processing (PID={pid}), handling job file "{job}"'.format(pid=lockinfo['pid'],
                                                                                                  job=lockinfo['job']))
                return True
            except Exception:
                # Exception means the process is no longer running, but the
                # lockfile is still there
                jobFile = lockinfo['job'].split('/')[-1]
                failedFilePath = self.get_path('failed', jobFile)
                shutil.move(lockinfo['job'], failedFilePath)

                self.log_change(
                    state='fail'
                )
                logger.error(
                    'Preprocessor failed in the last run? '
                    'Job file "{job}" moved to failed'.format(job=lockinfo['job'])
                )
                os.remove(lockFile)

        return False

    def parse_job(self, jsonData='{}'):
        """
        Parse a json job file, and tries to retrieve the validated
        filenames then returns a dictionary of sources with a list
        of files to be processed.

        :rtype: object
        """
        files = {
            'imports': {},
            'deletes': {}
        }
        self.job = json.loads(jsonData)

        # Get the id of the job
        self.jobId = self.job.get('id')

        # tabulaRasa, if true, import straight to current
        self.tabulaRasa = self.job.get('tabula_rasa', False)

        # Get the name of the supplier
        self.supplier = self.job.get('data_supplier')

        # Get the date of the job
        rawdate = self.job.get('date', False)
        if rawdate:
            self.jobDate = parser.parse(rawdate)

        # Parse the validator part, get the outfiles
        if self.job.get('validator'):
            for key in self.job.get('validator').keys():
                export = self.job.get('validator').get(key)
                for validfile in export.get('results').get('outfiles').get('valid'):
                    source = self.supplier + '-' + key
                    if source not in files['imports']:
                        files['imports'][source] = []
                    files['imports'][source].append(validfile.split('/')[-1])

        if self.job.get('delete'):
            for key in self.job.get('delete').keys():
                for deletefile in self.job.get('delete').get(key):
                    source = self.supplier + '-' + key
                    if source not in files['deletes']:
                        files['deletes'][source] = []
                    files['deletes'][source].append(deletefile.split('/')[-1])

        return files

    def handle_job(self, jobFile='', tabulaRasa=False):
        """
        Handles the jobfile

        :param jobFile:
        :return:
        """
        global cache

        files = None
        with open(jobFile, "r") as fp:
            jsonData = fp.read()
            files = self.parse_job(jsonData)
            if tabulaRasa:
                self.tabulaRasa = True

        if files is None:
            return False

        self.lock(jobFile)
        self.slack('*Percolator* started `{job}`'.format(job=jobFile))

        # import each file
        if len(files['imports']):
            self.process_importfiles(files['imports'])

        if len(files['deletes']):
            self.process_deletefiles(files['deletes'])

        # everything is finished and okay, remove the lock
        self.finish_job()

        return True

    def process_importfiles(self, files):
        """
        Takes each import file and does an import

        :param files:
        """
        for source, filenames in files.items():
            for filename in filenames:
                self.filename = filename
                self.set_source(source.lower())

                filePath = self.get_path('incoming', filename)

                self.set_metainfo(key='in', value=filePath, source=source.lower(), filename=filename)

                #
                # self.log_change(
                #    state='import',
                #    comment='{filepath}'.format(filepath=filePath)
                # )

                if self.tabulaRasa:
                    self.tabularasa_import(filename, source)
                else:
                    self.normal_import(filename, source)

    def normal_import(self, filename, source):
        """
        Do a default import of a jsonlines file to a defined source

        :param filename:
        :param source:
        """
        filePath = self.get_path('incoming', filename)

        try:
            self.import_data(table=self.sourceConfig.get('table') + '_import', datafile=filePath)
        except Exception:
            # import fails? remove the lock, return false
            self.set_metainfo(key='status', value='failed', source=source.lower(), filename=filename)
            logger.error(
                "Import of '{file}' into '{source}' failed".format(file=filePath, source=source.lower())
            )
            # return False
        # import successful, move the data file
        processed_path = self.get_path('processed', filename)
        self.set_metainfo(key='out', value=processed_path, source=source.lower(), filename=filename)

        shutil.move(filePath, processed_path)
        self.remove_doubles()
        self.handle_changes()

    def tabularasa_import(self, filename, source):
        """
        Do a tabula rasa import (clear the database first, and import straight to current) of a jsonlines file to a defined source

        :param filename:
        :param source:
        """
        global cache

        filePath = self.get_path('incoming', filename)

        self.clear_data(self.sourceConfig.get('table') + '_current')
        self.import_data(self.sourceConfig.get('table') + '_current', datafile=filePath)
        self.remove_doubles(suffix='current')
        self.set_indexes(self.sourceConfig.get('table') + '_current')

        # copy the data straight to the import
        outputPath = self.get_path('delta', filename)

        self.add_deltafile(outputPath)
        enrichSources = self.sourceConfig.get('src-enrich', None)
        if enrichSources:
            cache.clear()
            with open(file=outputPath, mode='w') as outputFile:
                self.export_records(fp=outputFile)
                logger.debug('Creating an enriched export file: "{file}"'.format(file=outputPath))
        else:
            shutil.copy(filePath, outputPath)
            logger.debug('Copy the import file: "{file}"'.format(file=outputPath))

        # move the import data
        processedPath = self.get_path('processed', filename)
        self.set_metainfo(key='out', value=processedPath, source=source.lower(), filename=filename)
        shutil.move(filePath, processedPath)

    def process_deletefiles(self, files):
        """
        Process the file with deleted records

        :param files:
        """
        for source, filenames in files.items():
            for filename in filenames:
                self.set_source(source.lower())

                filePath = self.get_path('incoming', filename)

                self.set_metainfo(key='in', value=filePath, source=source.lower(), filename=filename)
                self.import_deleted(filePath)

                processed_path = self.get_path('processed', filename)
                shutil.move(filePath, processed_path)

    def finish_job(self):
        """
        Finish the current job

        :return:
        """
        self.unlock()
        infuserJobFile = self.get_path('done', self.jobId + '.json')

        if len(self.deltafiles):
            self.percolatorMeta['outfiles'] = self.deltafiles
        self.job['percolator'] = self.percolatorMeta

        self.slack('*Percolator* finished `{job}` ```{json}```'.format(
            job=self.jobId,
            json=json.dumps(self.percolatorMeta, indent=3)
        )
        )

        try:
            jobFile = open(infuserJobFile, 'w')
        except Exception:
            msg = 'Unable to write to "{filepath}"'.format(filepath=infuserJobFile)
            logger.fatal(msg)
            self.slack('*Percolator* failed: {msg}'.format(msg=msg))
            return

        json.dump(self.job, jobFile)
        jobFile.close()

    def delta_writable_test(self):
        """
        Test if the directory exists where the deltafiles should go to

        :return:
        """
        deltaPath = self.paths.get('delta', '/tmp')
        if not os.path.isdir(deltaPath):
            msg = "Delta directory {deltapath} does not exist".format(deltapath=deltaPath)
            logger.fatal(msg)
            self.slack('*Percolator* failed: {msg}'.format(msg=msg))
            sys.exit(msg)
        # if not os.access(deltaPath,'w'):
        #    msg = "Delta directory {deltapath} is not writable".format(deltapath=deltaPath)
        #    logger.fatal(msg)
        #    sys.exit(msg)
        return True

    def open_deltafile(self, action='new', index='unknown'):
        """
        Open the delta file for updated, new or deleted records
        """
        if not self.jobId:
            filename = "{ts}-{index}-{action}.json".format(
                index=index,
                ts=time.strftime('%Y%m%d%H%M%S'),
                action=action
            )
        else:
            filename = "{jobid}-{index}-{action}.json".format(
                jobid=self.jobId,
                index=index,
                action=action
            )
        filePath = self.get_path('delta', filename)

        try:
            deltaFile = open(filePath, 'a')
        except Exception:
            msg = 'Unable to write to "{filepath}"'.format(filepath=filePath)
            logger.fatal(msg)
            self.slack('*Percolator* failed: {msg}'.format(msg=msg))
            sys.exit(msg)

        self.add_deltafile(filePath)

        logger.debug(filePath + ' opened')

        return deltaFile

    def lock_datafile(self, datafile=''):
        """
        Locking for single datafiles, this is different
        from the locking of jobs. Maybe it should be combined.

        :param datafile:
        :return:
        """
        lockfile = os.path.basename(datafile) + '.lock'
        filePath = self.get_path('delta', lockfile)

        if os.path.isfile(filePath):
            # Lock file already exists
            return False
        else:
            with open(file=filePath, mode='w'):
                os.utime(filePath, None)
            return True

    def unlock_datafile(self, datafile=''):
        """
        Unlocking for single datafiles, this is different from the
        locking of jobs. Maybe it should be combined.

        :param datafile:
        :return:
        """
        lockfile = os.path.basename(datafile) + '.lock'
        filePath = self.get_path('delta', lockfile)

        if os.path.isfile(filePath):
            # Lock file already exists
            os.remove(filePath)

        return True

    def log_change(self, state='unknown', recid='percolator', source='', type='', comment=''):
        """
        Logging of the state change of a record to the elastic logging
        database

        :param state:
        :param recid:
        :param comment:
        :param source:
        :param type:
        :return:
        """

        rec = {
            '@timestamp': datetime.now().isoformat(),
            'state': state,
            'ppd_timestamp': self.jobDate.isoformat(),
            'type': type,
            'source': source,
            'comment': comment
        }

        if self.elastic_logging:
            try:
                self.es.index(
                    index=self.jobId.lower(),
                    id=recid,
                    doc_type='logging',
                    body=json.dumps(rec)
                )
            except ConnectionError as err:
                logger.error('Timeout logging to elastic search: "{error}"'.format(error=err))
            except TransportError as err:
                logger.error('Failed to log to elastic search: "{error}"'.format(error=err))

    def slack(self, msg):
        """
        Send message to slack

        :param msg:
        """
        webhook_url = os.environ.get('SLACK_WEBHOOK', None)
        if webhook_url:
            slack_data = {'text': msg}

            response = requests.post(
                webhook_url, data=json.dumps(slack_data),
                headers={'Content-Type': 'application/json'}
            )
            if response.status_code != 200:
                raise ValueError(
                    'Request to slack returned an error %s, the response is:\n%s'
                    % (response.status_code, response.text)
                )

    @db_session
    def export_records(self, fp=None):
        """
        Exports all the records from a source table (enriched)

        :param fp:
        """
        srcEnrich = self.sourceConfig.get('src-enrich', None)

        base = self.sourceConfig.get('table')
        tableName = base.capitalize() + '_current'

        exportsql = 'SELECT rec ' \
                    'FROM {tablename}'.format(
            tablename=tableName
        )
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(exportsql)
                for r in cursor():
                    jsonRec = r[0]
                    if srcEnrich:
                        jsonRec = self.enrich_record(jsonRec, srcEnrich)
                    if fp:
                        json.dump(jsonRec, fp)
                        fp.write('\n')
                    else:
                        print(json.dumps(jsonRec))


    @db_session
    def clear_data(self, table=''):
        """
        Remove data from table
        """

        self.db.execute("TRUNCATE TABLE public.{table}".format(table=table))
        logger.debug('Truncated table "{table}"'.format(table=table))

    @db_session
    def import_deleted(self, filename=''):
        """
        Import deleted records list

        :param filename:
        """
        table = self.sourceConfig.get('table')
        index = self.sourceConfig.get('index', 'noindex')
        enriches = self.sourceConfig.get('dst-enrich', None)
        start = lap = timer()
        deleteIds = []

        try:
            with open(file=filename, mode='r') as f:
                deleteIds = f.read().splitlines()
        except Exception:
            msg = '"{filename}" cannot be read'.format(filename=filename)
            logger.fatal(msg)
            self.slack('*Percolator* failed: {msg}'.format(msg=msg))
            sys.exit(msg)

        deltaFile = self.open_deltafile('kill', index)
        for deleteId in deleteIds:
            if deltaFile:
                deleteRecord = self.create_delete_record(self.source, deleteId, 'REMOVED')
                json.dump(deleteRecord, deltaFile)
                deltaFile.write('\n')

            statusRecord = Deleted_records.get(recid=deleteId)
            if not statusRecord:
                statusRecord = Deleted_records(recid=deleteId, status='REMOVED', count=0)
            statusRecord.count += 1

            code = self.sourceConfig.get('code')

            self.log_change(
                state='kill',
                recid=deleteId,
                type=index,
                source=code
            )

            oldRecord = self.get_record(deleteId)
            if oldRecord:
                self.delete_record(oldrecord[0])

                if enriches:
                    for source in enriches:
                        logger.debug('Enrich source = {source}'.format(source=source))

                        self.handle_impacted(source, oldRecord)

                logger.debug(
                    '[{elapsed:.2f} seconds] Permanently deleted (kill) record "{recordid}" in "{source}"'.format(
                        source=table + '_current',
                        elapsed=(timer() - lap),
                        recordid=deleteId
                    )
                )
                lap = timer()

        if deltaFile:
            deltaFile.close()
            meta = {
                'count': len(deleteIds),
                'file': deltaFile.name,
                'elapsed': timer() - start
            }
            self.set_metainfo(key='delete', value=meta)

    @db_session
    def import_data(self, table='', datafile=''):
        """
        Imports data directly to the postgres database.
        """
        lap = timer()

        enrichmentSource = self.sourceConfig.get('src-enrich', False)
        enrichmentDestination = self.sourceConfig.get('dst-enrich', False)

        logger.debug(
            '[{elapsed:.2f} seconds] Start import data "{datafile}" into "{table}"'.format(
                datafile=datafile,
                table=table,
                elapsed=(timer() - lap)
            )
        )
        # Use the name of the filename as a job id
        if not self.jobId:
            filename = datafile.split('/')[-1]
            self.jobId = filename.replace('.json', '')

        self.db.execute("TRUNCATE public.{table}".format(table=table))

        # empties the table
        self.db.execute('ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS hindex'.format(table=table))

        # removes indexes
        self.db.execute('DROP INDEX IF EXISTS public.idx_{table}__jsonid'.format(table=table))
        self.db.execute('DROP INDEX IF EXISTS public.idx_{table}__hash'.format(table=table))
        self.db.execute('DROP INDEX IF EXISTS public.idx_{table}__gin'.format(table=table))

        # removes the hash column
        self.db.execute("ALTER TABLE public.{table} ALTER COLUMN hash DROP NOT NULL".format(table=table))
        logger.debug('[{elapsed:.2f} seconds] Reset "{table}" for import'.format(table=table, elapsed=(timer() - lap)))
        lap = timer()

        # imports all data by reading the jsonlines as a one column csv
        try:
            self.db.execute(
                "COPY public.{table} (rec) FROM '{datafile}' "
                "CSV QUOTE e'\x01' DELIMITER e'\x02'".format(
                    table=table,
                    datafile=datafile
                )
            )
        except Exception as err:
            msg = 'Import of "{datafile}" into "{table}" failed:\n\n{error}'.format(table=table,
                                                                                    datafile=datafile,
                                                                                    error=str(err))
            logger.fatal(msg)
            self.slack('*Percolator* failed: {msg}'.format(msg=msg))
            raise

        logger.debug(
            '[{elapsed:.2f} seconds] End import data "{datafile}" into "{table}"'.format(
                datafile=datafile,
                table=table,
                elapsed=(timer() - lap)
            )
        )
        lap = timer()

        # zet de hash
        logger.debug(
            '[{elapsed:.2f} seconds] Start set hashing on "{table}"'.format(table=table, elapsed=(timer() - lap)))
        self.db.execute("UPDATE {table} SET hash=md5(rec::text)".format(table=table))
        logger.debug(
            '[{elapsed:.2f} seconds] End set hashing on "{table}"'.format(table=table, elapsed=(timer() - lap)))
        lap = timer()

        self.set_indexes(table=table)

    @db_session
    def set_indexes(self, table=''):
        """
        Sets the indexes of a table

        :param table:
        """
        lap = timer()
        enrichmentSource = self.sourceConfig.get('src-enrich', False)
        enrichmentDestination = self.sourceConfig.get('dst-enrich', False)

        # zet de hashing index
        logger.debug(
            '[{elapsed:.2f} seconds] Start set hashing index on "{table}"'.format(
                table=table,
                elapsed=(timer() - lap)
            )
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_{table}__hash "
            "ON public.{table} USING BTREE(hash)".format(
                table=table)
        )
        logger.debug(
            '[{elapsed:.2f} seconds] End set hashing index on "{table}"'.format(
                table=table,
                elapsed=(timer() - lap)
            )
        )
        lap = timer()

        # zet de jsonid index
        logger.debug(
            '[{elapsed:.2f} seconds] Start set index on jsonid '.format(
                elapsed=(timer() - lap)
            )
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_{table}__jsonid "
            "ON public.{table} USING BTREE(({table}.rec->>'{idfield}'))".format(
                table=table,
                idfield=self.sourceConfig.get('id', 'id')
            )
        )
        logger.debug(
            '[{elapsed:.2f} seconds] End set index on jsonid '.format(
                elapsed=(timer() - lap)
            )
        )

        # set an index on identifications, which should be present in enriched data
        if enrichmentSource:
            logger.debug(
                '[{elapsed:.2f} seconds] Start set index on indentifications in "{table}"'.format(
                    table=table,
                    elapsed=(timer() - lap))
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS idx_{table}__gin "
                "ON public.{table} USING gin((rec->'identifications') jsonb_path_ops)".format(
                    table=table)
            )
            logger.debug(
                '[{elapsed:.2f} seconds] End set index on indentifications in "{table}"'.format(
                    table=table,
                    elapsed=(timer() - lap))
            )

        # set an index on the part containing scientificNameGroup,
        # which should be present in taxa sources
        if enrichmentDestination:
            logger.debug(
                '[{elapsed:.2f} seconds] Start set index on scientificNameGroup in "{table}"'.format(
                    table=table,
                    elapsed=(timer() - lap))
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS idx_{table}__sciname "
                "ON public.{table} USING gin((rec->'acceptedName') jsonb_path_ops)".format(
                    table=table
                )
            )
            logger.debug(
                '[{elapsed:.2f} seconds] End set index on scientificNameGroup in "{table}"'.format(
                    table=table,
                    elapsed=(timer() - lap))
            )

    @db_session
    def get_record(self, id, suffix="current"):
        """
        Gets records from the (current) table, suffix is optional and
        'current' by default. The other option is 'import'.

        :param id:
        :param suffix (optional, 'current' is default):
        :return query result:
        """
        base = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id', 'id')

        tableName = base.capitalize() + '_' + suffix

        logger.debug('Get record {id} from {table}'.format(
            table=tableName,
            id=id
        ))
        jsonsql = '(rec->>\'{idfield}\' = \'{idvalue}\')'.format(
            idfield=idField,
            idvalue=id
        )
        result = False
        query = "SELECT * " \
                "FROM {table} " \
                "WHERE {where}".format(
            table=tableName,
            where=jsonsql
        )
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()

        return result

    @db_session
    def delete_record(self, id, suffix="current"):
        """
        Gets records from the (current) table, suffix is optional and
        'current' by default. The other option is 'import'.

        :param id:
        :param suffix (optional, 'current' is default):
        :return query result:
        """
        base = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id', 'id')

        tableName = base.capitalize() + '_' + suffix

        query = "DELETE FROM {table} " \
                "WHERE id={id}".format(
            table=tableName,
            id=id
        )
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

        return

    @db_session
    def remove_doubles(self, suffix='import'):
        """
        Removes double records. Some sources can contain double
        records, these should be removed, before checking the hash.
        """
        start = lap = timer()

        logger.debug(
            '[{elapsed:.2f} seconds] Start filtere records with more than one entry in the source data'.format(
                elapsed=(timer() - lap))
        )
        doubleQuery = "SELECT array_agg(id) importids, rec->>'{idfield}' recid " \
                      "FROM {source}_{suffix} " \
                      "GROUP BY rec->>'{idfield}' HAVING COUNT(*) > 1".format(
            suffix=suffix,
            source=self.sourceConfig.get('table'),
            idfield=self.sourceConfig.get('id'))

        doubles = self.db.select(doubleQuery)
        logger.debug('[{elapsed:.2f} seconds] Find doubles'.format(elapsed=(timer() - lap)))

        lap = timer()

        count = 0
        for double in doubles:
            for importid in double.importids[:-1]:
                deletequery = "DELETE FROM {source}_{suffix} WHERE id = {importid}".format(
                    suffix=suffix,
                    source=self.sourceConfig.get('table'),
                    importid=importid)
                self.db.execute(deletequery)
            count += 1

        logger.debug(
            '[{elapsed:.2f} seconds] End filtered {doubles} records with more than one entry in the source data'.format(
                doubles=count,
                elapsed=(timer() - lap))
        )

        doubles = {
            'count': count,
            'elapsed': (timer() - start)
        }
        self.set_metainfo(key='doubles', value=doubles)

    @db_session
    def list_changes(self):
        """
        Identifies differences between the current database and the
        imported data. It does this by comparing hashes.

        If a hash is missing in the current database, but if it is
        present in the imported, than it could be a new record, or
        an update.

        A hash that is present in the current data, but is missing
        in the imported data can be deleted record. But this
        comparison can only be done with complete datasets. The
        changes dictionary looks something like this.

        The 'update' changes should be pairs of record id, which
        point to the id of records in the import and the current
        databases.

        ```
            changes = {
                'new': {
                    '3732672@BRAHMS' : [12345],
                    '1369617@BRAHMS' : [45678],
                    '2455323@BRAHMS' : [99999]
                },
                'update': {
                    '3732673@BRAHMS' : [12345,67676]
                },
                'delete': {
                    '3732674@BRAHMS' : [55555]
                }
            }
        ```

        """

        self.changes = {
            'new': {},
            'update': {},
            'delete': {}
        }
        source_base = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id')
        updateOrDeletes = None

        lap = timer()
        if len(source_base):
            logger.debug(
                '[{elapsed:.2f} seconds] Start left full outer join on "{source}"'.format(
                    source=source_base,
                    elapsed=(timer() - lap)
                )
            )
            leftdiffquery = 'SELECT {source}_import.id, {source}_import.hash ' \
                            'FROM {source}_import ' \
                            'FULL OUTER JOIN {source}_current ' \
                            'ON {source}_import.hash = {source}_current.hash ' \
                            'WHERE {source}_current.hash is null'.format(source=source_base)

            # @todo: maybe rewrite this to native postgres
            neworupdates = self.db.select(leftdiffquery)
            logger.debug(
                '[{elapsed:.2f} seconds] End left full outer join on "{source}": {count}'.format(
                    source=source_base,
                    elapsed=(timer() - lap),
                    count=len(neworupdates)
                )
            )
            lap = timer()

            if not self.is_incremental():
                # this part is only done when a source is non incremental
                logger.debug(
                    '[{elapsed:.2f} seconds] Start right full outer join on "{source}"'.format(
                        source=source_base,
                        elapsed=(timer() - lap)
                    )
                )
                rightdiffquery = 'SELECT {source}_current.id, {source}_current.hash ' \
                                 'FROM {source}_import ' \
                                 'FULL OUTER JOIN {source}_current ' \
                                 'ON {source}_import.hash = {source}_current.hash ' \
                                 'WHERE {source}_import.hash is null'.format(source=source_base)

                # @todo: maybe rewrite this to native postgres
                updateOrDeletes = self.db.select(rightdiffquery)
                logger.debug(
                    '[{elapsed:.2f} seconds] End right full outer join on "{source}": {count}'.format(
                        source=source_base,
                        elapsed=(timer() - lap),
                        count=len(updateOrDeletes)
                    )
                )
            lap = timer()

            #importtable = globals()[source_base.capitalize() + '_import']
            #currenttable = globals()[source_base.capitalize() + '_current']

            # new or update
            count = 0
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    for result in neworupdates:
                        if result[1]:
                            count += 1
                            if count % 1000 == 0:
                                logger.debug('{count} neworupdates: '
                                             '{new} new, {update} updates, {delete} deletes'.format(
                                    count=count,
                                    update=len(self.changes['update']),
                                    delete=len(self.changes['delete']),
                                    new=len(self.changes['new'])
                                ))
                            importsql = 'SELECT id, rec ' \
                                        'FROM {source}_import ' \
                                        'WHERE {source}_import.hash=%s'.format(
                                source=source_base
                            )
                            cursor.execute(importsql, (result[1],))
                            r = cursor.fetchone()
                            if r:
                                rec = json.loads(r[1])
                                uuid = rec[idField]
                                oldrec = self.get_record(uuid)
                                if self.is_incremental() and oldrec:
                                    self.changes['update'][uuid] = [r[0]]
                                    self.changes['update'][uuid].append(oldrec[0])
                                    logger.debug('Update {oldid} to {newid}'.format(
                                        oldid=oldrec[0],
                                        newid=r[0]
                                    ))
                                else:
                                    self.changes['new'][uuid] = [r[0]]
                        else:
                            logger.error('Empty hash in neworupdates')

            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not self.is_incremental():
                        # incremental sources only have explicit deletes
                        count = 0
                        for result in updateOrDeletes:
                            count += 1
                            if count % 1000 == 0:
                                logger.debug('{count} updatedordeletes: '
                                             '{new} new, {update} updates, {delete} deletes'.format(
                                    count=count,
                                    update=len(self.changes['update']),
                                    delete=len(self.changes['delete']),
                                    new=len(self.changes['new'])
                                ))
                            if result[1]:
                                currentsql = 'SELECT id, rec ' \
                                             'FROM {source}_current ' \
                                             'WHERE {source}_current.hash=%s'.format(source=source_base)
                                cursor.execute(currentsql, (result[1],))
                                r = cursor.fetchone()
                                if r:
                                    rec = json.loads(r[1])
                                    uuid = rec[idField]
                                    if self.changes['new'].get(uuid, False):
                                        self.changes['update'][uuid] = self.changes['new'].get(uuid)
                                        self.changes['update'][uuid].append(r[0])
                                        del self.changes['new'][uuid]
                                    else:
                                        self.changes['delete'][uuid] = [r[0]]
                            else:
                                logger.error('Empty hash in updateordeletes')

            if len(self.changes['new']) or len(self.changes['update']) or len(self.changes['delete']):
                if len(self.changes['new']):
                    logger.info(
                        '[{elapsed:.2f} seconds] {new} inserted'.format(
                            new=len(self.changes['new']),
                            elapsed=(timer() - lap)
                        )
                    )
                if len(self.changes['update']):
                    logger.info(
                        '[{elapsed:.2f} seconds] {update} updated'.format(
                            update=len(self.changes['update']),
                            elapsed=(timer() - lap)
                        )
                    )
                if len(self.changes['delete']):
                    logger.info(
                        '[{elapsed:.2f} seconds] {delete} removed'.format(
                            delete=len(self.changes['delete']),
                            elapsed=(timer() - lap)
                        )
                    )
            else:
                logger.info(
                    '[{elapsed:.2f} seconds] No changes'.format(
                        elapsed=(timer() - lap)
                    )
                )

        return self.changes

    @db_session
    def handle_new(self):
        """
        Handles new records
        """

        table = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id')
        index = self.sourceConfig.get('index', 'noindex')
        srcEnrich = self.sourceConfig.get('src-enrich', False)
        dstEnrich = self.sourceConfig.get('dst-enrich', None)

        deltaFile = self.open_deltafile('new', index)

        start = lap = timer()

        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                for jsonId, databaseIds in self.changes['new'].items():
                    importId = databaseIds[0]

                    importsql = 'SELECT rec ' \
                                'FROM {source}_import ' \
                                'WHERE {source}_import.id=%s'.format(
                        source=table.capitalize()
                    )
                    cursor.execute(importsql, (importId,))
                    r = cursor.fetchone()
                    jsonRec = json.loads(r[0])
                    if srcEnrich:
                        jsonRec = self.enrich_record(jsonRec, srcEnrich)

                    insertQuery = "INSERT INTO {table}_current (rec, hash, datum) " \
                                  "SELECT rec, hash, datum FROM {table}_import where id={id}".format(
                        table=self.sourceConfig.get('table'),
                        id=importId
                    )

                    self.db.execute(insertQuery)
                    if deltaFile:
                        json.dump(jsonRec, deltaFile)
                        deltaFile.write('\n')

                    code = self.sourceConfig.get('code')
                    if dstEnrich:
                        self.cache_taxon_record(jsonRec, code)

                    self.log_change(
                        state='new',
                        recid=jsonRec.get(idField, 'no id'),
                        source=code,
                        type=index
                    )
                    logger.debug(
                        '[{elapsed:.2f} seconds] New record "{recordid}" inserted in "{source}"'.format(
                            elapsed=(timer() - lap),
                            source=table + '_current',
                            recordid=jsonRec.get(idField, 'no id')
                        )
                    )
                    lap = timer()

        self.set_indexes(table + '_current')

        if deltaFile:
            deltaFile.close()
            meta = {
                'count': len(self.changes['new']),
                'file': deltaFile.name,
                'elapsed': timer() - start
            }
            self.set_metainfo(key='new', value=meta)

    @db_session
    def handle_updates(self):
        """
        Handles updates by storing the import records into the
        current table.

        When a source record needs to get enriched it retrieves the
        enrichment part from a taxon record and adds it to the record.

        If the source record enriches another source, the impacted
        records get enriched again.
        """
        tableBase = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id')
        enrichDestinations = self.sourceConfig.get('dst-enrich', None)
        enrichSources = self.sourceConfig.get('src-enrich', None)
        index = self.sourceConfig.get('index', 'noindex')
        code = self.sourceConfig.get('code', '')

        deltaFile = self.open_deltafile('update', index)
        # Write updated records to the deltafile

        start = lap = timer()
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                for change, recordIds in self.changes['update'].items():
                    # first id points to the new rec
                    importsql = 'SELECT {source}_import.rec ' \
                                'FROM {source}_import ' \
                                'WHERE {source}_import.id=%s'.format(
                        source=tableBase.capitalize()
                    )
                    cursor.execute(importsql, (recordIds[0],))
                    importRec = cursor.fetchone()

                    currentsql = 'SELECT {source}_current.rec ' \
                                 'FROM {source}_current ' \
                                 'WHERE {source}_current.id=%s'.format(
                        source=tableBase.capitalize()
                    )
                    cursor.execute(currentsql, (recordIds[1],))
                    oldRec = cursor.fetchone()
                    if (oldRec):
                        jsonRec = json.loads(importRec[0])

                        # If this record should be enriched by specified sources
                        if enrichSources:
                            jsonRec = self.enrich_record(jsonRec, enrichSources)

                        # @todo: when it is an update, the record should be checked in the deleted list
                        updateQuery = "UPDATE {table}_current SET (rec, hash, datum) = " \
                                      "(SELECT rec, hash, datum FROM {table}_import " \
                                      "WHERE {table}_import.id={importid}) " \
                                      "WHERE {table}_current.id={currentid}".format(
                            table=tableBase,
                            currentid=recordIds[1],
                            importid=recordIds[0])

                        if deltaFile:
                            json.dump(jsonRec, deltaFile)
                            deltaFile.write('\n')

                        self.db.execute(updateQuery)

                        # If this record has impact on records that should
                        # be enriched again
                        if enrichDestinations:
                            code = self.sourceConfig.get('code')
                            self.cache_taxon_record(jsonRec, code)

                            for source in enrichDestinations:
                                logger.debug(
                                    'Enrich source = {source}'.format(source=source)
                                )
                                self.handle_impacted(source, jsonRec)

                        logger.debug(
                            '[{elapsed:.2f} seconds] Updated record "{recordid}" in "{source}"'.format(
                                source=tableBase + '_current',
                                elapsed=(timer() - lap),
                                recordid=jsonRec.get(idField,'')
                            )
                        )
                        self.log_change(
                            state='update',
                            recid=jsonRec.get(idField,''),
                            source=code,
                            type=index
                        )
                        lap = timer()

        if deltaFile:
            deltaFile.close()
            meta = {
                'count': len(self.changes['update']),
                'file': deltaFile.name,
                'elapsed': timer() - start
            }
            self.set_metainfo(key='update', value=meta)

    @db_session
    def handle_deletes(self):
        """
        Handles temporary deleted records
        """
        table = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id')
        enriches = self.sourceConfig.get('enriches', None)
        index = self.sourceConfig.get('index', 'noindex')
        code = self.sourceConfig.get('code', '')

        # Write data to deltafile file
        deltaFile = self.open_deltafile('delete', index)

        start = lap = timer()
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                for change, recordIds in self.changes['delete'].items():
                    currentsql = 'SELECT {source}_current.rec ' \
                                 'FROM {source}_current ' \
                                 'WHERE {source}_current.id=%s'.format(
                        source=table.capitalize()
                    )
                    cursor.execute(currentsql, (recordIds[0],))
                    oldRecord = cursor.fetchone()
                    if oldRecord:
                        jsonRec = json.loads(oldRecord[0])
                        deleteId = jsonRec.get(idField)
                        if deltaFile and deleteId:
                            deleteRecord = self.create_delete_record(self.source, deleteId, 'REJECTED')
                            json.dump(deleteRecord, deltaFile)
                            deltaFile.write('\n')

                        statusRecord = Deleted_records.get(recid=deleteId)
                        if not statusRecord:
                            statusRecord = Deleted_records(recid=deleteId, status='REJECTED', count=0)
                        statusRecord.count += 1

                        # @todo: only when a certain threshold is reached, the old record should be removed
                        deleteqry = 'DELETE FROM {source}_current ' \
                                    'WHERE {source}_current.id=%s'.format(
                            source=table.capitalize()
                        )
                        cursor.execute(deleteqry, (recordIds[0],))

                        self.log_change(
                            state='delete',
                            recid=deleteId,
                            type=index,
                            source=code
                        )

                        if enriches:
                            code = self.sourceConfig.get('code')
                            self.cache_taxon_record(jsonRec, code)

                            for source in enriches:
                                logger.debug('Enrich source = {source}'.format(source=source))
                                self.handle_impacted(source, jsonRec)

                        logger.debug(
                            '[{elapsed:.2f} seconds] Temporarily deleted record "{deleteid}" in "{source}"'.format(
                                source=table + '_current',
                                elapsed=(timer() - lap),
                                deleteid=deleteId
                            )
                        )
                        lap = timer()

                        logger.info("Record [{deleteid}] deleted".format(deleteid=deleteId))

        if deltaFile:
            deltaFile.close()
            meta = {
                'count': len(self.changes['delete']),
                'file': deltaFile.name,
                'elapsed': timer() - start
            }
            self.set_metainfo(key='delete', value=meta)

    def list_impacted(self, sourceConfig, scientificNameGroup):
        """
        Looks for impacted records based on scientificnamegroup

        :param scientificNameGroup:
        :return bool or list of items:
        """
        table = sourceConfig.get('table')
        currenttable = globals()[table.capitalize() + '_current']

        jsonsql = 'rec->\'identifications\' @> \'[{"scientificName":{"scientificNameGroup":"%s"}}]\'' % (
            scientificNameGroup
        )
        items = []
        query = "SELECT * " \
                "FROM {table} " \
                "WHERE {where}".format(
            table=table.capitalize() + '_current',
            where=jsonsql
        )
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                items = cursor.fetchall()

        if len(items):
            logger.info(
                "Found {number} records in {source} with scientificNameGroup={namegroup}".format(
                    number=len(items),
                    source=table.capitalize(),
                    namegroup=scientificNameGroup)
            )
            return items
        else:
            logger.error(
                "Found no records in {source} with scientificNameGroup={namegroup}".format(
                    number=len(items),
                    source=table.capitalize(),
                    namegroup=scientificNameGroup)
            )
            return items

    def get_taxon(self, scientificNameGroup, source):
        """
        Retrieves a taxon from the database on the field
        'acceptedName.scientificNameGroup'

        :param scientificNameGroup:
        :param source:
        :return:
        """
        sourceConfig = self.config.get('sources').get(source, False)
        if not sourceConfig:
            return False

        table = sourceConfig.get('table')
        if not table:
            return False

        code = sourceConfig.get('code')

        # Retrieve the taxon from cache
        taxonKey = '_'.join([code, scientificNameGroup])
        taxons = cache.get(taxonKey)

        if taxons is not None:
            logger.debug('get_taxon: {taxonkey} got json from cache'.format(
                taxonkey=taxonKey
            ))
            return taxons

        currentTable = globals().get(table.capitalize() + '_current')
        if not currentTable:
            return False

        # Retrieve the taxon from the database
        sciSql = 'rec->\'acceptedName\' @> \'{"scientificNameGroup":"%s"}\'' % (
            scientificNameGroup
        )
        #taxonQuery = currentTable.select(lambda p: raw_sql(sciSql))
        query = "SELECT rec " \
                "FROM {table} " \
                "WHERE {where}".format(
            table=table.capitalize() + '_current',
            where=sciSql
        )

        taxons = []
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                logger.debug('get_taxon: {taxonkey} store json in cache'.format(
                    taxonkey=taxonKey
                ))
                for taxon in cursor:
                    taxons.append(taxon[0])

        cache.set(taxonKey, taxons)
        logger.debug('get_taxon: {taxonkey} store {records} records in cache'.format(
            taxonkey=taxonKey,
            records=len(taxons)
        ))

        return taxons

    def cache_taxon_record(self, jsonRec, systemCode):
        """
        Caches the taxon record

        :param jsonRec:
        :param systemCode:
        """
        taxons = []
        if jsonRec.get('acceptedName') and jsonRec.get('acceptedName').get('scientificNameGroup'):
            scientificNameGroup = jsonRec.get('acceptedName').get('scientificNameGroup')
            taxonKey = '_'.join([systemCode, scientificNameGroup])

            cachedTaxons = cache.get(taxonKey)
            if cachedTaxons:
                for jsonTaxon in cachedTaxons:
                    taxon = json.loads(jsonTaxon)
                    if taxon.get('id') == jsonRec.get('id'):
                        taxons.append(json.dumps(jsonRec))
                    else:
                        taxons.append(jsonTaxon)
            else:
                taxons.append(json.dumps(jsonRec))

            logger.debug('cache_taxon_records: {taxonkey} store json in cache'.format(
                    taxonkey=taxonKey
                )
            )
            cache.set(taxonKey, taxons)

    def create_name_summary(self, vernacularName):
        """
        Creates a scientific name summary, only use the fields specified

        :param vernacularName:
        :return dict:
        """
        fields = [
            'name',
            'language'
        ]
        summary = {}
        for field in fields:
            if vernacularName.get(field):
                summary[field] = vernacularName.get(field)

        return summary

    def create_scientific_summary(self, scientificName):
        """
        Creates a scientific summary, only use certain fields

        :param scientificName:
        :return dict:
        """
        fields = [
            'fullScientificName',
            'taxonomicStatus',
            'genusOrMonomial',
            'subgenus',
            'specificEpithet',
            'infraspecificEpithet',
            'authorshipVerbatim'
        ]
        summary = {}
        for field in fields:
            if scientificName.get(field):
                summary[field] = scientificName.get(field)

        return summary

    def create_enrichments(self, taxonRecs, source):
        """
        Creates the enrichment

        :param rec:
        :param source:
        :return:
        """
        enrichments = []
        for jsonRec in taxonRecs:
            lap = timer()
            rec = json.loads(jsonRec)
            vernacularNames = rec.get('vernacularNames')
            scientificNameGroup = rec.get('acceptedName').get('scientificNameGroup')
            enrichment = {}

            if vernacularNames:
                enrichment['vernacularNames'] = []
                for name in vernacularNames:
                    enrichment['vernacularNames'].append(self.create_name_summary(name))

            enrichment['taxonId'] = rec.get('id')

            synonyms = rec.get('synonyms', False)
            if synonyms:
                enrichment['synonyms'] = []
                for scientificName in synonyms:
                    enrichment['synonyms'].append(self.create_scientific_summary(scientificName))

            if (rec.get('sourceSystem') and rec.get('sourceSystem').get('code')):
                enrichment['sourceSystem'] = {}
                enrichment['sourceSystem']['code'] = rec.get('sourceSystem').get('code')

                if (rec.get('sourceSystem').get('code') == 'COL'):
                    if rec.get('defaultClassification'):
                        enrichment['defaultClassification'] = rec.get('defaultClassification')

            enrichments.append(enrichment)

            logger.debug(
                '[{elapsed:.2f} seconds] Created enrichment for "{scinamegroup}" in "{source}"'.format(
                    source=source,
                    elapsed=(timer() - lap),
                    scinamegroup=scientificNameGroup
                )
            )

        return enrichments

    def create_delete_record(self, source, recordId, status='REJECTED'):
        """
        Creates a delete record

        :param source:
        :param recordId:
        :param status:
        :return dict:
        """
        sourceConfig = None
        if self.config.get('sources').get(source):
            sourceConfig = self.config.get('sources').get(source)

        deleteRecord = dict()

        deleteRecord['unitID'] = recordId
        deleteRecord['sourceSystemCode'] = sourceConfig.get('code', '')
        deleteRecord['status'] = status

        return deleteRecord

    def get_enrichments(self, sciNameGroup, source):
        """
        First tries to retrieve the enrichments from cache. When it
        is not generated, new enrichments are created from a taxon
        json record and stored in cache

        :param sciNameGroup:
        :param source:
        :return enrichment(dictionary) or False:
        """
        lap = timer()
        taxons = self.get_taxon(sciNameGroup, source)

        if taxons:
            return self.create_enrichments(taxons, source)
        else:
            logger.debug(
                '[{elapsed:.2f} seconds] No enrichment for "{scinamegroup}" in "{source}"'.format(
                    source=source,
                    elapsed=(timer() - lap),
                    scinamegroup=sciNameGroup
                )
            )
            return False

    def enrich_record(self, rec, sources):
        """
        Enriches a json record with taxon information from the
        sources it does this by checking each element in
        identifications[] and if it contains a
        'scientificName.scientificNameGroup' it tries to generate
        an enrichment from each source

        :param rec:
        :param sources:
        :return:
        """
        sciNameGroup = False
        if not rec.get('identifications', False):
            return rec

        identifications = rec.get('identifications')
        for index, identification in enumerate(identifications):
            if identification.get('scientificName') and \
                    identification.get('scientificName').get('scientificNameGroup'):
                sciNameGroup = identification.get('scientificName').get('scientificNameGroup')

                enrichments = []
                for source in sources:
                    enrichment = self.get_enrichments(sciNameGroup, source)
                    if enrichment:
                        enrichments = enrichments + enrichment

                if len(enrichments) > 0:
                    rec.get('identifications')[index]['taxonomicEnrichments'] = enrichments

        return rec

    @db_session
    def handle_impacted(self, source, record):
        """
        Handles the record that are impacted by a taxon record change

        :param source:
        :param record:
        """
        scientificNameGroup = None
        sourceConfig = self.config.get('sources').get(source)
        enrichmentSources = sourceConfig.get('src-enrich', False)
        idField = sourceConfig.get('id')
        index = sourceConfig.get('index', 'noindex')

        lap = start = timer()

        # Retrieve scientificNameGroup from the acceptedName part
        if record.get('acceptedName'):
            scientificNameGroup = record.get('acceptedName').get('scientificNameGroup')

        if scientificNameGroup:
            impactedRecords = self.list_impacted(sourceConfig, scientificNameGroup)
            if impactedRecords:
                deltaFile = self.open_deltafile('enrich', index)
                if deltaFile:
                    for impacted in impactedRecords:
                        jsonRecord = json.loads(impacted[1])
                        if enrichmentSources:
                            jsonRecord = self.enrich_record(jsonRecord, enrichmentSources)

                        json.dump(jsonRecord, deltaFile)
                        deltaFile.write('\n')

                        impactId = jsonRecord.get(idField)
                        logger.debug(
                            '[{elapsed:.2f} seconds] Record "{recordid}" of "{source}" needs to be enriched'.format(
                                source=source,
                                elapsed=(timer() - lap),
                                recordid=impactId
                            )
                        )
                        self.log_change(
                            state='enrich',
                            recid=impactId,
                            source=sourceConfig.get('code'),
                            type=index
                        )
                        lap = timer()

                    meta = self.get_metainfo(key='enrich:' + index)
                    if isinstance(meta, dict):
                        meta['count'] += len(impactedRecords)
                        meta['elapsed'] += timer() - start
                    else:
                        meta = {
                            'count': len(impactedRecords),
                            'file': deltaFile.name,
                            'elapsed': timer() - start
                        }

                    self.set_metainfo(key='enrich:' + index, value=meta)

                    deltaFile.close()

    @db_session
    def handle_changes(self):
        """
        Handles all the changes
        """

        self.list_changes()

        if len(self.changes['new']):
            self.handle_new()
        if len(self.changes['update']):
            self.handle_updates()
        if not self.is_incremental():
            # Only deletes in case a source supplies complete sets
            if (len(self.changes['delete'])):
                self.handle_deletes()

        return
