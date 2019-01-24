"""NBA preprocessing database module - the percolator

This module contains all the database dependencies and functions used
for importing new and updated data into the NBA documentstore.
"""
import json
import logging
import os
import glob
import shutil
import sys
import time
import yaml
from timeit import default_timer as timer
from elasticsearch import Elasticsearch
from pony.orm import db_session
from dateutil import parser
from diskcache import Cache
from .schema import *

# Setup logging
logging.basicConfig(format=u'%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ppdb_nba')
logger.setLevel(logging.INFO)

# Caching on disk (diskcache) using sqlite, it should be fast
cache = Cache('/tmp/import_cache')
cache.clear()


# noinspection SqlNoDataSourceInspection,SqlResolve,PyTypeChecker,PyUnresolvedReferences,SpellCheckingInspection
class ppdb_NBA():
    """
    Preprocessor class containing all functions needed for importing the data and
    create incremental insert, update or delete files.
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
                    self.config = yaml.load(ymlFile)
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
            sys.exit(msg)

        self.es = self.connect_to_elastic()
        self.connect_to_database()

        self.jobDate = datetime.now()

        self.jobId = ''
        self.source = ''
        self.sourceConfig = {}

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

    def connect_to_elastic(self):
        """
        Connect to elastic search for logging

        :return:
        """
        try:
            es = Elasticsearch(hosts=self.config['elastic']['host'])
            return es
        except Exception:
            msg = 'Cannot connect to elastic search server (needed for logging)'
            logger.fatal(msg)
            sys.exit(msg)

    def connect_to_database(self):
        """
        Connects to postgres database
        """
        global ppdb

        self.db = ppdb

        logger.debug('Connecting to database')
        try:
            self.db.bind(
                provider='postgres',
                user=self.config['postgres']['user'],
                password=self.config['postgres']['pass'],
                host=self.config['postgres']['host'],
                database=self.config['postgres']['db']
            )
        except Exception:
            msg = 'Cannot connect to postgres database'
            logger.fatal(msg)
            sys.exit(msg)

    def generate_mapping(self, create_tables=False):
        """
        Generates mapping of the database
        """
        try:
            self.db.generate_mapping(create_tables=create_tables)
        except Exception:
            msg = 'Creating tables needed for preprocessing failed'
            logger.fatal(msg)
            sys.exit(msg)

    def is_incremental(self):
        return self.sourceConfig.get('incremental', True)

    def lock(self, jobFile):
        """
        Generates a locking file
        """
        jobsPath = self.config.get('paths').get('jobs', os.getcwd() + "/jobs")
        with open(os.path.join(jobsPath, '.lock'), 'w') as lockFile:
            lockRecord = {
                'job': jobFile,
                'pid': os.getpid()
            }
            json.dump(lockRecord, lockFile)

    def unlock(self):
        """
        Removes the locking file
        """
        jobsPath = self.config.get('paths').get('jobs', os.getcwd() + "/jobs")
        locks = glob.glob(os.path.join(jobsPath, '.lock'))
        lock = locks.pop()

        os.remove(lock)

    def is_locked(self):
        """
        Checks if there's a lockfile and if so, parse it. A lockfile is a
        json record with PID as well as job filepath. If the process is
        still running then the process is still locked. If not it has
        failed.

        If no lock file exists it is no longer locked.

        :return:  True = still locked / False = no longer locked
        """
        jobsPath = self.config.get('paths').get('jobs', os.getcwd() + "/jobs")

        locks = glob.glob(jobsPath + '/.lock')
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
                failedpath = self.config.get('paths').get('failed', os.path.join(os.getcwd(), "failed"))
                shutil.move(lockinfo['job'], os.path.join(failedpath, jobFile))

                self.log_change(
                    state='fail'
                )
                logger.error(
                    'Preprocessor failed in the last run? '
                    'Job file "{job}" moved to failed'.format(job=lockinfo['job'])
                )
                os.remove(lockFile)

        return False

    def parse_job(self, jobFile=''):
        """
        Parse a json job file, and tries to retrieve the validated filenames
        then returns a dictionary of sources with a list of files.

        :rtype: object
        """
        files = {}
        with open(jobFile) as jsonData:
            jobRecord = json.load(jsonData)

            # Get the id of the job
            self.jobId = jobRecord.get('id')

            # Get the name of the supplier
            self.supplier = jobRecord.get('data_supplier')

            # Get the date of the job
            rawdate = jobRecord.get('date', False)
            if rawdate:
                self.jobDate = parser.parse(rawdate)

            # Parse the validator part, get the outfiles
            if jobRecord.get('validator'):
                for key in jobRecord.get('validator').keys():
                    export = jobRecord.get('validator').get(key)
                    for validfile in export.get('results').get('outfiles').get('valid'):
                        source = self.supplier + '-' + key
                        if source not in files:
                            files[source] = []
                        files[source].append(validfile.split('/')[-1])

        return files

    def handle_job(self, jobFile=''):
        """
        Handle the job

        :param jobFile:
        :return:
        """
        self.lock(jobFile)

        files = self.parse_job(jobFile)
        incoming_path = self.config.get('paths').get('incoming', '/tmp')

        self.log_change(
            state='start'
        )

        # import each file
        for source, filenames in files.items():
            for filename in filenames:
                self.set_source(source.lower())
                filePath = os.path.join(incoming_path, filename)

                self.log_change(
                    state='import',
                    comment='{filepath}'.format(filepath=filePath)
                )
                try:
                    self.import_data(table=self.sourceConfig.get('table') + '_import', datafile=filePath)
                except Exception:
                    # import fails? remove the lock, return false
                    logger.error(
                        "Import of '{file}' into '{source}' failed".format(file=filePath, source=source.lower()))
                    return False

                # import successful, move the data file
                processed_path = os.path.join(self.config.get('paths').get('processed', '/tmp'), filename)
                shutil.move(filePath, processed_path)

                self.remove_doubles()
                self.handle_changes()
        self.log_change(
            state='finish'
        )

        # everything is finished and okay, remove the lock
        self.unlock()

        return True

    def open_deltafile(self, action='new', index='unknown'):
        """
        Open the delta file for the updated, new or deleted records
        """
        deltaPath = self.config.get('paths').get('delta', '/tmp')
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
        filePath = os.path.join(deltaPath, filename)

        try:
            deltaFile = open(filePath, 'w')
        except Exception:
            msg = 'Unable to write to "{filepath}"'.format(filepath=filePath)
            logger.fatal(msg)
            sys.exit(msg)

        logger.debug(filePath + ' opened')

        return deltaFile

    def lock_datafile(self, datafile=''):
        """
        Locking for single datafiles, this is different from the locking of jobs.
        Maybe it should be combined.

        :param datafile:
        :return:
        """
        destinationPath = self.config.get('paths').get('delta', '/tmp')
        lockfile = os.path.basename(datafile) + '.lock'
        filePath = os.path.join(destinationPath, lockfile)

        if os.path.isfile(filePath):
            # Lock file already exists
            return False
        else:
            with open(file=filePath, mode='a'):
                os.utime(filePath, None)
            return True

    def unlock_datafile(self, datafile=''):
        """
        Unlocking for single datafiles, this is different from the locking of jobs.
        Maybe it should be combined.

        :param datafile:
        :return:
        """
        destinationPath = self.config.get('paths').get('delta', '/tmp')
        lockfile = os.path.basename(datafile) + '.lock'
        filePath = os.path.join(destinationPath, lockfile)

        if os.path.isfile(filePath):
            # Lock file already exists
            os.remove(filePath)

        return True

    def log_change(self, state='unknown', recid='ppdb_nba', comment=''):
        """
        Logging of the state change of a record to the elastic logging database

        :param state:
        :param recid:
        :param comment:
        :return:
        """

        rec = {
            '@timestamp': datetime.now().isoformat(),
            'state': state,
            'ppd_timestamp': self.jobDate.isoformat(),
            'comment': comment
        }

        try:
            self.es.index(
                index=self.jobId.lower(),
                id=recid,
                doc_type='logging',
                body=json.dumps(rec)
            )
        except Exception:
            logger.error(self, 'Failed to log to elastic search')

    @db_session
    def clear_data(self, table=''):
        """
        Remove data from table
        """

        self.db.execute("TRUNCATE public.{table}".format(table=table))
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
        lap = timer()
        deleteIds = []

        try:
            with open(file=filename, mode='r') as f:
                deleteIds = f.read().splitlines()
        except Exception:
            msg = '"{filename}" cannot be read'.format(filename=filename)
            logger.fatal(msg)
            sys.exit(msg)

        deltaFile = None
        deltaFile = self.open_deltafile('kill', index)
        for deleteId in deleteIds:
            if deltaFile:
                deleteRecord = self.create_delete_record(self.source, deleteId, 'REMOVED')
                json.dump(deleteRecord, deltaFile)
                deltaFile.write('\n')

            # @todo: register the delete in Deleted_records table
            # if it already exists, change the status
            oldRecord = self.get_record(deleteId)
            if oldRecord:
                oldRecord.delete()

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

    @db_session
    def import_data(self, table='', datafile=''):
        """
        Imports data directly to the postgres database.
        """
        lap = timer()

        enrichmentSource = self.sourceConfig.get('src-enrich', False)
        enrichmentDestination = self.sourceConfig.get('dst-enrich', False)

        # Use the name of the filename as a job id
        if not self.jobId:
            filename = datafile.split('/')[-1]
            self.jobId = filename.replace('.json', '')

        self.db.execute("TRUNCATE public.{table}".format(table=table))

        # empties the table, removes indexes
        self.db.execute('ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS hindex'.format(table=table))
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
            logger.fatal(
                'Import of "{datafile}" into "{table}" failed:\n\n{error}'.format(table=table,
                                                                                  datafile=datafile,
                                                                                  error=str(err)))
            raise

        logger.debug(
            '[{elapsed:.2f} seconds] Import data "{datafile}" into "{table}"'.format(
                datafile=datafile,
                table=table,
                elapsed=(timer() - lap)
            )
        )
        lap = timer()

        # zet de hash
        self.db.execute("UPDATE {table} SET hash=md5(rec::text)".format(table=table))
        logger.debug(
            '[{elapsed:.2f} seconds] Set hashing on "{table}"'.format(table=table, elapsed=(timer() - lap)))
        lap = timer()

        # zet de hashing index
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_{table}__hash "
            "ON public.{table} USING BTREE(hash)".format(
                table=table)
        )
        logger.debug(
            '[{elapsed:.2f} seconds] Set hashing index on "{table}"'.format(
                table=table,
                elapsed=(timer() - lap)
            )
        )
        lap = timer()

        # zet de jsonid index
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_{table}__jsonid "
            "ON public.{table} USING BTREE(({table}.rec->>'{idfield}'))".format(
                table=table,
                idfield=self.sourceConfig.get('id', 'id')
            )
        )
        logger.debug(
            '[{elapsed:.2f} seconds] Set index on jsonid '.format(
                elapsed=(timer() - lap)
            )
        )

        # set an index on identifications, which should be present in enriched data
        if enrichmentSource:
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS idx_{table}__gin "
                "ON public.{table} USING gin((rec->'identifications') jsonb_path_ops)".format(
                    table=table)
            )
            logger.debug(
                '[{elapsed:.2f} seconds] Set index on indentifications in "{table}"'.format(
                    table=table,
                    elapsed=(timer() - lap))
            )

        # set an index on the part containing scientificNameGroup,
        # which should be present in taxa sources
        if enrichmentDestination:
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS idx_{table}__sciname "
                "ON public.{table} USING gin((rec->'acceptedName') jsonb_path_ops)".format(
                    table=table
                )
            )
            logger.debug(
                '[{elapsed:.2f} seconds] Set index on scientificNameGroup in "{table}"'.format(
                    table=table,
                    elapsed=(timer() - lap))
            )

    @db_session
    def get_record(self, id, suffix="current"):
        """
        Get record from the (current) table, suffix is optional and 'current' by default.
        The other option is 'import'.

        :param id:
        :param suffix:
        :return:
        """
        base = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id', 'id')

        tableName = base.capitalize() + '_' + suffix

        logger.debug('Get record {id} from {table}'.format(
            table=tableName,
            id=id
        ))
        dataTable = globals()[tableName]
        query = dataTable.select(lambda p: p.rec[idField] == id)

        return query.get()

    @db_session
    def remove_doubles(self):
        """
        Some sources can contain double records, these should be removed,
        before checking the hash.
        """
        lap = timer()

        doubleQuery = "SELECT array_agg(id) importids, rec->>'{idfield}' recid " \
                      "FROM {source}_import " \
                      "GROUP BY rec->>'{idfield}' HAVING COUNT(*) > 1".format(
                        source=self.sourceConfig.get('table'),
                        idfield=self.sourceConfig.get('id'))
        doubles = self.db.select(doubleQuery)
        logger.debug('[{elapsed:.2f} seconds] Find doubles'.format(elapsed=(timer() - lap)))
        lap = timer()

        count = 0
        for double in doubles:
            for importid in double.importids[:-1]:
                deletequery = "DELETE FROM {source}_import WHERE id = {importid}".format(
                    source=self.sourceConfig.get('table'),
                    importid=importid)
                self.db.execute(deletequery)
            count += 1

        logger.debug(
            '[{elapsed:.2f} seconds] Filtered {doubles} records with more than one entry in the source data'.format(
                doubles=count,
                elapsed=(timer() - lap))
        )

    @db_session
    def list_changes(self):
        """
        Identifies differences between the current database and the imported data. It does this
        by comparing hashes.

        If a hash is missing in the current database, but if it is present in the imported, than
        it could be a new record, or an update.

        A hash that is present in the current data, but is missing in the imported data can be
        deleted record. But this comparison can only be done with complete datasets. The changes
        dictionary looks something like this.

        The 'update' changes should be pairs of record id, which point to the id of
        records in the import and the current databases.

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
            leftdiffquery = 'SELECT {source}_import.id, {source}_import.hash ' \
                            'FROM {source}_import ' \
                            'FULL OUTER JOIN {source}_current ON {source}_import.hash = {source}_current.hash ' \
                            'WHERE {source}_current.hash is null'.format(source=source_base)
            neworupdates = self.db.select(leftdiffquery)
            logger.debug(
                '[{elapsed:.2f} seconds] Left full outer join on "{source}": {count}'.format(
                    source=source_base,
                    elapsed=(timer() - lap),
                    count=len(neworupdates)
                )
            )
            lap = timer()

            if not self.is_incremental():
                # this part is only done when a source is non incremental
                rightdiffquery = 'SELECT {source}_current.id, {source}_current.hash ' \
                                 'FROM {source}_import ' \
                                 'FULL OUTER JOIN {source}_current ON {source}_import.hash = {source}_current.hash ' \
                                 'WHERE {source}_import.hash is null'.format(source=source_base)
                updateOrDeletes = self.db.select(rightdiffquery)
                logger.debug(
                    '[{elapsed:.2f} seconds] Right full outer join on "{source}": {count}'.format(
                        source=source_base,
                        elapsed=(timer() - lap),
                        count=len(updateOrDeletes)
                    )
                )
            lap = timer()

            importtable = globals()[source_base.capitalize() + '_import']
            currenttable = globals()[source_base.capitalize() + '_current']

            # new or update
            for result in neworupdates:
                r = importtable.get(hash=result[1])
                if (r.rec):
                    uuid = r.rec[idField]
                    if self.is_incremental() and self.get_record(uuid):
                        oldrec = self.get_record(uuid)
                        self.changes['update'][uuid] = [r.id]
                        self.changes['update'][uuid].append(oldrec.id)
                        logger.debug('Update {oldid} to {newid}'.format(
                            oldid=oldrec.id,
                            newid=r.id
                        ))
                    else:
                        self.changes['new'][uuid] = [r.id]

            if not self.is_incremental():
                # incremental sources only have explicit deletes
                for result in updateOrDeletes:
                    r = currenttable.get(hash=result[1])
                    if (r.rec):
                        uuid = r.rec[idField]
                        if self.changes['new'].get(uuid, False):
                            self.changes['update'][uuid] = self.changes['new'].get(uuid)
                            self.changes['update'][uuid].append(r.id)
                            del self.changes['new'][uuid]
                        else:
                            self.changes['delete'][uuid] = [r.id]

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
        Handle new records
        """
        table = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id')
        index = self.sourceConfig.get('index', 'noindex')
        importTable = globals()[table.capitalize() + '_import']
        srcEnrich = self.sourceConfig.get('src-enrich', False)
        dstEnrich = self.sourceConfig.get('dst-enrich', None)

        deltaFile = self.open_deltafile('new', index)
        # Schrijf de data naar incrementele files

        lap = timer()
        for jsonId, databaseIds in self.changes['new'].items():
            importId = databaseIds[0]
            importRec = importTable[importId]
            jsonRec = importRec.rec
            if srcEnrich:
                jsonRec = self.enrich_record(jsonRec, srcEnrich)

            insertQuery = "INSERT INTO {table}_current (rec, hash, datum) " \
                          "SELECT rec, hash, datum FROM {table}_import where id={id}".format(
                            table=self.sourceConfig.get('table'),
                            id=importId)

            if (deltaFile):
                json.dump(jsonRec, deltaFile)
                deltaFile.write('\n')

            self.db.execute(insertQuery)

            if (dstEnrich):
                code = self.sourceConfig.get('code')
                self.cache_taxon_record(jsonRec, code)

            self.log_change(
                state='new',
                recid=importRec.rec[idField]
            )
            logger.debug(
                '[{elapsed:.2f} seconds] New record "{recordid}" inserted in "{source}"'.format(
                    elapsed=(timer() - lap),
                    source=table + '_current',
                    recordid=importRec.rec[idField]
                )
            )
            lap = timer()
        if deltaFile:
            deltaFile.close()

    @db_session
    def handle_updates(self):
        """
        Handles updates by storing the import records into the current table.

        When a source record needs to get enriched it retrieves the enrichment part from a taxon record and adds it
        to the record.

        If the source record enriches another source, the impacted records get enriched again.
        """
        tableBase = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id')
        enrichDestinations = self.sourceConfig.get('dst-enrich', None)
        enrichSources = self.sourceConfig.get('src-enrich', None)
        importTable = globals()[tableBase.capitalize() + '_import']
        currentTable = globals()[tableBase.capitalize() + '_current']
        index = self.sourceConfig.get('index', 'noindex')

        deltaFile = self.open_deltafile('update', index)
        # Write updated records to the deltafile

        lap = timer()
        for change, recordIds in self.changes['update'].items():
            # first id points to the new rec
            importRec = importTable[recordIds[0]]
            oldRec = currentTable[recordIds[1]]
            jsonRec = importRec.rec

            # If this record should be enriched by specified sources
            if enrichSources:
                jsonRec = self.enrich_record(jsonRec, enrichSources)

            updateQuery = "UPDATE {table}_current SET (rec, hash, datum) = " \
                          "(SELECT rec, hash, datum FROM {table}_import " \
                          "WHERE {table}_import.id={importid}) " \
                          "WHERE {table}_current.id={currentid}".format(
                            table=tableBase,
                            currentid=recordIds[1],
                            importid=importRec.id)
            if deltaFile:
                json.dump(jsonRec, deltaFile)
                deltaFile.write('\n')

            self.db.execute(updateQuery)

            # If this record has impact on records that should be enriched again
            if (enrichDestinations):
                code = self.sourceConfig.get('code')
                self.cache_taxon_record(jsonRec, code)

                for source in enrichDestinations:
                    logger.debug(
                        'Enrich source = {source}'.format(source=source)
                    )
                    self.handle_impacted(source, oldRec)

            logger.debug(
                '[{elapsed:.2f} seconds] Updated record "{recordid}" in "{source}"'.format(
                    source=tableBase + '_current',
                    elapsed=(timer() - lap),
                    recordid=importRec.rec[idField]
                )
            )
            self.log_change(
                state='update',
                recid=importRec.rec[idField],
            )
            lap = timer()

        if deltaFile:
            deltaFile.close()

    @db_session
    def handle_deletes(self):
        """
        Handles temporary deletes
        """
        table = self.sourceConfig.get('table')
        idField = self.sourceConfig.get('id')
        currentTable = globals()[table.capitalize() + '_current']
        enriches = self.sourceConfig.get('enriches', None)
        index = self.sourceConfig.get('index', 'noindex')

        # Write data to deltafile file
        deltaFile = self.open_deltafile('delete', index)

        lap = timer()
        for change, dbids in self.changes['delete'].items():
            oldRecord = currentTable[dbids[0]]
            if oldRecord:
                jsonRec = oldRecord.rec
                deleteId = oldRecord.rec.get(idField)
                if deltaFile:
                    deleteRecord = self.create_delete_record(self.source, deleteId, 'REJECTED')
                    json.dump(deleteRecord, deltaFile)
                    deltaFile.write('\n')

                # @todo: register the delete in Deleted_records table
                # if it already exists, up the counter
                oldRecord.delete()

                self.log_change(
                    state='delete',
                    recid=deleteId
                )

                if enriches:
                    code = self.sourceConfig.get('code')
                    self.cache_taxon_record(jsonRec, code)

                    for source in enriches:
                        logger.debug('Enrich source = {source}'.format(source=source))
                        self.handle_impacted(source, oldRecord)

                logger.debug(
                    '[{elapsed:.2f} seconds] Temporarily deleted record "{deleteid}" in "{source}"'.format(
                        source=table + '_current',
                        elapsed=(timer() - lap),
                        deleteid=deleteId
                    )
                )
                lap = timer()

                logger.info("Record [{deleteid}] deleted".format(deleteid=deleteId))

        if (deltaFile):
            deltaFile.close()

    def list_impacted(self, sourceConfig, scientificNameGroup):
        """
        Looks for impacted records based on scientificnamegroup

        :param scientificNameGroup:
        :return bool:
        """
        table = sourceConfig.get('table')
        currenttable = globals()[table.capitalize() + '_current']

        jsonsql = 'rec->\'identifications\' @> \'[{"scientificName":{"scientificNameGroup":"%s"}}]\'' % (
            scientificNameGroup
        )
        items = currenttable.select(lambda p: raw_sql(jsonsql))

        if (len(items)):
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
            logger.debug(items.get_sql())
            return False

    def get_taxon(self, source, scientificNameGroup):
        """
        Retrieve a taxon from the database on the field 'acceptedName.scientificNameGroup'

        :param source:
        :param scientificNameGroup:
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
        taxon = cache.get(taxonKey)
        if taxon is not None:
            logger.debug('get_taxon: {taxonkey} got json from cache'.format(
                taxonkey=taxonKey
            ))
            return taxon

        currentTable = globals().get(table.capitalize() + '_current')
        if not currentTable:
            return False

        # Retrieve the taxon from the database
        sciSql = 'rec->\'acceptedName\' @> \'{"scientificNameGroup":"%s"}\'' % (
            scientificNameGroup
        )
        taxonQuery = currentTable.select(lambda p: raw_sql(sciSql))
        taxon = taxonQuery.get()

        if taxon:
            logger.debug('get_taxon: {taxonkey} store json in cache'.format(
                taxonkey=taxonKey
            ))
            cache.set(taxonKey, taxon.rec)
            return taxon.rec
        else:
            # No taxon found, store this also in the cache
            logger.debug('get_taxon: {taxonkey} store FALSE in cache'.format(
                taxonkey=taxonKey
            ))
            cache.set(taxonKey, False)
            return False

    def cache_taxon_record(self, jsonRec, systemCode):
        if jsonRec.get('acceptedName') and jsonRec.get('acceptedName').get('scientificNameGroup'):
            scientificNameGroup = jsonRec.get('acceptedName').get('scientificNameGroup')
            taxonKey = '_'.join([systemCode, scientificNameGroup])

            logger.debug('cache_taxon_record: {taxonkey} store json in cache'.format(
                taxonkey=taxonKey
            ))
            cache.set(taxonKey, jsonRec)

    def create_name_summary(self, vernacularName):
        """
        Create a scientific name summary, only use certain fields

        :param vernacularName:
        :return:
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
        Create a scientific summary, only use certain fields

        :param scientificName:
        :return:
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

    def create_enrichment(self, rec, source):
        """
        Create the enrichment

        :param rec:
        :param source:
        :return:
        """
        lap = timer()
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

        logger.debug(
            '[{elapsed:.2f} seconds] Created enrichment for "{scinamegroup}" in "{source}"'.format(
                source=source,
                elapsed=(timer() - lap),
                scinamegroup=scientificNameGroup
            )
        )

        return enrichment

    def create_delete_record(self, source, recordId, status='REJECTED'):
        sourceConfig = None
        if self.config.get('sources').get(source):
            sourceConfig = self.config.get('sources').get(source)

        deleteRecord = dict()

        deleteRecord['unitID'] = recordId
        deleteRecord['sourceSytemCode'] = sourceConfig.get('code', '')
        deleteRecord['status'] = status

        return deleteRecord

    def get_enrichment(self, sciNameGroup, source):
        """
        First tries to retrieve the enrichment from cache. When it is not generated
        a new enrichment is created from a taxon json record and stored in cache

        :param sciNameGroup:
        :param source:
        :return:
        """
        lap = timer()
        taxonJson = self.get_taxon(source, sciNameGroup)

        if taxonJson:
            return self.create_enrichment(taxonJson, source)
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
        Enrich a json record with taxon information from the sources it does this
        by checking each element in identifications[] and if it contains a
        'scientificName.scientificNameGroup' it tries to generate an enrichment
        from each source

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
                rec.get('identifications')[index]['taxonomicEnrichments'] = []

                for source in sources:
                    enrichment = self.get_enrichment(sciNameGroup, source)
                    if (enrichment):
                        rec.get('identifications')[index]['taxonomicEnrichments'].append(enrichment)

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
        index = self.sourceConfig.get('index', 'noindex')

        lap = timer()

        # Retrieve scientificNameGroup from the acceptedName part
        if record.rec.get('acceptedName'):
            scientificNameGroup = record.rec.get('acceptedName').get('scientificNameGroup')

        if scientificNameGroup:
            impactedRecords = self.list_impacted(sourceConfig, scientificNameGroup)
            if (impactedRecords):
                deltaFile = self.open_deltafile('enrich', index)
                if (deltaFile):
                    for impacted in impactedRecords:
                        jsonRecord = impacted.rec
                        if enrichmentSources:
                            jsonRecord = self.enrich_record(jsonRecord, enrichmentSources)

                        json.dump(jsonRecord, deltaFile)
                        deltaFile.write('\n')

                        impactId = impacted.rec[idField]
                        logger.debug(
                            '[{elapsed:.2f} seconds] Record "{recordid}" of "{source}" needs to be enriched'.format(
                                source=source,
                                elapsed=(timer() - lap),
                                recordid=impactId
                            )
                        )
                        self.log_change(
                            state='enrich',
                            recid=impactId
                        )
                        lap = timer()
                    deltaFile.close()

    @db_session
    def handle_changes(self):
        """
        Handles the changes
        """
        self.list_changes()

        if (len(self.changes['new'])):
            self.handle_new()
        if (len(self.changes['update'])):
            self.handle_updates()
        if (not self.sourceConfig.get('incremental')):
            # Alleen incrementele deletes afhandelen als de bron complete sets levert
            if (len(self.changes['delete'])):
                self.handle_deletes()

        return
