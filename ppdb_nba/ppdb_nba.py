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
from timeit import default_timer as timer
import yaml
from elasticsearch import Elasticsearch
from pony.orm import db_session, set_sql_debug
from dateutil import parser
from diskcache import Cache
from .schema import *

# Setup logging
logging.basicConfig(format=u'%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ppdb_nba')
logger.setLevel(logging.INFO)

# Caching on disk (diskcache) using sqlite, it is fast
cache = Cache('tmp')
cache.clear()


class ppdb_NBA():
    """
    Preprocessor class containing all functions needed for importing the data and
    create incremental insert, update or delete files.
    """

    def __init__(self, config):
        """
        Reading the config.yml file where all sources, configuration and
        specifics are listed
        """
        if isinstance(config, str):
            # config is string, read the config file
            try:
                with open(file=config, mode='r') as ymlfile:
                    self.config = yaml.load(ymlfile)
            except:
                msg = '"config.yml" with configuration options of sources is missing in working directory'
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

        if (not self.config.get('sources', False)):
            msg = 'Sources part missing in config'
            sys.exit(msg)

        self.es = self.connect_to_elastic()
        self.connect_to_database()

        self.jobdate = datetime.now()

        self.jobid = ''

    def set_source(self, source):
        """
        Setting the data source of the import (and it's source config)

        :param source:
        """

        self.source = source
        if (self.config.get('sources').get(source)):
            self.source_config = self.config.get('sources').get(source)
        else:
            msg = 'Source "%s" does not exist in config file' % (source)
            sys.exit(msg)

    def connect_to_elastic(self):
        """
        Connect to elastic search for logging

        :return:
        """
        # Verbinden met Elastic search
        try:
            es = Elasticsearch(hosts=self.config['elastic']['host'])
            return es
        except:
            msg = 'Cannot connect to elastic search server'
            logger.fatal(msg)
            sys.exit(msg)

    def connect_to_database(self):
        """
        Connect to postgres database
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
        except:
            msg = 'Cannot connect to postgres database'
            logger.fatal(msg)
            sys.exit(msg)

    def generate_mapping(self, create_tables=False):
        """
        Generate mapping of the database
        """
        try:
            self.db.generate_mapping(create_tables=create_tables)
        except:
            msg = 'Creating tables needed for preprocessing failed'
            logger.fatal(msg)
            sys.exit(msg)

    def lock(self, jobfile):
        """
        Generates a locking file
        """
        jobs_path = self.config.get('paths').get('jobs', os.getcwd() + "/jobs")
        with open(os.path.join(jobs_path, '.lock'), 'w') as fp:
            lockrec = {
                'job': jobfile,
                'pid': os.getpid()
            }
            json.dump(lockrec, fp)

    def unlock(self):
        """
        Remove the locking file
        """
        jobspath = self.config.get('paths').get('jobs', os.getcwd() + "/jobs")
        locks = glob.glob(os.path.join(jobspath, '.lock'))
        lock = locks.pop()

        os.remove(lock)

    def islocked(self):
        """
        Check if there is a lockfile and if so, parse it. A lockfile is a
        json record with PID as well as job filepath. If the process is
        still running then the process is still locked. If not it has
        failed. If no lock file exists it is no longer locked.

        :return:  True = still locked / False = no longer locked
        """
        jobspath = self.config.get('paths').get('jobs', os.getcwd() + "/jobs")

        locks = glob.glob(jobspath + '/.lock')
        if (len(locks) > 0):
            lockfile = locks.pop()
            with open(file=lockfile, mode='r') as fp:
                lockinfo = json.load(fp)

            # check of the process in the lockfile is still running, kill signal=0
            # this does not kill the process, just checks if the process is there
            try:
                os.kill(lockinfo['pid'], 0)
                logger.info(
                    'Preprocessor still processing (PID={pid}), handling job file "{job}"'.format(pid=lockinfo['pid'],
                                                                                                  job=lockinfo['job']))
                return True
            except:
                # Exception means the process is no longer running, but the
                # lockfile is still there
                jobfile = lockinfo['job'].split('/')[-1]
                failedpath = self.config.get('paths').get('failed', os.path.join(os.getcwd(), "failed"))
                shutil.move(lockinfo['job'], os.path.join(failedpath, jobfile))

                self.log_change(
                    state='fail'
                )
                logger.error(
                    'Preprocessor failed in the last run? '
                    'Job file "{job}" moved to failed'.format(job=lockinfo['job'])
                )
                os.remove(lockfile)

        return False

    def parse_job(self, jobfile=''):
        """
        Parse a json job file, and tries to retrieve the validated file names
        then returns a dictionary of sources with a list of files.

        :rtype: object
        """
        files = {}
        with open(jobfile) as json_data:
            jobrec = json.load(json_data)
            # Get the id of the job
            self.jobid = jobrec.get('id')

            # Get the name of the supplier
            self.supplier = jobrec.get('data_supplier')

            # Get the date of the job
            rawdate = jobrec.get('date', False)
            if rawdate:
                self.jobdate = parser.parse(rawdate)

            # Parse the validator part, get the outfiles
            if jobrec.get('validator'):
                for key in jobrec.get('validator').keys():
                    export = jobrec.get('validator').get(key)
                    for validfile in export.get('results').get('outfiles').get('valid'):
                        source = self.supplier + '-' + key
                        if source not in files:
                            files[source] = []
                        files[source].append(validfile.split('/')[-1])
        return files

    def handle_job(self, jobfile=''):
        """
        Handle the job

        :param jobfile:
        :return:
        """
        self.lock(jobfile)

        files = self.parse_job(jobfile)
        incoming_path = self.config.get('paths').get('incoming', '/tmp')

        self.log_change(
            state='start'
        )

        # import each file
        for source, filenames in files.items():
            for filename in filenames:
                self.set_source(source.lower())
                filepath = os.path.join(incoming_path, filename)

                self.log_change(
                    state='import',
                    comment='{filepath}'.format(filepath=filepath)
                )
                try:
                    self.import_data(table=self.source_config.get('table') + '_import', datafile=filepath)
                except Exception:
                    # import fails? remove the lock, return false
                    logger.error(
                        "Import of '{file}' into '{source}' failed".format(file=filepath, source=source.lower()))
                    return False

                # import successful, move the data file
                processed_path = os.path.join(self.config.get('paths').get('processed', '/tmp'), filename)
                shutil.move(filepath, processed_path)
                self.remove_doubles()
                self.handle_changes()

        self.log_change(
            state='finish'
        )

        # everything, okay and finished, remove the lock
        self.unlock()

        return True

    def open_deltafile(self, action='new', index='unknown'):
        """
        Open the delta file for the updated, new or deleted records
        """
        destpath = self.config.get('paths').get('delta', '/tmp')
        if not self.jobid:
            filename = "{ts}-{index}-{action}.json".format(
                index=index,
                ts=time.strftime('%Y%m%d%H%M%S'),
                action=action
            )
        else:
            filename = "{jobid}-{index}-{action}.json".format(
                jobid=self.jobid,
                index=index,
                action=action
            )
        filepath = os.path.join(destpath, filename)

        try:
            fp = open(filepath, 'a')
        except:
            msg = 'Unable to write to "{filepath}"'.format(filepath=filepath)
            logger.fatal(msg)
            sys.exit(msg)
        logger.debug(filepath + ' opened')

        return fp

    def lock_datafile(self, datafile=''):
        """
        Locking for single datafiles, this is different from the locking of jobs.
        Maybe it should be combined.

        :param datafile:
        :return:
        """
        destpath = self.config.get('paths').get('delta', '/tmp')
        lockfile = os.path.basename(datafile) + '.lock'
        filepath = os.path.join(destpath, lockfile)

        if (os.path.isfile(filepath)):
            # Lock file already exists
            return False
        else:
            with open(file=filepath, mode='a'):
                os.utime(filepath, None)
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
            'ppd_timestamp': self.jobdate.isoformat(),
            'comment': comment
        }

        try:
            self.es.index(
                index=self.jobid.lower(),
                id=recid,
                doc_type='logging',
                body=json.dumps(rec)
            )
        except:
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
        table = self.source_config.get('table')
        index = self.source_config.get('index', 'noindex')
        currenttable = globals()[table.capitalize() + '_current']
        idfield = self.source_config.get('id', 'id')
        enriches = self.source_config.get('dst-enrich', None)
        lap = timer()

        delids = []
        try:
            with open(file=filename, mode='r') as f:
                delids = f.read().splitlines()
        except:
            msg = '"{filename}" cannot be read'.format(filename=filename)
            logger.fatal(msg)
            sys.exit(msg)

        fp = None
        for id in delids:
            if (not fp):
                fp = self.open_deltafile('kill', index)
            if (fp):
                fp.write('{deleteid}\n'.format(deleteid=id))
            oldqry = currenttable.select(lambda p: p.rec[idfield] == id)

            oldrec = oldqry.get()
            if (oldrec):
                oldrec.delete()

                if (enriches):
                    for source in enriches:
                        logger.debug('Enrich source = {source}'.format(source=source))

                        self.handle_impacted(source, oldrec)

                logger.debug(
                    '[{elapsed:.2f} seconds] Permanently deleted (kill) record "{recordid}" in "{source}"'.format(
                        source=table + '_current',
                        elapsed=(timer() - lap),
                        recordid=id
                    )
                )
                lap = timer()

        if fp:
            fp.close()

    @db_session
    def import_data(self, table='', datafile=''):
        """
        Imports data directly to the postgres database.
        """
        lap = timer()

        src_enrich = self.source_config.get('src-enrich', False)
        dst_enrich = self.source_config.get('dst-enrich', False)

        # Use the name of the filename as a job id
        if not self.jobid:
            filename = datafile.split('/')[-1]
            self.jobid = filename.replace('.json','')

        self.db.execute("TRUNCATE public.{table}".format(table=table))

        # gooi de tabel leeg, weg met de indexes
        self.db.execute('ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS hindex'.format(table=table))
        self.db.execute('DROP INDEX IF EXISTS public.idx_{table}__jsonid'.format(table=table))
        self.db.execute('DROP INDEX IF EXISTS public.idx_{table}__hash'.format(table=table))
        self.db.execute('DROP INDEX IF EXISTS public.idx_{table}__gin'.format(table=table))

        # verwijder de indexes
        self.db.execute("ALTER TABLE public.{table} ALTER COLUMN hash DROP NOT NULL".format(table=table))
        logger.debug('[{elapsed:.2f} seconds] Reset "{table}" for import'.format(table=table, elapsed=(timer() - lap)))
        lap = timer()

        # db.execute("COPY public.{table} (rec) FROM '{datafile}'".format(table=table, datafile=datafile))
        # import alle data
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
                'Import of "{datafile}" into "{table}" failed:\n\n{error}'.format(table=table, datafile=datafile,
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
            '[{elapsed:.2f} seconds] Set hashing index on "{table}"'.format(table=table, elapsed=(timer() - lap)))
        lap = timer()

        # zet de jsonid index
        lap = timer()
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_{table}__jsonid "
            "ON public.{table} USING BTREE(({table}.rec->>'{idfield}'))".format(
                table=table,
                idfield=self.source_config.get('id')
            )
        )
        logger.debug(
            '[{elapsed:.2f} seconds] Set index on jsonid '.format(
                elapsed=(timer() - lap)
            )
        )

        # set an index on identifications, which should be present in enriched data
        if src_enrich:
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
        if dst_enrich:
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
    def remove_doubles(self):
        """
        Some sources can contain double records, these should be removed,
        before checking the hash.
        """
        lap = timer()

        doublequery = "SELECT array_agg(id) importids, rec->>'{idfield}' recid " \
                      "FROM {source}_import " \
                      "GROUP BY rec->>'{idfield}' HAVING COUNT(*) > 1".format(
            source=self.source_config.get('table'),
            idfield=self.source_config.get('id')
        )
        doubles = self.db.select(doublequery)
        logger.debug('[{elapsed:.2f} seconds] Find doubles'.format(elapsed=(timer() - lap)))
        lap = timer()

        count = 0
        for double in doubles:
            for importid in double.importids[:-1]:
                deletequery = "DELETE FROM {source}_import WHERE id = {importid}".format(
                    source=self.source_config.get('table'),
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

        self.changes = {'new': {}, 'update': {}, 'delete': {}}
        source_base = self.source_config.get('table')
        idfield = self.source_config.get('id')

        lap = timer()
        if (len(source_base)):
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

            # @todo: non incremental updates should check the updates this way
            # this part should be skipped if the source is 'incremental==no'
            rightdiffquery = 'SELECT {source}_current.id, {source}_current.hash ' \
                             'FROM {source}_import ' \
                             'FULL OUTER JOIN {source}_current ON {source}_import.hash = {source}_current.hash ' \
                             'WHERE {source}_import.hash is null'.format(source=source_base)
            updateordeletes = self.db.select(rightdiffquery)
            logger.debug(
                '[{elapsed:.2f} seconds] Right full outer join on "{source}": {count}'.format(
                    source=source_base,
                    elapsed=(timer() - lap),
                    count=len(updateordeletes)
                )
            )
            lap = timer()

            importtable = globals()[source_base.capitalize() + '_import']
            currenttable = globals()[source_base.capitalize() + '_current']

            # new or update
            for result in neworupdates:
                r = importtable.get(hash=result[1])
                if (r.rec):
                    uuid = r.rec[idfield]
                    self.changes['new'][uuid] = [r.id]

            # updates or deletes
            for result in updateordeletes:
                r = currenttable.get(hash=result[1])
                if (r.rec):
                    uuid = r.rec[idfield]
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
        table = self.source_config.get('table')
        idfield = self.source_config.get('id')
        index = self.source_config.get('index', 'noindex')
        importtable = globals()[table.capitalize() + '_import']
        src_enrich = self.source_config.get('src-enrich', False)
        dst_enrich = self.source_config.get('dst-enrich', None)

        fp = self.open_deltafile('new', index)
        # Schrijf de data naar incrementele files

        lap = timer()
        for jsonid, dbids in self.changes['new'].items():
            importid = dbids[0]
            importrec = importtable[importid]
            jsonrec = importrec.rec
            if src_enrich:
                jsonrec = self.enrich_record(jsonrec, src_enrich)

            insertquery = "INSERT INTO {table}_current (rec, hash, datum) " \
                          "SELECT rec, hash, datum FROM {table}_import where id={id}".format(
                table=self.source_config.get('table'),
                id=importid
            )

            if (fp):
                json.dump(jsonrec, fp)
                fp.write('\n')

            self.db.execute(insertquery)

            if (dst_enrich):
                code = self.source_config('code')
                self.cache_taxon_record(jsonrec, code)

            self.log_change(
                state='new',
                recid=importrec.rec[idfield]
            )
            logger.debug(
                '[{elapsed:.2f} seconds] New record "{recordid}" inserted in "{source}"'.format(
                    elapsed=(timer() - lap),
                    source=table + '_current',
                    recordid=importrec.rec[idfield]
                )
            )
            lap = timer()
        if (fp):
            fp.close()

    @db_session
    def handle_updates(self):
        """
        Handles updates
        """
        table = self.source_config.get('table')
        idfield = self.source_config.get('id')
        dst_enrich = self.source_config.get('dst-enrich', None)
        src_enrich = self.source_config.get('src-enrich', None)
        importtable = globals()[table.capitalize() + '_import']
        currenttable = globals()[table.capitalize() + '_import']
        index = self.source_config.get('index', 'noindex')

        fp = self.open_deltafile('update', index)
        # Schrijf de data naar incrementele file

        lap = timer()
        for change, dbids in self.changes['update'].items():
            importrec = importtable[dbids[0]]
            oldrec = currenttable[dbids[0]]
            jsonrec = importrec.rec

            if (src_enrich) :
                jsonrec = self.enrich_record(jsonrec, src_enrich)

            updatequery = "UPDATE {table}_current SET (rec, hash, datum) = " \
                          "(SELECT rec, hash, datum FROM {table}_import " \
                          "WHERE {table}_import.id={importid}) " \
                          "WHERE {table}_current.id={currentid}".format(
                table=table,
                currentid=dbids[1],
                importid=importrec.id
            )
            if (fp):
                json.dump(jsonrec, fp)
                fp.write('\n')

            self.db.execute(updatequery)

            if (dst_enrich):
                code = self.source_config.get('code')
                self.cache_taxon_record(jsonrec, code)

                for source in dst_enrich:
                    logger.debug(
                        'Enrich source = {source}'.format(source=source)
                    )
                    self.handle_impacted(source, oldrec)

            logger.debug(
                '[{elapsed:.2f} seconds] Updated record "{recordid}" in "{source}"'.format(
                    source=table + '_current',
                    elapsed=(timer() - lap),
                    recordid=importrec.rec[idfield]
                )
            )
            self.log_change(
                state='update',
                recid=importrec.rec[idfield],
            )
            lap = timer()

        if (fp):
            fp.close()

    @db_session
    def handle_deletes(self):
        """
        Handles temporary deletes
        """
        table = self.source_config.get('table')
        idfield = self.source_config.get('id')
        currenttable = globals()[table.capitalize() + '_current']
        enriches = self.source_config.get('enriches', None)
        index = self.source_config.get('index', 'noindex')

        # Write data to incremental file
        fp = self.open_deltafile('delete', index)

        lap = timer()
        for change, dbids in self.changes['delete'].items():
            oldrec = currenttable[dbids[0]]
            if (oldrec):
                jsonrec = oldrec.rec
                deleteid = oldrec.rec.get(idfield)
                if (fp):
                    fp.write('{deleteid}\n'.format(deleteid=deleteid))

                oldrec.delete()

                self.log_change(
                    state='delete',
                    recid=deleteid
                )

                if (enriches):
                    code = self.source_config.get('code')
                    self.cache_taxon_record(jsonrec, code)

                    for source in enriches:
                        logger.debug('Enrich source = {source}'.format(source=source))
                        self.handle_impacted(source, oldrec)

                logger.debug(
                    '[{elapsed:.2f} seconds] Temporarily deleted record "{deleteid}" in "{source}"'.format(
                        source=table + '_current',
                        elapsed=(timer() - lap),
                        deleteid=deleteid
                    )
                )
                lap = timer()

                logger.info("Record [{deleteid}] deleted".format(deleteid=deleteid))
        if (fp):
            fp.close()

    def list_impacted(self, source_config, scientificnamegroup):
        """
        Looks for impacted records based on scientificnamegroup

        :param scientificnamegroup:
        :return bool:
        """
        table = source_config.get('table')
        currenttable = globals()[table.capitalize() + '_current']

        jsonsql = 'rec->\'identifications\' @> \'[{"scientificName":{"scientificNameGroup":"%s"}}]\'' % (
            scientificnamegroup
        )
        items = currenttable.select(lambda p: raw_sql(jsonsql))

        if (len(items)):
            logger.info(
                "Found {number} records in {source} with scientificNameGroup={namegroup}".format(
                    number=len(items),
                    source=table.capitalize(),
                    namegroup=scientificnamegroup)
            )
            return items
        else:
            logger.error(
                "Found no records in {source} with scientificNameGroup={namegroup}".format(
                    number=len(items),
                    source=table.capitalize(),
                    namegroup=scientificnamegroup)
            )
            logger.debug(items.get_sql())
            return False

    def get_taxon(self, source, sciNameGroup):
        """
        Retrieve a taxon from the database on the field 'acceptedName.scientificNameGroup'

        :param source:
        :param sciNameGroup:
        :return:
        """
        source_config = self.config.get('sources').get(source, False)
        if not source_config:
            return False

        table = source_config.get('table')
        if not table:
            return False

        code = source_config.get('code')

        taxonkey = '_'.join([code,sciNameGroup])
        taxon = cache.get(taxonkey)
        if taxon != None:
            return taxon

        currenttable = globals().get(table.capitalize() + '_current', False)
        if not currenttable:
            return False

        scisql = 'rec->\'acceptedName\' @> \'{"scientificNameGroup":"%s"}\'' % (
            sciNameGroup
        )
        taxaqry = currenttable.select(lambda p: raw_sql(scisql))
        taxon = taxaqry.get()

        if (taxon):
            cache.set(taxonkey, taxon)
        else:
            cache.set(taxonkey, False)

        return taxon

    def cache_taxon_record(self, jsonRec, systemCode):
        if jsonRec.get('acceptedName') and jsonRec.get('acceptedName').get('scientificNameGroup'):
            scientificNameGroup = jsonRec.get('acceptedName').get('scientificNameGroup')
            taxonKey = '_'.join([systemCode, scientificNameGroup])
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
        sciNameGroup = rec.get('acceptedName').get('scientificNameGroup')
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
                if rec.get('defaultClassification') :
                    enrichment['defaultClassification'] = rec.get('defaultClassification')

        logger.debug(
            '[{elapsed:.2f} seconds] Created enrichment for "{scinamegroup}" in "{source}"'.format(
                source=source,
                elapsed=(timer() - lap),
                scinamegroup=sciNameGroup
            )
        )

        return enrichment

    def get_enrichment(self, sciNameGroup, source):
        """
        First tries to retrieve the enrichment from cache. When it is not generated
        a new enrichment is created from a taxon json record and stored in cache

        :param sciNameGroup:
        :param source:
        :return:
        """
        lap = timer()
        taxon = self.get_taxon(source, sciNameGroup)

        if taxon:
            self.create_enrichment(taxon.rec, source)
        else :
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
        if rec.get('identifications', False):
            identifications = rec.get('identifications')
            for index, ident in enumerate(identifications):
                if ident.get('scientificName') and ident.get('scientificName').get('scientificNameGroup'):
                    sciNameGroup = ident.get('scientificName').get('scientificNameGroup')
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
        scientificnamegroup = None
        source_config = self.config.get('sources').get(source)
        src_enrich = source_config.get('src-enrich', False)
        idfield = source_config.get('id')
        index = self.source_config.get('index', 'noindex')

        lap = timer()

        if record.rec.get('acceptedName'):
            scientificnamegroup = record.rec.get('acceptedName').get('scientificNameGroup')

        if scientificnamegroup:
            impactedrecords = self.list_impacted(source_config, scientificnamegroup)
            if (impactedrecords):
                fp = self.open_deltafile('enrich', index)
                if (fp):
                    for impactedrec in impactedrecords:
                        jsonrec = impactedrec.rec
                        if src_enrich:
                            jsonrec = self.enrich_record(jsonrec, src_enrich)

                        json.dump(jsonrec, fp)
                        fp.write('\n')

                        impactid = impactedrec.rec[idfield]
                        logger.debug(
                            '[{elapsed:.2f} seconds] Record "{recordid}" of "{source}" needs to be enriched'.format(
                                source=source,
                                elapsed=(timer() - lap),
                                recordid=impactid
                            )
                        )
                        self.log_change(
                            state='enrich',
                            recid=impactid
                        )
                        lap = timer()
                    fp.close()

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
        if (not self.source_config.get('incremental')):
            # Alleen incrementele deletes afhandelen als de bron complete sets levert
            if (len(self.changes['delete'])):
                self.handle_deletes()

        return
