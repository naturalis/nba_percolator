# -*- coding: utf-8 -*-
"""NBA preprocessing database module

Hierin zitten alle functies en database afhankelijkheden waarmee import data 
kan worden gefilterd alvorens een import in de NBA documentstore plaatsvindt.
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
from pony.orm import db_session
from .schema import *

# Setup logging
logging.basicConfig(format=u'%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ppdb_nba')
logger.setLevel(logging.INFO)

class ppdbNBA():
    """
    Preprocessor class containing all the functions needed for importing the data and
    create incremental insert, update or delete files.
    """

    def __init__(self, config):
        """
        Inlezen van de config.yml file waarin alle bronnen en hun specifieke wensen in moeten worden vermeld.
        """
        if isinstance(config, str):
            # config is string, read the config file
            try:
                with open(file=config, mode='r') as ymlfile:
                    self.config = yaml.load(ymlfile)
            except:
                msg = '"config.yml" with configuration options of sources is missing'
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

        self.jobid = ''

    def set_source(self, source):
        """
        Setting the data source of the import (and it's source config)

        :param source:
        """

        if (self.config.get('sources').get(source)):
            self.source_config = self.config.get('sources').get(source)
        else:
            msg = 'Source "%s" does not exist in config file' % (source)
            sys.exit(msg)

    def connect_to_elastic(self):
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
         Contact maken met postgres database
        """
        global ppdb

        self.db = ppdb

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

        # Tabel definities met pony db
        try:
            self.db.generate_mapping(create_tables=True)
        except:
            msg = 'Creating tables needed for preprocessing failed'
            logger.fatal(msg)
            sys.exit(msg)

    def parse_job(self, jobfile=''):
        """
        Parses a json job file, and tries to retrieve the validated file names
        then returns a dictionary of sources with a list of files.

        :rtype: object
        """
        files = {}
        with open(jobfile) as json_data:
            jobrec = json.load(json_data)
            # Get the name of the supplier
            self.supplier = jobrec.get('data_supplier')

            # Get the date of the job
            self.jobdate = jobrec.get('date')

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
        files = self.parse_job(jobfile)

        incoming_path = self.config.get('paths').get('incoming', '/tmp')
        processed_path = self.config.get('paths').get('processed', '/tmp')

        for source,filenames in files.items():
            for filename in filenames:
                self.set_source(source.lower())
                filepath = os.path.join(incoming_path, filename)
                destpath = os.path.join(processed_path, filename)

                try:
                    self.import_data(table=self.source_config.get('table') + '_import', datafile=filepath)
                except Exception:
                    logger.error("Import of '{file}' into '{source}' failed".format(file=filepath,source=source.lower()))
                    return False

                shutil.move(filepath,destpath)
                self.remove_doubles()
                self.handle_changes()

        return True

    def open_deltafile(self, action='new', index='unknown'):
        """
        Open een delta bestand met records of id's om weg te schrijven.
        """
        destpath = self.config.get('paths').get('delta', '/tmp')
        filename = "{index}-{ts}-{action}.json".format(
            index=index,
            ts=time.strftime('%Y%m%d%H%M%S'),
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

    def lock_datafile(self,datafile=''):
        destpath = self.config.get('paths').get('delta', '/tmp')
        lockfile = os.path.basename(datafile) + '.lock'
        filepath = os.path.join(destpath, lockfile)
        if (os.path.isfile(filepath)) :
            # Lock file already exists
            return False
        else :
            with open(file=filepath, mode='a'):
                os.utime(filepath, None)
            return True

    def log_change(self, state='unknown', recid='', comment=''):
        """
        Logging of the state change of a record

        :param state:
        :param recid:
        :param comment:
        :return:
        """
        rec = {
            '@timestamp' : datetime.now().isoformat(),
            'state' : state,
            'ppd_timestamp' : datetime.now().isoformat(),
            'comment' : comment
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
        """ Verwijder data uit tabel. """
        self.db.execute("TRUNCATE public.{table}".format(table=table))
        logger.debug('Truncated table "{table}"'.format(table=table))

    @db_session
    def import_deleted(self, filename=''):

        table = self.source_config.get('table')
        currenttable = globals()[table.capitalize() + '_current']
        idfield = self.source_config.get('id','id')
        enriches = self.source_config.get('enriches', None)
        lap = timer()

        delids = []
        try :
            with open(file=filename, mode='r') as f:
                delids = f.read().splitlines()
        except:
            msg = '"{filename}" cannot be read'.format(filename=filename)
            logger.fatal(msg)
            sys.exit(msg)

        fp = None
        for id in delids:
            if (not fp):
                fp = self.open_deltafile('kill', table)
            if (fp):
                fp.write('{deleteid}\n'.format(deleteid=id))
            oldrec = currenttable.select(lambda p: p.rec[idfield] == id).get()
            if (oldrec):
                if (enriches):
                    for source in enriches:
                        logger.debug('Enrich source = {source}'.format(source=source))

                        self.handle_enrichment(source, oldrec)

                oldrec.delete()
                logger.debug(
                    '[{elapsed:.2f} seconds] Permanently deleted (kill) record "{recordid}" in "{source}"'.format(
                        source=table + '_current',
                        elapsed=(timer() - lap),
                        recordid=id
                    )
                )
                lap = timer()

        if (fp):
            fp.close()

    @db_session
    def import_data(self, table='', datafile='', enriched=False):
        """
        Importeer data direct in de postgres database. En laat zoveel mogelijk over aan postgres zelf.
        """
        lap = timer()

        # Use the name of the filename as a job id
        self.jobid = datafile.split('/')[-1]

        self.db.execute("TRUNCATE public.{table}".format(table=table))

        # gooi de tabel leeg, weg met de indexes
        self.db.execute("ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS hindex".format(table=table))
        self.db.execute("DROP INDEX IF EXISTS public.idx_{table}__jsonid".format(table=table))
        self.db.execute("DROP INDEX IF EXISTS public.idx_{table}__hash".format(table=table))
        self.db.execute("DROP INDEX IF EXISTS public.idx_{table}__gin".format(table=table))

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
            logger.fatal('Import of "{datafile}" into "{table}" failed:\n\n{error}'.format(table=table,datafile=datafile, error=str(err)))
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

        if (enriched):
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


    @db_session
    def remove_doubles(self):
        """ Bepaalde bronnen kunnen dubbele records bevatten, deze moeten eerst worden verwijderd, voordat de hash vergelijking wordt uitgevoerd. """
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
        Identificeert de verschillen tussen de huidige database en de nieuwe data, op basis van hash.

        Als een hash ontbreekt in de bestaande data, maar aanwezig is in de nieuwe data. Dan kan het gaan
        om een nieuw (new) record of een update.

        Een hash die aanwezig is in de bestaande data, maar ontbreekt in de nieuwe data kan gaan om een
        verwijderd record. Maar dit is alleen te bepalen bij analyse van complete datasets. Een changes
        dictionary ziet er over het algemeen zo uit.

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
                '[{elapsed:.2f} seconds] Left full outer join on "{source}"'.format(source=source_base, elapsed=(timer() - lap)))
            lap = timer()

            rightdiffquery = 'SELECT {source}_current.id, {source}_current.hash ' \
                             'FROM {source}_import ' \
                             'FULL OUTER JOIN {source}_current ON {source}_import.hash = {source}_current.hash ' \
                             'WHERE {source}_import.hash is null'.format(source=source_base)
            updateordeletes = self.db.select(rightdiffquery)
            logger.debug(
                '[{elapsed:.2f} seconds] Right full outer join on "{source}"'.format(source=source_base, elapsed=(timer() - lap)))
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
                    else :
                        self.changes['delete'][uuid] = [r.id]

            if (len(self.changes['new']) or len(self.changes['update']) or len(self.changes['delete'])):
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
        Afhandelen van alle nieuwe records.
        """
        table = self.source_config.get('table')
        idfield = self.source_config.get('id')
        importtable = globals()[table.capitalize() + '_import']

        fp = self.open_deltafile('new', self.source_config.get('table'))
        # Schrijf de data naar incrementele files

        lap = timer()
        for jsonid, dbids in self.changes['new'].items():
            importid = dbids[0]
            importrec = importtable[importid]
            insertquery = "INSERT INTO {table}_current (rec, hash, datum) " \
                          "SELECT rec, hash, datum FROM {table}_import where id={id}".format(
                table=self.source_config.get('table'),
                id=importid
            )
            if (fp):
                json.dump(importrec.rec, fp)
                fp.write('\n')

            self.db.execute(insertquery)

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
        Afhandelen van alle updates.
        """
        table = self.source_config.get('table')
        idfield = self.source_config.get('id')
        enriches = self.source_config.get('enriches', None)
        importtable = globals()[table.capitalize() + '_import']
        currenttable = globals()[table.capitalize() + '_import']

        fp = self.open_deltafile('update', self.source_config.get('table'))
        # Schrijf de data naar incrementele file

        lap = timer()
        for change, dbids in self.changes['update'].items():
            importrec = importtable[dbids[0]]
            oldrec = currenttable[dbids[0]]
            updatequery = "UPDATE {table}_current SET (rec, hash, datum) = " \
                          "(SELECT rec, hash, datum FROM {table}_import " \
                          "WHERE {table}_import.id={importid}) " \
                          "WHERE {table}_current.id={currentid}".format(
                              table=table,
                              currentid=dbids[1],
                              importid=importrec.id
                          )
            if (fp):
                json.dump(importrec.rec, fp)
                fp.write('\n')

            if (enriches):
                for source in enriches:
                    logger.debug(
                        'Enrich source = {source}'.format(source=source)
                    )
                    self.handle_enrichment(source, oldrec)

            self.db.execute(updatequery)
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
        Afhandelen van temporarily deletes.
        """
        table = self.source_config.get('table')
        idfield = self.source_config.get('id')
        currenttable = globals()[table.capitalize() + '_current']
        enriches = self.source_config.get('enriches', None)

        fp = self.open_deltafile('delete', self.source_config.get('table'))
        # Schrijf de data naar incrementele file

        lap = timer()
        for change, dbids in self.changes['delete'].items():
            oldrec = currenttable[dbids[0]]
            if (oldrec):
                deleteid = oldrec.rec[idfield]
                if (fp):
                    fp.write('{deleteid}\n'.format(deleteid=deleteid))

                if (enriches):
                    for source in enriches:
                        logger.debug('Enrich source = {source}'.format(source=source))
                        self.handle_enrichment(source, oldrec)

                oldrec.delete()

                self.log_change(
                    state='delete',
                    recid=deleteid
                )

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
        Zoekt uit welke gerelateerde records opnieuw verrijkt moeten worden.

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

    @db_session
    def handle_enrichment(self, source, rec):
        scientificnamegroup = None
        source_config = self.config.get('sources').get(source)
        idfield = source_config.get('id')

        lap = timer()

        if (rec.rec.get('acceptedName')):
            scientificnamegroup = rec.rec.get('acceptedName').get('scientificNameGroup')

        if (scientificnamegroup):
            impactedrecords = self.list_impacted(source_config, scientificnamegroup)
            if (impactedrecords):
                fp = self.open_deltafile('enrich', source_config.get('table'))
                if (fp):
                    for impactedrec in impactedrecords:
                        json.dump(impactedrec.rec, fp)
                        fp.write('\n')
                        impactid=impactedrec.rec[idfield]
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
        Afhandelen van alle veranderingen.
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
