# -*- coding: utf-8 -*-
"""Dit is de NBA preprocessing database module.

Hierin zitten alle functies en database afhankelijkheden waarmee import data 
kan worden gefilterd alvorens een import in de NBA documentstore plaatsvind.
"""
import json
import logging
import os
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
stopwatch = timer()


class ppdbNBA():
    """
    Preprocessor class containing all the functions needed for importing the data and
    create incremental insert, update or delete files.
    """

    def __init__(self, config, source):
        """
        Inlezen van de config.yml file waarin alle bronnen en hun specifieke wensen in moeten worden vermeld.
        """
        if isinstance(config, str):
            # config is string, read the config file
            try:
                with open(config, 'r') as ymlfile:
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
        if (self.config.get('sources').get(source)):
            self.source_config = self.config.get('sources').get(source)
        else:
            msg = 'Source "%s" does not exist in config file' % (source)
            sys.exit(msg)

        if (self.source_config.get('es', False)):
            self.es = self.connect_to_elastic()

        self.connect_to_database()

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
        # Contact maken met postgres database
        global ppdb
        self.db = ppdb

        try:
            self.db.bind(
                provider='postgres',
                user=self.config['postgres']['user'],
                password=self.config['postgres']['pass'],
                host=self.config['postgres']['host'],
                database=self.config['postgres']['db'])
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

    def open_deltafile(self, action='new', index='unknown'):
        """
        Open een delta bestand met records of id's om weg te schrijven.
        """
        destpath = self.config.get('deltapath', '/tmp')
        filename = "{index}-{ts}-{action}.json".format(index=index, ts=time.strftime('%Y%m%d%H%M%S'), action=action)
        filepath = os.path.join(destpath, filename)

        try:
            fp = open(filepath, 'a')
        except:
            msg = 'Unable to write to "{filepath}"'.format(filepath=filepath)
            logger.fatal(msg)
            sys.exit(msg)
        logger.debug(filepath + ' opened')

        return fp

    def kill_index(self):
        """
        Verwijder de index uit elastic search.
        """
        if (self.source_config):
            index = self.source_config.get('index', 'specimen')
            self.es.indices.delete(index=index, ignore=[400, 404])
            logger.info('Elastic search index "{index}" removed'.format(index=index))

    @db_session
    def clear_data(self, table=''):
        """ Verwijder data uit tabel. """
        self.db.execute("TRUNCATE public.{table}".format(table=table))
        logger.debug('Truncated table "{table}"'.format(table=table))

    @db_session
    def import_data(self, table='', datafile='', enriched=False):
        """
        Importeer data direct in de postgres database. En laat zoveel mogelijk over aan postgres zelf.
        """
        start = timer()
        # todo: check if table exists
        # todo: check if data file exists and is readable
        self.db.execute("TRUNCATE public.{table}".format(table=table))

        # gooi de tabel leeg, drop indexes
        self.db.execute("ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS hindex".format(table=table))
        self.db.execute("DROP INDEX IF EXISTS public.idx_{table}__jsonid".format(table=table))
        self.db.execute("DROP INDEX IF EXISTS public.idx_{table}__hash".format(table=table))
        self.db.execute("DROP INDEX IF EXISTS public.idx_{table}__gin".format(table=table))

        # verwijder de indexes
        self.db.execute("ALTER TABLE public.{table} ALTER COLUMN hash DROP NOT NULL".format(table=table))

        # db.execute("COPY public.{table} (rec) FROM '{datafile}'".format(table=table, datafile=datafile))
        self.db.execute(
            "COPY public.{table} (rec) FROM '{datafile}' CSV QUOTE e'\x01' DELIMITER e'\x02'".format(table=table,
                                                                                                     datafile=datafile))

        # import alle data
        #
        # @todo: In bijvoorbeeld de xenocanto waarnemingen zitten velden met quotes in de tekst. Die zijn zo
        # gecodeerd: "dit is \"een\" voorbeeld". Dit accepteert het COPY statement niet. Die verwacht dubbele
        # backslashes.  Hier moet iets op bedacht worden.
        #
        self.db.execute("UPDATE {table} SET hash=md5(rec::text)".format(table=table))
        # zet de hash
        self.db.execute(
            "CREATE INDEX idx_{table}__hash ON public.{table} USING btree (hash) TABLESPACE pg_default".format(
                table=table))
        # zet hashing index
        if (enriched):
            self.db.execute(
                "CREATE INDEX idx_{table}__gin ON public.{table} USING gin((rec->'identifications') jsonb_path_ops)".format(
                    table=table))
        # zet json record index voor scientificNameGroup

        elapsed = "%0.2f" % (timer() - start)
        logger.debug(
            'Imported data "{datafile}" into "{table} [{elapsed} seconds]'.format(datafile=datafile, table=table,
                                                                                  elapsed=elapsed))

    @db_session
    def remove_doubles(self):
        """ Bepaalde bronnen bevatte dubbele records, deze moeten eerst worden verwijderd, voordat de hash vergelijking wordt uitgevoerd. """
        start = timer()
        self.db.execute(
            "CREATE INDEX idx_{source}_import__jsonid ON public.{source}_import((rec->>'{idfield}'))".format(
                source=self.source_config.get('table'), idfield=self.source_config.get('id')))
        elapsed = "%0.2f" % (timer() - start)
        logger.debug('Index [{elapsed} seconds]'.format(elapsed=elapsed))
        doublequery = "SELECT array_agg(id) importids, rec->>'{idfield}' recid " \
                      "FROM {source}_import GROUP BY rec->>'{idfield}' HAVING COUNT(*) > 1".format(
            source=self.source_config.get('table'), idfield=self.source_config.get('id'))
        doubles = self.db.select(doublequery)
        elapsed = "%0.2f" % (timer() - start)
        logger.debug('Find doubles [{elapsed} seconds]'.format(elapsed=elapsed))
        count = 0
        for double in doubles:
            for importid in double.importids[:-1]:
                deletequery = "DELETE FROM {source}_import WHERE id = {importid}".format(
                    source=self.source_config.get('table'),
                    importid=importid)
                self.db.execute(deletequery)
            count += 1
        elapsed = "%0.2f" % (timer() - start)
        logger.debug(
            'Filtered {doubles} records with more than one entry in the source data [{elapsed} seconds]'.format(
                doubles=count, elapsed=elapsed))

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
                'new': [
                    '3732672@BRAHMS',
                    '1369617@BRAHMS',
                    '2455323@BRAHMS'
                ],
                'update': [],
                'delete': []
            }
        ```

        """
        self.changes = {'new': [], 'update': [], 'delete': []}
        source_base = self.source_config.get('table')
        idfield = self.source_config.get('id')

        if (len(source_base)):
            leftdiffquery = 'SELECT {source}_import.hash FROM {source}_import ' \
                            'FULL OUTER JOIN {source}_current ON {source}_import.hash = {source}_current.hash ' \
                            'WHERE {source}_current.hash is null'.format(source=source_base)
            neworupdates = self.db.select(leftdiffquery)

            rightdiffquery = 'SELECT {source}_current.hash FROM {source}_import ' \
                             'FULL OUTER JOIN {source}_current ON {source}_import.hash = {source}_current.hash ' \
                             'WHERE {source}_import.hash is null'.format(source=source_base)
            updateordeletes = self.db.select(rightdiffquery)

            importtable = globals()[source_base.capitalize() + '_import']
            currenttable = globals()[source_base.capitalize() + '_current']

            # new or update
            for result in neworupdates:
                r = importtable.get(hash=result)
                if (r.rec):
                    self.changes['new'].append(r.rec[idfield])

            # updates or deletes
            for result in updateordeletes:
                r = currenttable.get(hash=result)
                if (r.rec):
                    uid = r.rec[idfield]
                    try:
                        self.changes['new'].index(uid)
                        self.changes['new'].remove(uid)
                        self.changes['update'].append(uid)
                    except:
                        self.changes['delete'].append(uid)

            if (len(self.changes['new']) or len(self.changes['update']) or len(self.changes['delete'])):
                logger.info('{new} new, {update} updated and {delete} removed'.format(new=len(self.changes['new']),
                                                                                      update=len(
                                                                                          self.changes['update']),
                                                                                      delete=len(
                                                                                          self.changes['delete'])))
            else:
                logger.info('No changes')
        return self.changes

    @db_session
    def handle_new(self):
        """
        Afhandelen van alle nieuwe records.
        """
        table = self.source_config.get('table')
        idfield = self.source_config.get('id')
        importtable = globals()[table.capitalize() + '_import']
        currenttable = globals()[table.capitalize() + '_current']

        fp = self.open_deltafile('new', self.source_config.get('table')) if not self.source_config.get(
            'elastic') else False
        # Geen toegang tot elastic search? Schrijf de data naar incrementele files

        for change in self.changes['new']:
            importrec = importtable.select(lambda p: p.rec[idfield] == change).get()
            if (importrec):
                insertquery = "insert into {table}_current (rec, hash, datum) " \
                              "select rec, hash, datum from {table}_import where id={id}".format(
                    table=self.source_config.get('table'),
                    id=importrec.id)
                if (fp):
                    json.dump(importrec.rec, fp)
                    fp.write('\n')

                if (self.es):
                    logger.debug("New record [{id}] posted to NBA".format(id=importrec.rec[idfield]))
                    self.es.index(index=self.source_config.get('index'),
                                  doc_type=self.source_config.get('doctype', 'unknown'),
                                  body=importrec.rec, id=importrec.rec[idfield])
                self.db.execute(insertquery)
                logger.info("New record [{id}] inserted".format(id=importrec.rec[idfield]))
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
        currenttable = globals()[table.capitalize() + '_current']

        fp = self.open_deltafile('update', self.source_config.get('table')) if not self.source_config.get(
            'elastic') else False
        # Geen toegang tot elastic search? Schrijf de data naar incrementele files

        for change in self.changes['update']:
            newrec = importtable.select(lambda p: p.rec[idfield] == change).get()
            oldrec = currenttable.select(lambda p: p.rec[idfield] == change).get()
            if (newrec and oldrec):
                updatequery = "update {table}_current set (rec, hash, datum) = " \
                              "(select rec, hash, datum from {table}_import where {table}_import.id={importid}) " \
                              "where {table}_current.id={currentid}".format(table=table,
                                                                            currentid=oldrec.id,
                                                                            importid=newrec.id)
                if (fp):
                    json.dump(newrec.rec, fp)
                    fp.write('\n')

                if (self.es):
                    logger.debug("Updated record [{id}] to NBA".format(id=newrec.rec[idfield]))
                    self.es.index(index=self.source_config.get('index'),
                                  doc_type=self.source_config.get('doctype', 'unknown'),
                                  body=newrec.rec,
                                  id=newrec.rec[idfield])

                if (enriches):
                    for source in enriches:
                        logger.debug('Enrich source = {source}'.format(source=source))
                        self.handle_enrichment(source, oldrec)

                self.db.execute(updatequery)

                logger.info("Record [{id}] updated".format(id=newrec.rec[idfield]))
        if (fp):
            fp.close()

    @db_session
    def handle_deletes(self):
        """
        Afhandelen van alle deletes.
        """
        table = self.source_config.get('table')
        idfield = self.source_config.get('id')
        currenttable = globals()[table.capitalize() + '_current']
        enriches = self.source_config.get('enriches', None)

        fp = self.open_deltafile('delete', self.source_config.get('table')) if not self.source_config.get(
            'elastic') else False
        # Geen toegang tot elastic search? Schrijf de data naar incrementele files

        for change in self.changes['delete']:
            oldrec = currenttable.select(lambda p: p.rec[idfield] == change).get()
            if (oldrec):
                deleteid = oldrec.rec[idfield]
                if (fp):
                    fp.write('{deleteid}\n'.format(deleteid=deleteid))

                if (self.es):
                    # delete from elastic search index
                    self.es.delete(index=self.source_config.get('index'),
                                   doc_type=self.source_config.get('doctype', 'unknown'),
                                   id=oldrec.rec[idfield],
                                   ignore=[400, 404])
                    logger.debug("Delete record [{id}] from NBA".format(id=deleteid))

                if (enriches):
                    for source in enriches:
                        logger.debug('Enrich source = {source}'.format(source=source))
                        self.handle_enrichment(source, oldrec)

                oldrec.delete()
                logger.info("Record [{deleteid}] deleted".format(deleteid=deleteid))
        if (fp):
            fp.close()

    def list_impacted(self, scientificnamegroup):
        """
        Zoekt uit welke gerelateerde records opnieuw verrijkt moeten worden.

        :param scientificnamegroup:
        :return bool:
        """
        table = self.source_config.get('table')
        currenttable = globals()[table.capitalize() + '_current']

        jsonsql = 'rec->\'identifications\' @> \'[{"scientificName":{"scientificNameGroup":"%s"}}]\'' % (
            scientificnamegroup)
        items = currenttable.select(lambda p: raw_sql(jsonsql))

        if (len(items)):
            logger.info(
                "Found {number} records in {source} with scientificNameGroup={namegroup}".format(number=len(items),
                                                                                                 source=table.capitalize(),
                                                                                                 namegroup=scientificnamegroup))
            return items
        else:
            logger.error("Found no records in {source} with scientificNameGroup={namegroup}".format(number=len(items),
                                                                                                    source=table.capitalize(),
                                                                                                    namegroup=scientificnamegroup))
            logger.debug(items.get_sql())
            return False

    @db_session
    def handle_enrichment(self, rec):
        scientificnamegroup = None

        if (rec.rec.get('acceptedName')):
            scientificnamegroup = rec.rec.get('acceptedName').get('scientificNameGroup')

        if (scientificnamegroup):
            impactedrecords = self.list_impacted(self.source_config, scientificnamegroup)
            if (impactedrecords):
                fp = self.open_deltafile('enrich', self.source_config.get('table'))
                for impactedrec in impactedrecords:
                    json.dump(impactedrec.rec, fp)
                    fp.write('\n')
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
            if (len(self.changes['delete'])):
                self.handle_deletes()

        return
