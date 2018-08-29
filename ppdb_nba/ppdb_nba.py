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

FORMAT = u'%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('ppdb')
logger.setLevel(logging.INFO)
stopwatch = timer()

try:
    with open("config.yml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
except:
    msg = '"config.yml" with configuration options of sources is missing'
    logger.fatal(msg)
    sys.exit(msg)

"""
Inlezen van de sources.yml file waarin alle bronnen en hun specifieke wensen in moeten worden vermeld.
"""

# Verbinden met Elastic search
try:
    es = Elasticsearch(hosts=cfg['elastic']['host'])
except:
    msg = 'Cannot connect to elastic search server'
    logger.fatal(msg)
    sys.exit(msg)


db = Database()
msg = 'Cannot connect to postgres database'
# Contact maken met postgres database
try:
    db.bind(provider='postgres', user=cfg['postgres']['user'], password=cfg['postgres']['pass'], host=cfg['postgres']['host'], database=cfg['postgres']['db'])
except:
    logger.fatal(msg)
    sys.exit(msg)

# Tabel definities met pony db
try:
    db.generate_mapping(create_tables=True)
except:
    msg = 'Creating tables needed for preprocessing failed'
    logger.fatal(msg)
    sys.exit(msg)


def kill_index(sourceconfig):
    """Verwijderd de index uit elastic search."""
    if (sourceconfig):
        index = sourceconfig.get('index', 'specimen')
        es.indices.delete(index=index, ignore=[400, 404])
        logger.info('Elastic search index "{index}" removed'.format(index=index))


# Importeren data
@db_session
def import_data(table='', datafile=''):
    """Importeert data direct in de postgres database. En laat zoveel mogelijk over aan postgres zelf."""
    # todo: check if data file exists and is readable
    # todo: check if table exists
    db.execute("TRUNCATE public.{table}".format(table=table))
    # gooi de tabel leeg
    db.execute("ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS hindex".format(table=table))
    db.execute("DROP INDEX IF EXISTS public.idx_{table}__jsonid".format(table=table))
    db.execute("DROP INDEX IF EXISTS public.idx_{table}__hash".format(table=table))
    # verwijder de indexes
    db.execute("ALTER TABLE public.{table} ALTER COLUMN hash DROP NOT NULL".format(table=table))
    db.execute("COPY public.{table} (rec) FROM '{datafile}'".format(table=table, datafile=datafile))
    # import alle data
    #
    # @todo: In bijvoorbeeld de xenocanto waarnemingen zitten velden met quotes in de tekst. Die zijn zo
    # gecodeerd: "dit is \"een\" voorbeeld". Dit accepteert het COPY statement niet. Die verwacht dubbele
    # backslashes.  Hier moet iets op bedacht worden.
    #
    db.execute("UPDATE {table} SET hash=md5(rec::text)".format(table=table))
    # zet de hash
    db.execute(
        "CREATE INDEX idx_{table}__hash ON public.{table} USING btree (hash) TABLESPACE pg_default".format(table=table))
    # zet de index
    logger.debug('Imported data "{datafile}" into "{table}"'.format(datafile=datafile, table=table))


@db_session
def remove_doubles(config):
    """ Bepaalde bronnen bevatte dubbele records, deze moeten eerst worden verwijderd, voordat de hash vergelijking wordt uitgevoerd. """
    logger.debug('Before index: {elapsed}'.format(elapsed=(timer() - stopwatch)))
    db.execute("CREATE INDEX idx_{source}_import__jsonid ON public.{source}_import((rec->>'{idfield}'))".format(
        source=config.get('table'), idfield=config.get('id')))
    logger.debug('After index: {elapsed}'.format(elapsed=(timer() - stopwatch)))
    doublequery = "SELECT  array_agg(id) importids, rec->>'{idfield}' recid FROM {source}_import GROUP BY rec->>'{idfield}' HAVING COUNT(*) > 1".format(
        source=config.get('table'), idfield=config.get('id'))
    doubles = db.select(doublequery)
    logger.debug('After query: {elapsed}'.format(elapsed=(timer() - stopwatch)))
    count = 0
    for double in doubles:
        for importid in double.importids[:-1]:
            deletequery = "DELETE FROM {source}_import WHERE id = {importid}".format(source=config.get('table'),
                                                                                     importid=importid)
            db.execute(deletequery)
        count += 1
    logger.debug('After removing doubles: {elapsed}'.format(elapsed=(timer() - stopwatch)))
    logger.debug('Filtered {doubles} records with more than one entry in the source data'.format(doubles=count))


@db_session
def clear_data(table=''):
    """ Verwijder data uit tabel. """
    db.execute("TRUNCATE public.{table}".format(table=table))
    logger.debug('Truncated table "{table}"'.format(table=table))


def list_changes(sourceconfig=''):
    """
    Identificeert de verschillen tussen de huidige database en de nieuwe data, op basis van hash.

    Als een hash ontbreekt in de bestaande data, maar aanwezig is in de nieuwe data. Dan kan het gaan
    om een nieuw (new) record of een update.

    Een hash die aanwezig is in de bestaande data, maar ontbreekt in de nieuwe data kan gaan om een
    verwijderd record. Maar dit is alleen te bepalen bij analyse van complete datasets.
    """
    changes = {'new': [], 'update': [], 'delete': []}
    source = sourceconfig.get('table')
    idfield = sourceconfig.get('id')

    if (len(source)):
        leftdiffquery = 'SELECT {source}_import.hash FROM {source}_import FULL OUTER JOIN {source}_current ON {source}_import.hash = {source}_current.hash WHERE {source}_current.hash is null'.format(
            source=source)
        neworupdates = db.select(leftdiffquery)

        rightdiffquery = 'SELECT {source}_current.hash FROM {source}_import FULL OUTER JOIN {source}_current ON {source}_import.hash = {source}_current.hash WHERE {source}_import.hash is null'.format(
            source=source)
        updateordeletes = db.select(rightdiffquery)

        importtable = globals()[source.capitalize() + '_import']
        currenttable = globals()[source.capitalize() + '_current']

        # new or update
        for result in neworupdates:
            r = importtable.get(hash=result)
            if (r.rec):
                changes['new'].append(r.rec[idfield])

        # updates or deletes
        for result in updateordeletes:
            r = currenttable.get(hash=result)
            if (r.rec):
                uid = r.rec[idfield]
                try:
                    changes['new'].index(uid)
                    changes['new'].remove(uid)
                    changes['update'].append(uid)
                except:
                    changes['delete'].append(uid)

        if (len(changes['new']) or len(changes['update']) or len(changes['delete'])):
            logger.info('{new} new, {update} updated and {delete} removed'.format(new=len(changes['new']),
                                                                                  update=len(changes['update']),
                                                                                  delete=len(changes['delete'])))
        else:
            logger.info('No changes')
    return changes


@db_session
def handle_new(changes, sourceconfig):
    """ Afhandelen van alle nieuwe records. """
    table = sourceconfig.get('table')
    idfield = sourceconfig.get('id')
    importtable = globals()[table.capitalize() + '_import']
    currenttable = globals()[table.capitalize() + '_current']

    fp = open_deltafile('new', sourceconfig.get('table')) if not sourceconfig.get('elastic') else False
    # Geen toegang tot elastic search? Schrijf de data naar incrementele files

    for change in changes['new']:
        importrec = importtable.select(lambda p: p.rec[idfield] == change).get()
        if (importrec):
            insertquery = "insert into {table}_current (rec, hash, datum) select rec, hash, datum from {table}_import where id={id}".format(
                table=sourceconfig.get('table'), id=importrec.id)
            if (fp):
                json.dump(importrec.rec, fp)
                fp.write('\n')
            else:
                logger.debug("New record [{id}] posted to NBA".format(id=importrec.rec[idfield]))
                es.index(index=sourceconfig.get('index'), doc_type=sourceconfig.get('doctype', 'unknown'),
                         body=importrec.rec, id=importrec.rec[idfield])
            db.execute(insertquery)
            logger.info("New record [{id}] inserted".format(id=importrec.rec[idfield]))
    if (fp):
        fp.close()


@db_session
def handle_updates(changes, sourceconfig):
    """ Afhandelen van alle updates. """
    table = sourceconfig.get('table')
    idfield = sourceconfig.get('id')
    importtable = globals()[table.capitalize() + '_import']
    currenttable = globals()[table.capitalize() + '_current']

    fp = open_deltafile('update', sourceconfig.get('table')) if not sourceconfig.get('elastic') else False
    # Geen toegang tot elastic search? Schrijf de data naar incrementele files

    for change in changes['update']:
        newrec = importtable.select(lambda p: p.rec[idfield] == change).get()
        oldrec = currenttable.select(lambda p: p.rec[idfield] == change).get()
        if (newrec and oldrec):
            updatequery = "update {table}_current set (rec, hash, datum) = (select rec, hash, datum from {table}_import where {table}_import.id={importid}) where {table}_current.id={currentid}".format(
                table=table, currentid=oldrec.id, importid=newrec.id)
            if (fp):
                json.dump(newrec.rec, fp)
                fp.write('\n')
            else:
                logger.debug("Updated record [{id}] to NBA".format(id=newrec.rec[idfield]))
                es.index(index=sourceconfig.get('index'), doc_type=sourceconfig.get('doctype', 'unknown'),
                         body=newrec.rec, id=newrec.rec[idfield])
            db.execute(updatequery)
            logger.info("Record [{id}] updated".format(id=newrec.rec[idfield]))
    if (fp):
        fp.close()


@db_session
def handle_deletes(changes, sourceconfig):
    """ Afhandelen van alle deletes. """
    table = sourceconfig.get('table')
    idfield = sourceconfig.get('id')
    currenttable = globals()[table.capitalize() + '_current']

    fp = open_deltafile('delete', sourceconfig.get('table')) if not sourceconfig.get('elastic') else False
    # Geen toegang tot elastic search? Schrijf de data naar incrementele files

    for change in changes['delete']:
        oldrec = currenttable.select(lambda p: p.rec[idfield] == change).get()
        if (oldrec):
            deleteid = oldrec.rec[idfield]
            if (fp):
                fp.write('{deleteid}\n'.format(deleteid=deleteid))
            else:
                # delete from elastic search index
                es.delete(index=sourceconfig.get('index'), doc_type=sourceconfig.get('doctype', 'unknown'),
                          id=oldrec.rec[idfield], ignore=[400, 404])
                logger.debug("Delete record [{id}] from NBA".format(id=deleteid))
            oldrec.delete()
            logger.info("Record [{deleteid}] deleted".format(deleteid=deleteid))
    if (fp):
        fp.close()


@db_session
def handle_changes(sourceconfig={}):
    """ Afhandelen van alle veranderingen. """
    changes = list_changes(sourceconfig)

    if (len(changes['new'])):
        handle_new(changes, sourceconfig)
    if (len(changes['update'])):
        handle_updates(changes, sourceconfig)
    if (not sourceconfig.get('incremental')):
        if (len(changes['delete'])):
            handle_deletes(changes, sourceconfig)

    return changes


def open_deltafile(action='new', index='unknown'):
    """ Wegschrijven van deltas in een bestand """
    destpath = cfg.get('deltapath', '/tmp')
    filename = "{index}-{ts}-{action}.json".format(index=index, ts=time.strftime('%Y%m%d%H%M%S'), action=action)
    filepath = os.path.join(destpath, filename)

    try:
        fp = open(filepath, 'w')
    except:
        msg = 'Unable to write to "{filepath}"'.format(filepath=filepath)
        logger.fatal(msg)
        sys.exit(msg)
    logger.debug(filepath + ' opened')

    return fp
