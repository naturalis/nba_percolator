"""Dit is het NBA preprocessing database schema.

Hierin zitten tabel definities waarmee import data kan worden gefilterd alvorens
een import in de NBA documentstore plaatsvind.
"""
from datetime import datetime
from pony.orm import Database, Optional, Json, Required, raw_sql

ppdb = Database()

class Nsrtaxa_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Nsrtaxa_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Nsrmedia_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Nsrmedia_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Crsspecimen_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Crsspecimen_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Crsmedia_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Crsmedia_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Brahmsspecimen_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Brahmsspecimen_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Brahmsmedia_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Brahmsmedia_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Xenocantospecimen_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Xenocantospecimen_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Xenocantomedia_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Xenocantomedia_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Coltaxa_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Coltaxa_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Waarnemingspecimen_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Waarnemingspecimen_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Waarnemingmedia_current(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Waarnemingmedia_import(ppdb.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')
