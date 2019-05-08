"""Dit is het NBA preprocessing database schema.

Hierin zitten tabel definities waarmee import data kan worden gefilterd alvorens
een import in de NBA documentstore plaatsvind.
"""
from datetime import datetime
from pony.orm import Database, Optional, Json, Required, raw_sql

db = Database()


class Nsrtaxa_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Nsrtaxa_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Nsrmedia_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Nsrmedia_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Crsspecimen_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Crsspecimen_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Crsmedia_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Crsmedia_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Brahmsspecimen_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Brahmsspecimen_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Brahmsmedia_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Brahmsmedia_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Xenocantospecimen_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Xenocantospecimen_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Xenocantomedia_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Xenocantomedia_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Coltaxa_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Coltaxa_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Waarnemingspecimen_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Waarnemingspecimen_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Waarnemingmedia_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Waarnemingmedia_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Testspecimen_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Testspecimen_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')

class Testmedia_import(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Testmedia_current(db.Entity):
    rec = Optional(Json)
    hash = Optional(str, index=True)
    datum = Required(datetime, sql_default='now()')


class Deleted_records(db.Entity):
    recid = Required(str, index=True)
    status = Required(str, index=True)
    count = Required(int, sql_default=1)
    datum = Required(datetime, sql_default='now()')
