from datetime import datetime

from pony.orm import Database, Optional, Json, Required

db = Database()
msg = 'Cannot connect to postgres database'
# Contact maken met postgres database
try:
    db.bind(provider='postgres', user='postgres', password='postgres', host='postgres', database='ppdb')
except:
    logger.fatal(msg)
    sys.exit(msg)

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

# Tabel definities met pony db
try:
    db.generate_mapping(create_tables=True)
except:
    msg = 'Creating tables needed for preprocessing failed'
    logger.fatal(msg)
    sys.exit(msg)
