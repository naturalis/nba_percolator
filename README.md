<h1 id="percolator">nba_percolator.Percolator</h1>

This is the NBA preprocessing database class.

Preprocessor class containing all functions needed for importing the 
data and create incremental insert, update or delete files.


## installatie

Install can be done through pip. This is a python3 module.

`pip install -e git+https://github.com/naturalis/nba_percolator.git#egg=nba_percolator`

This installs `nba_percolator` as well as the `percolator` script in your executable
path.

```
percolator
```

or

```
percolator --source [bronnaam] /shared-data/source/jsonlinesfile.json
```

**LET OP: Het pad naar de jsonlines bestand moet exact 
hetzelfde zijn als die voor postgres instance. En bij voorkeur 
absoluut!**

Meer opties zijn te vinden bij aanroep met --help

```
usage: percolator --source sourcename /path/file1

Preprocessing data to create incremental updates

positional arguments:
  files            One json data file

optional arguments:
  -h, --help       show this help message and exit
  --source SOURCE  Name of the data source
  --config CONFIG  Config file
  --current        Import data directly to current table (default is normal
                   incremental import)
  --delete         Handle permanent deletes (default is normal incremental
                   import)
  --force          Ignore lockfiles, to force the import
  --createtables   Generate database tables needed for importing
  --debug          Set debugging level logging
```

## Cron

By default the percolator script will be run by the crontab scheduler.

```
 * * * * * cd /shared-data && percolator
```

Periodiek scant `percolator` de jobs directory. De files die hier worden 
aangetroffen worden op volgorde van timestamp (oplopend) verwerkt. Er wordt 
maar één job per keer verwerkt. Op het moment dat een job wordt behandelt 
wordt er een lock file gezet, zolang die lock file er staat wordt er geen 
import proces gestart. In de lock file wordt de naam van het
job bestand gezet.

Na het succesvol afhandelen van de import file(s) in een job worden de 
jsonlines bestanden in `./imported/` gezet. De job file gaat naar done. 
Als een job niet succesvol wordt afgehandeld wordt hij in failed gezet, 
de import data blijft in dit geval staan.

Nadat een job file is afgehandeld wordt de .lock file weer verwijderd, 
waarna de volgende job wordt opgepakt.

## Logging

percolator logt naar een elastic search server. Alle relevante acties 
worden weggeschreven:

 - new
 - update
 - delete
 - kill
 
Dit gebeurt met de functie 
[log_change](https://github.com/naturalis/nba_percolator/blob/c499b29875254045e0093006d8655731973a9129/ppdb_nba/nba_percolator.py#L316).

## Directory structuur

De onderstaande mappen structuur is volledig instelbaar via `config.yml`, die in
de root van de shared data directory. Op de huidige productie machine is dat in
`/data/shared-data`, de mappenstructuur staat alsvolgt gedefinieerd:

```
paths:
    incoming: /shared-data/incoming
    processed: /shared-data/processed
    jobs: /shared-data/jobs
    failed: /shared-data/failed
    done: /shared-data/done
    delta: /shared-data/incremental 
```

**LET OP: De paden moeten in alle docker containers te vinden zijn op 
dezelfde locatie  `/shared-data/' anders gaat het mis.**

### /shared-data/incoming

Hier komen alle json lines bestanden die (nog) moeten worden opgepakt.

### /shared-data/jobs

De json files in deze directory bevatten de meta informatie over
te importeren data. Job files die in behandeling zijn worden
gelocked in de `.lock` file. 

### /shared-data/processed

Hier komen de data files terecht die succesvol zijn afgehandeld.

### /shared-data/done

Hier komen de job files terecht die zijn afgehandeld.

### /shared-data/failed

Hier komen de job files terecht die *niet* succesvol zijn afgehandeld.

### /shared-data/incremental

Hier komen alle `delta` veranderingen op de nba database. Dit zijn 
de records die kunnen worden ingelezen en gepost naar de elastic 
search database.

### Import proces

Periodiek scant `percolator` de jobs directory. De files die hier worden 
aangetroffen worden op volgorde van timestamp (oplopend) verwerkt. Er
wordt maar een job per keer verwerkt. Op het moment dat een job wordt
behandelt wordt er een lock file gezet, zolang die lock file wordt
er geen import proces gestart. In de lock file wordt de naam van het
job bestand gezet.

Na het succesvol afhandelen van de import file(s) in een job worden de 
jsonlines bestanden in `./imported/` gezet. De job file gaat naar done.
Als een job niet succesvol wordt afgehandeld wordt hij in failed gezet,
de import data blijft in dit geval staan.

Nadat een job file is afgehandeld wordt de .lock file weer verwijderd, 
waarna de volgende job wordt opgepakt.


## Class

Om de class te gebruiken:

```python
from nba_percolator import Percolator
pp = Percolator(config='config.yml')
pp.set_source('naam-van-bron')
```

<h2 id="nba_percolator.nba_percolator.clear_data">Percolator.clear_data</h2>

```python
pp.clear_data(table='')
```

Remove data from table

<h2 id="nba_percolator.nba_percolator.import_data">Percolator.import_data</h2>

```python
pp.import_data(table='', datafile='')
```

Imports data directly to the postgres database.

<h2 id="nba_percolator.nba_percolator.remove_doubles">Percolator.remove_doubles</h2>

```python
pp.remove_doubles()
```

Removes double records. Some sources can contain double records,
these should be removed, before checking the hash.

<h2 id="nba_percolator.nba_percolator.list_changes">Percolator.list_changes</h2>

```python
pp.list_changes()
```

Identifies differences between the current database and the imported 
data. It does this by comparing hashes.

If a hash is missing in the current database, but if it is present 
in the imported, than it could be a new record, or an update.

A hash that is present in the current data, but is missing 
in the imported data can be a deleted record. But this comparison 
can only be done with complete datasets. The changes
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
The 'update' changes should be pairs of record id, which point to the id of
records in the import and the current databases.


<h2 id="nba_percolator.nba_percolator.handle_new">Percolator.handle_new</h2>

```python
pp.handle_new()
```

Handles all new records.


<h2 id="nba_percolator.nba_percolator.handle_updates">Percolator.handle_updates</h2>

```python
pp.handle_updates()
```

Handles all updated records.

<h2 id="nba_percolator.nba_percolator.handle_deletes">Percolator.handle_deletes</h2>

```python
pp.handle_deletes()
```

Handles all deleted records.

<h2 id="nba_percolator.nba_percolator.handle_changes">Percolator.handle_changes</h2>

```python
pp.handle_changes()
```

Handles all changes.

## Percolator.import_deleted(filename='')

```python
pp.import_deleted('/path/to/listofdeleteids.txt')
```

Imports deleted records. These are *forced* deletes from sources that
do not supply complete dumps.


## Typical use

Example of a normal import flow

```python
from nba_percolator import *
logger.setLevel(logging.DEBUG)
# Zet de logging level op DEBUG

pp = Percolator(config='config.yml')
pp.set_source('brahms-specimen')

pp.clear_data(table=pp.source_config.get('table') + "_import")

pp.import_data(table=pp.source_config.get('table') + "_import", datafile='/shared-data/brahms-specimen/1-base.json')

pp.remove_doubles()

pp.list_changes()

pp.handle_changes()
```

