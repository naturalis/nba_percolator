<h1 id="ppdb_nba">ppdb_nba</h1>

Dit is de NBA preprocessing database class.

Hierin zitten alle functies en database afhankelijkheden waarmee import data
kan worden gefilterd alvorens een import in de NBA documentstore plaatsvind.

## installatie

Installeren kan het beste via pip. Dit is een python3 module.

`pip install -e git+https://github.com/naturalis/ppdb_nba.git#egg=ppdb_nba`

Vervolgens wordt de module `ppdb_nba` en het commando `ppdb_nba` toegevoegd
aan je executable path. De aanroep is meestal:

```
ppdb_nba --source [bronnaam] /pad/naar/jsonlinesfile.txt
```

Meer opties zijn te vinden bij aanroep met --help

```
ppdb_nba --help
```

En om de class te gebruiken:

```python
from ppdb_nba import ppdbNBA
pp = ppdbNBA(config='config.yml',source='/pad/naar/jsonlinesfile.json')
```

<h2 id="ppdb_nba.ppdb_nba.open_deltafile">ppdbNBA.open_deltafile</h2>

```python
pp.open_deltafile(action='new', index='unknown')
```

Open een delta bestand met records of id's om weg te schrijven.

<h2 id="ppdb_nba.ppdb_nba.clear_data">ppdbNBA.clear_data</h2>

```python
pp.clear_data(table='')
```
Verwijder data uit tabel.
<h2 id="ppdb_nba.ppdb_nba.import_data">ppdbNBA.import_data</h2>

```python
pp.import_data(table='', datafile='')
```

Importeert data direct in de postgres database. En laat zoveel mogelijk over aan postgres zelf.

<h2 id="ppdb_nba.ppdb_nba.remove_doubles">ppdbNBA.remove_doubles</h2>

```python
pp.remove_doubles()
```
Bepaalde bronnen bevatte dubbele records, deze moeten eerst worden verwijderd, voordat de hash vergelijking wordt uitgevoerd.
<h2 id="ppdb_nba.ppdb_nba.list_changes">ppdbNBA.list_changes</h2>

```python
pp.list_changes()
```

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


<h2 id="ppdb_nba.ppdb_nba.handle_new">ppdbNBA.handle_new</h2>

```python
pp.handle_new()
```

Afhandelen van alle nieuwe records.


<h2 id="ppdb_nba.ppdb_nba.handle_updates">ppdbNBA.handle_updates</h2>

```python
pp.handle_updates()
```

Afhandelen van alle updates.

<h2 id="ppdb_nba.ppdb_nba.handle_deletes">ppdbNBA.handle_deletes</h2>

```python
pp.handle_deletes()
```

Afhandelen van alle deletes.

<h2 id="ppdb_nba.ppdb_nba.handle_changes">ppdbNBA.handle_changes</h2>

```python
pp.handle_changes()
```

Afhandelen van alle veranderingen.


## Voorbeelden

Voorbeeld van een script dat een import doet.

```python
from ppdb_nba import *
logger.setLevel(logging.DEBUG)
# Zet de logging level op DEBUG

pp = ppdbNBA(config='config.yml',source='brahms-specimen')
# configureer de preprocessor
pp.clear_data(table=pp.source_config.get('table') + "_import")
# verwijder de data uit de import tabel
pp.import_data(table=pp.source_config.get('table') + "_import", datafile='/data/brahms-specimen/1-base.json')
# importeer de basis data
pp.remove_doubles()
# verwijder de dubbele
pp.list_changes()
# bepaal de veranderingen (allemaal nieuwe records)
pp.handle_changes()
# handel de nieuwe af
```

