<h1 id="ppdb_nba">ppdb_nba</h1>

Dit is de NBA preprocessing database module.

Hierin zitten alle functies en database afhankelijkheden waarmee import data
kan worden gefilterd alvorens een import in de NBA documentstore plaatsvind.

## installeren

Installeren kan het beste via pip. Dit is een python3 module.

`pip install -e git+https://github.com/jandevires71/ppdb_nba.git#egg=ppdb_nba`

<h2 id="ppdb_nba.ppdb_nba.open_deltafile">open_deltafile</h2>

```python
open_deltafile(action='new', index='unknown')
```

Open een delta bestand met records of id's om weg te schrijven.

<h2 id="ppdb_nba.ppdb_nba.kill_index">kill_index</h2>

```python
kill_index(sourceconfig)
```

Verwijdert de index uit elastic search.

<h2 id="ppdb_nba.ppdb_nba.clear_data">clear_data</h2>

```python
clear_data(table='')
```
Verwijder data uit tabel.
<h2 id="ppdb_nba.ppdb_nba.import_data">import_data</h2>

```python
import_data(table='', datafile='')
```

Importeert data direct in de postgres database. En laat zoveel mogelijk over aan postgres zelf.

<h2 id="ppdb_nba.ppdb_nba.remove_doubles">remove_doubles</h2>

```python
remove_doubles(config)
```
Bepaalde bronnen bevatte dubbele records, deze moeten eerst worden verwijderd, voordat de hash vergelijking wordt uitgevoerd.
<h2 id="ppdb_nba.ppdb_nba.list_changes">list_changes</h2>

```python
list_changes(sourceconfig='')
```

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


<h2 id="ppdb_nba.ppdb_nba.handle_new">handle_new</h2>

```python
handle_new(changes={}, sourceconfig={})
```

Afhandelen van alle nieuwe records.

Parameters:

 * changes - dictionary met veranderingen
 * sourceconfig - de configuratie van een bron


<h2 id="ppdb_nba.ppdb_nba.handle_updates">handle_updates</h2>

```python
handle_updates(changes={}, sourceconfig={})
```

Afhandelen van alle updates.

Parameters:

 * changes - dictionary met veranderingen
 * sourceconfig - de configuratie van een bron

<h2 id="ppdb_nba.ppdb_nba.handle_deletes">handle_deletes</h2>

```python
handle_deletes(changes={}, sourceconfig={})
```

Afhandelen van alle deletes.

Parameters:

 * changes - dictionary met veranderingen
 * sourceconfig - de configuratie van een bron

<h2 id="ppdb_nba.ppdb_nba.handle_changes">handle_changes</h2>

```python
handle_changes(sourceconfig={})
```

Afhandelen van alle veranderingen.

Parameters:

 * changes - dictionary met veranderingen
 * sourceconfig - de configuratie van een bron


## Voorbeelden

Voorbeeld van een script dat een kale import doet.

```python
from ppdb_nba import *
logger.setLevel(logging.DEBUG)
# Zet de logging level op DEBUG
srcconfig = cfg.get('sources').get('brahms-specimen')
# Haal de configuratie voor de bron
clear_data(table=srcconfig.get('table') + "_import")
# Maak de current tabel leeg
kill_index(srcconfig)
# Verwijder de index uit elastic search
import_data(table=srcconfig.get('table') + "_import", datafile='/data/brahms-specimen/1-base.json')
# importeer de basis data
remove_doubles(srcconfig)
# verwijder de dubbele
changes = list_changes(srcconfig)
# bepaalde veranderingen (allemaal nieuwe records)
handle_changes(srcconfig)
# handel de nieuwe af
```

Een voorbeeld van een script dat in een bestaande tabel aanpassingen, importeert:

```python
from ppdb_nba import *
logger.setLevel(logging.DEBUG)
# Zet de logging level op DEBUG
srcconfig = cfg.get('sources').get('brahms-specimen')
# Verwijder de index uit elastic search
import_data(table=srcconfig.get('table') + "_import", datafile='/data/brahms-specimen/5-updatesnew.json')
# importeer de basis data
remove_doubles(srcconfig)
# verwijder de dubbele
changes = list_changes(srcconfig)
# bepaalde veranderingen (allemaal nieuwe records)
handle_changes(srcconfig)
# handel de nieuwe af
```

