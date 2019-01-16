## Enrichment tests

Om te controleren of enrichment goed werkt zijn er een
aantal test nodig. Hierbij maken we gebruik van kleine
subsets van betreffende bronnen en full sets.

### Subset tests

#### 1. Brahms specimens 'bellis*'

De data set bevat:

```json
{
  "conditions": [
    {
      "field": "identifications.scientificName.scientificNameGroup",
      "operator": "STARTS_WITH_IC",
      "value": "bellis"
    },
    {
      "field": "sourceSystem.code",
      "operator": "EQUALS_IC",
      "value": "BRAHMS"
    }
  ],
  "logicalOperator": "AND",
  "sortFields": [
    {
      "path": "unitID",
      "sortOrder": "desc"
    }
  ],
  "from": 0
}
```

[Download testset](https://api.biodiversitydata.nl/v2/specimen/download/?_querySpec=%7B%20%20%20%22conditions%22%20%3A%20%5B%0A%20%20%20%20%7B%20%22field%22%20%3A%20%22identifications.scientificName.scientificNameGroup%22%2C%20%22operator%22%20%3A%20%22STARTS_WITH_IC%22%2C%20%22value%22%20%3A%20%22bellis%22%20%7D%2C%0A%20%20%20%20%7B%20%22field%22%20%3A%20%22sourceSystem.code%22%2C%20%22operator%22%20%3A%20%22EQUALS_IC%22%2C%20%22value%22%20%3A%20%22BRAHMS%22%20%7D%0A%20%20%5D%2C%0A%20%20%22logicalOperator%22%20%3A%20%22AND%22%2C%0A%20%20%22sortFields%22%20%3A%20%5B%20%7B%20%22path%22%20%3A%20%22unitID%22%2C%20%22sortOrder%22%20%3A%20%22desc%22%20%7D%20%5D%2C%0A%20%20%22from%22%20%3A%200%0A%7D
)

```bash
ppdb_nba --current --debug --source brahms-specimen /shared-data/joepitest/enrichment_tests/bellis-before.json
```
 
 * Doe een update van 1 record
 
id = AMD.30719@BRAHMS 
 
```
locality:"Amsterdam, langs Amsterdam. Rijnkanaal."
```

wordt

```
locality:"Amsterdam, langs Amsterdam-Rijnkanaal."
```

```bash
ppdb_nba --current --debug --source brahms-specimen /shared-data/joepitest/enrichment_tests/bellis-before.json
ppdb_nba --debug --source brahms-specimen /shared-data/joepitest/enrichment_tests/bellis-1update.json
```

Verschillen na eerste run:

 1. NSR vernacularNames bevat in percolator versie ook references (moet weg)
 2. COL ppdb_nba versie gebruikt taxonId [32919201@COL](https://api.biodiversitydata.nl/v2/taxon/find/32919201@COL) i.p.v. [45441552@COL](https://api.biodiversitydata.nl/v2/taxon/find/45441552@COL) (die laatste ontbreekt in lokale col)
 3. COL synonyms bevat ook `scientificNameGroup` in ppdb_nba (moet weg?)
 4. COL defaultClassification ontbreekt in ppdb_nba (moet worden toegevoegd)
 5. COL synonym waarden worden ge-html encode in ppdb_nba versie (vreemd, uitzoeken)

ad 2. oplossen door nieuwe col taxa dump.



 * Doe een update van meer records
 
`s/Excursie/Werkbezoek/`

Acht veranderde records:

```
AMD.30760@BRAHMS
L.3014469@BRAHMS
L.3014570@BRAHMS
L.3014604@BRAHMS
L.3419551@BRAHMS
L.3419598@BRAHMS
L.3747561@BRAHMS
L.3747562@BRAHMS
```

```bash
ppdb_nba --current --debug --source brahms-specimen /shared-data/joepitest/enrichment_tests/bellis-before.json
ppdb_nba --debug --source brahms-specimen /shared-data/joepitest/enrichment_tests/bellis-moreupdates.json
```
 
 Resultaat: een json met acht geupdate records inclusief enrichment
    
 * Voeg een records toe
 
 JV00001@BRAHMS 
 
```bash
ppdb_nba --current --debug --source brahms-specimen /shared-data/joepitest/enrichment_tests/bellis-before.json
ppdb_nba --debug --source brahms-specimen /shared-data/joepitest/enrichment_tests/bellis-addone.json
```
 
 
 Resultaat: een json met de nieuwe records inclusief enrichment

#### 2. CRS specimens 'fulica*'

De data set bevat:

```json
{
  "conditions": [
    {
      "field": "identifications.scientificName.scientificNameGroup",
      "operator": "STARTS_WITH_IC",
      "value": "fulica"
    },
    {
      "field": "sourceSystem.code",
      "operator": "EQUALS_IC",
      "value": "CRS"
    }
  ],
  "logicalOperator": "AND",
  "sortFields": [
    {
      "path": "unitID",
      "sortOrder": "desc"
    }
  ],
  "from": 0
}
```
    
[Download testset](https://api.biodiversitydata.nl/v2/specimen/download/?_querySpec=%7B%20%20%20%22conditions%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22field%22%3A%20%22identifications.scientificName.scientificNameGroup%22%2C%0A%20%20%20%20%20%20%22operator%22%3A%20%22STARTS_WITH_IC%22%2C%0A%20%20%20%20%20%20%22value%22%3A%20%22fulica%22%0A%20%20%20%20%7D%2C%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22field%22%3A%20%22sourceSystem.code%22%2C%0A%20%20%20%20%20%20%22operator%22%3A%20%22EQUALS_IC%22%2C%0A%20%20%20%20%20%20%22value%22%3A%20%22CRS%22%0A%20%20%20%20%7D%0A%20%20%5D%2C%0A%20%20%22logicalOperator%22%3A%20%22AND%22%2C%0A%20%20%22sortFields%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22path%22%3A%20%22unitID%22%2C%0A%20%20%20%20%20%20%22sortOrder%22%3A%20%22desc%22%0A%20%20%20%20%7D%0A%20%20%5D%2C%0A%20%20%22from%22%3A%200%0A%7D
)

```bash
ppdb_nba --current --debug --source crs-specimen /shared-data/joepitest/enrichment_tests/fulica-before.json
```
    
 * Doe een update van 1 record
 
 Toevoegen: `"sex":"male"`  aan RMNH.ART.2185@CRS

```bash
ppdb_nba --current --debug --source crs-specimen /shared-data/joepitest/enrichment_tests/fulica-before.json
ppdb_nba --debug --source crs-specimen /shared-data/joepitest/enrichment_tests/fulica-1update.json
```
 
 Resultaat: een json van een record inclusief enrichment
 
 * Doe een update van meer records
 
`s/Nederlek/Krimpenerwaard/`
 
 Vier veranderde records:
 
```
RMNH.AVES.162877.a@CRS
RMNH.AVES.162877.b@CRS 
RMNH.AVES.228329@CRS
RMNH.AVES.228330@CRS
```

```bash
ppdb_nba --current --debug --source crs-specimen /shared-data/joepitest/enrichment_tests/fulica-before.json
ppdb_nba --debug --source crs-specimen /shared-data/joepitest/enrichment_tests/fulica-moreupdates.json
```
 
 Resultaat: een json met de geupdate records inclusief enrichment
    
 * Voeg een of meer records toe
 
```bash
ppdb_nba --current --debug --source crs-specimen /shared-data/joepitest/enrichment_tests/fulica-before.json
ppdb_nba --debug --source crs-specimen /shared-data/joepitest/enrichment_tests/fulica-addone.json
```
 
 Resultaat: een json met de nieuwe records inclusief enrichment

#### 3. Col taxa update

 * Update taxon 'fulica atra'.
 
 Resultaat: alle 'fulica atra' records updated inclusief enrichment
 
 * Update taxon 'bellis perennis'.
    
 Resultaat: alle 'bellis perennis' records updated inclusief enrichment

#### 4. NSR taxa update

 * Update taxon 'fulica atra'.
 
 Resultaat: alle 'fulica atra' records updated inclusief enrichment
 
 * Update taxon 'bellis perennis'.
    
 Resultaat: alle 'bellis perennis' records updated inclusief enrichment


