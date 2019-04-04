# Testen met percolator script

Op dit moment is de percolator nog in testfase. Een eigen docker omgeving heeft hij nog
niet. Standaard wordt het percolator project nu gestart in de _jupyter_ omgeving. Deze
staat op machine `145.136.242.91` in de `/opt/ppdb` directory. Om in die omgeving te komen
volstaat het commando:

`cd /opt/ppdb;docker-compose exec jupyter bash`

Voor de test zijn de volgende files en directories in die docker omgeving van belang.

 - `/shared-data/config.yml`

 Deze file is buiten de docker omgeving te vinden in `/data/shared-data/config.yml` en
 bevat de complete configuratie van het percolator script en de diverse bronnen.  Ook 
 staan hierin de paden gedefinieerd.

 - `incoming: /shared-data/percolator/incoming`

 Hierin staan de jsonlines bestanden die nog verwerkt moeten worden.

 - `processed: /shared-data/percolator/archive`

 Bestanden die succesvol zijn ingelezen komen hier in.

 - `jobs: /shared-data/percolator/jobs`

 Hierin staan de job files die periodiek worden gescand.

 - `failed: /shared-data/percolator/failed`

 Hierin komen de jobs files die periodiek worden gescand.

 - `done: /shared-data/infuser/jobs`

 Hierin komen de job files die zijn afgehandeld.

 - `/shared-data/infuser/incoming`

 Hierin komen de files met delta gegevens die door de _infuser_ moeten worden ingelezen.

## Meest recente versie van percolator installeren

Een terugkerend probleem is dat de jupyter docker instance een versie van het percolator
script bevat die verouderd is. Dat komt omdat het script nog in volle ontwikkeling is.
Het is geen overbodige luxe om aan het begin van iedere test en bij iedere wijziging
aan het script via pip de meest recente versie te installeren.

```
cd /opt/ppdb
docker-compose exec jupyter bash
pip install -e git+https://github.com/naturalis/ppdb_nba.git#egg=ppdb_nba
```

## Legen van een database tabel bijvoorbeeld *brahmsspecimen_current*

Tijdens de testfase komt het meer dan eens voor dat de current tabel moet worden geleegd.
De enige manier waarop dit op dit moment gemakkelijk kan is in postgres zelf.

Let goed op dat je de database gebruikt die gespecificeerd staat in `/shared-data/config.yml`.

```
cd /opt/ppdb
docker-compose exec postgres psql -U postgres test20190314
TRUNCATE TABLE public.brahmsspecimen_current;
\q
``

## Inladen in current

Als een dataset binnenkomt die niet incrementeel hoeft te worden geladen dan moet de
data rechtstreeks worden ingelezen in de current tabel. Dit kan nog niet automatisch
en moet dus 'met de hand'. Gebruikelijk is hierbij de volgende procedure te gebruiken:

```
cd /opt/ppdb
docker-compose exec jupyter bash
cd /shared-data
ppdb_nba --debug --current --source brahmsspecimen /shared-data/percolator/incoming/brahms-specimen-bestand.json
```

Goed opletten, als aan dit jsonlines bestand wordt gerefereerd in een job bestand in
`/shared-data/percolator/jobs` dan moet die uit de jobs directory worden verwijderd.
Anders wordt deze alsnog uitgevoerd. Op zich niet erg, maar het kost bij grote bestanden
veel extra tijd.

## Importeren met job

Dit is zoals de percolator normaal gesproken gaat werken. De jobs directory wordt
gescand en het meest oude job bestand met de extensie '.json' wordt als eerste
opgepakt. De procedure:

```
cd /opt/ppdb
docker-compose exec jupyter bash
cd /shared-data
ppdb_nba --debug
```

Als alles goed gaat krijg je te zien welke stappen het proces doorloopt. Als alles
succesvol verloopt dan worden de incrementele files aangemaakt in 
`/shared-data/infuser/incoming`. Er zijn echter ook situaties mogelijk waarbij dingen
fout gaan. Mogelijke problemen:

### De boel loopt vast

Om een of andere reden gaat er iets mis. Als het goed is verschijnt er een python
error die je meer informatie geeft. Meestal is het een configuratieprobleem. Maar
mocht je toch iets in de code willen veranderen dan kan dat in de codebase
van de [ppdb_nba](https://github.com/naturalis/ppdb_nba/).

Let op dat je na wijzigingen wel weer de nieuwste versie van het percolator script
moet installeren (zie eerder in deze instructie).

### Er is nog een .lock file aanwezig in /shared-data/jobs/.lock

Deze blijft staan als een vorige run van ppdb_nba faalt of nog bezig is. Als de
percolator niet meer draait wordt de job file naar failed verplaatst. De corresponderende
data bestanden blijven staan in incoming (als ze nog niet zijn ingelezen).

## Kopiëren naar Infuser

Alles is gelukt? Alleen nu staan de incrementele bestanden nog niet op de goeie plek.
Via de jupyter docker instance kunnen we (nog) niet bij de minio/s3 directory waar
Tom kijkt. Via de machine waarop de percolator wordt gehost kan dit gelukkig wel.
De laatste stap is dan:

```
mv /data/shared-data/infuser/incoming/* /data/shared-data/incremental/
```

Daarna moet Tom gevraagd worden om zijn import stap uit te voeren.


