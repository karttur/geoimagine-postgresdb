# geoimagine-postgresdb

Karttur Geoimagine PostgreSQL python project

## Introduction

Karttur's GeoImagine Framework is an attempt for semi-automated processing of spatial data. 
The postgresdb package contains the processing for handling all database processes.

The postgresdb package contains specific modules for each of the schema's in the PostgreSQL database that is part 
of Karttur's GeoImagine Framework. In the default setup of the Framework, each schema is linked to a specific 
database user with rights for that particular schema. For more information on how to setup the Integrated Development 
Environment (IDE) see ???.

The following modules are included in the postgresdb package:

- \_\_init.py\_\_
- ancillary.py
- compositions.py
- fileformats.py
- compositions.py
- landsat.py
- layout.py
- modis.py
- processes.py
- region.py
- selectuser.py
- sentinel.py
- session.py
- smap.py
- soilmoisture.py
- version.py

### ancillary.py

#### GeoImagine Dependencies

geoimagine.support.karttur_dt

#### Classes

- ManageAncillary(PGsession) 

### compositions.py

#### GeoImagine Dependencies

geoimagine.support.karttur_dt

#### Classes

None

### fileformats.py

#### GeoImagine Dependencies

None

#### Classes

None 

### landsat.py

#### GeoImagine Dependencies

- geoimagine.support.karttur_dt

#### Classes

- ManageLandsat(PGsession) 

### layout.py

#### GeoImagine Dependencies

- geoimagine.support.karttur_dt

#### Classes

- SelectLayout(PGsession)
- ManageLayout(PGsession)

### modis.py

#### GeoImagine Dependencies

- geoimagine.support.karttur_dt

#### Classes

- ManageModis(PGsession) 

### processes.py

#### GeoImagine Dependencies

None

#### Classes

- SelectProcess(PGsession)
- ManageProcess(PGsession) 

### region.py

#### GeoImagine Dependencies

- geoimagine.support.karttur_dt

#### Classes

- ManageRegion(PGsession)

### selectuser.py

#### GeoImagine Dependencies

None

#### Classes

- SelectUser(PGsession)

### sentinel.py

#### GeoImagine Dependencies

- geoimagine.support.karttur_dt

#### Classes

- ManageSentinel(PGsession)
