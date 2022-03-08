# Airport Flights Analysis 


Project contains classes and methods for:
- Downloading the data from external source via HTTP
- Generating datasets for analysis
- SQL scripts for report generation
- Logging

Project Architecture:
- ./app/src/AuthClass get authorization credentials
- ./app/src/Connector handles database usage
- ./app/src/DataExtractor the data from external source
- ./app/src/DataTransformer clears received data
- ./app/src/ScheduleGenerator generates flights schedule
- ./app/src/FlightsGenerator generates flights as it has happened in real world
- ./app/src/ReportGenerator handles SQL scripts execution and forming CSV reports
- ./app/sql contains SQL scripts
- ./migrations SQL scripts for initiating DB and replicating additional dictionaries 
- ./pg_data contains CSV file with airport coordinates (from external source)

Application Architecture:
- Implemented classic ETL pipeline
- All data from external source stored AS-IS in staging. 
Data is everything, that is why we need to keep received data in form it came to us 
- Received data has corrupted entities, to prepare it for replication to DB and analysis separate class implemented
- Received data has gaps in its nature (..."long": XX.XXXX, "lat": "Kosovo"...). I downloaded dictionary of all airports from external source and pre-downloaded it into DB to fix it
- After Transform step we generate datasets based on test cse conditions and load it to DB

Used technologies:
- GIT 2.31.1
- Docker v20.10.12
- Docker Compose v1.27.1
- PostgreSQL v13.2
- Python v3.10.2
- Pycharm 2022
- DBeaver 21.0.1
- External py-packages: psycopg2, geopy, dateutil, requests

Work Station:
- Intel(R) Core(TM) i5-3570K CPU @ 3.40GHz
- 8GB RAM
- Fedora 33 (Workstation Edition)


Instruction:
1. Clone repo, change working directory to the project 
2. Run command in teminal/PowerShell `docker-compose up --build -d`
3. After daemon started, run command `docker exec -it alar_container /bin/bash`