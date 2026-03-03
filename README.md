# Using DB2 as a source with CDC via Change Tracking (aka DB2 SQL Replication) to import data into MongoDB

## Run
You can also simply run Dsync Enterprise with Docker using the image built from https://github.com/adiom-data/dsynct/tree/db2:
```
export MONGODB_URI="..."

docker run --rm --platform=linux/amd64 -p 8080:8080 -v $(pwd)/dsync-transform.yml:/dsync-transform.yml  -v $(pwd)/dsync_db2.yaml:/dsync_db2.yaml -e DSYNCT_MODE=simple alexadiom/dsynct-db2 --host-port 0.0.0.0:8080 sync --namespace "Accounts, Policies" --namespace-mapping "Accounts:db2.accounts, Policies:db2.policies" --transform sqlbatch --config dsync_db2.yaml $MONGODB_URI dsync-transform://dsync-transform.yml
```

This will take Accounts and Policies tables from DB2 and sync them to MongoDB into collections "accounts" and "policies" in the "db2" database. The transformer is an identity transformer that just renames the ID field to _id.

## Prepare DB2
### 1. DB2 Container

Use debezium docker image as it has all CDC tables initialized and cool stored procedures to make working with change tracking easier: 
```
docker run -itd --name mydb2 --privileged=true -p 50000:50000 -e LICENSE=accept -e DB2INST1_PASSWORD=<choose an instance password> -e DBNAME=testdb -v <db storage dir>:/database devingbost/debezium-db2:0.0.1
```
OR
```
docker run -d \
  --name mydb2_dbz \
  --hostname 0e5243ac9fec \
  -it \
  -p 50000:50000 \
  -p 55000:55000 \
  -p 60006:60006 \
  -p 60007:60007 \
  -p 2222:22 \
  -e LICENSE=accept \
  -e DB2INST1_PASSWORD=mypassword \
  -e DBNAME=testdb \
  -e STORAGE_DIR=/database \
  -e HADR_SHARED_DIR=/hadr \
  -e DBPORT=50000 \
  -e TSPORT=55000 \
  -e SETUPDIR=/var/db2_setup \
  -e "NOTVISIBLE=in users profile" \
  -e LICENSE_NAME=db2dec.lic \
  -v db2_database:/database \
  -v db2_hadr:/hadr \
  devingbost/debezium-db2:0.0.1
```
For details, see https://github.com/debezium/debezium-connector-db2/tree/main/src/test/docker/db2-cdc-docker

### 2. Initialize the database (and optionally run the load generation later)

To initialize the database first:

```
python3 loader.py --init-db
```

To run the loader yourself:

```
python3 loader.py --parallel <threads> --duration <seconds> --ins <ratio> --upd <ratio> --delete <ratio>
```

Example: 

```
python3 loader.py --parallel 4 --duration 30 --ins 60 --upd 30 --delete 10
```
### 3. Enable CDC
Connect to DB2 using DBeaver (username DB2INST1) and enable capture - start the capture service, add tables to capture if needed, and if you added new tables, reinit the capture service
```
VALUES ASNCDC.ASNCDCSERVICES('start','asncdc'); // run on startup (note that it may not always start successfully)
VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');
CALL ASNCDC.ADDTABLE('DB2INST1', 'ACCOUNTS'); // Add table to the capture
CALL ASNCDC.ADDTABLE('DB2INST1', 'AGENTS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'CUSTOMERS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'CLAIMS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'TRANSACTIONS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'POLICIES');
VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc'); 
```

## Helper SQL scripts:
```
SELECT 
    SOURCE_OWNER, 
    SOURCE_TABLE, 
    HEX(SYNCHPOINT) AS CAPTURE_PROGRESS_LSN, 
    SYNCHTIME AS LAST_LOG_TIME
FROM 
    ASNCDC.IBMSNAP_REGISTER
WHERE 
    SOURCE_OWNER = 'DB2INST1' 
    AND SOURCE_TABLE = 'ACCOUNTS';

SELECT max(t.SYNCHPOINT) FROM ( SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER UNION ALL SELECT SYNCHPOINT AS SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER) t

SELECT 
    CASE 
        WHEN IBMSNAP_OPERATION = 'D' AND LEAD(IBMSNAP_OPERATION, 1, 'X') OVER (PARTITION BY IBMSNAP_COMMITSEQ ORDER BY IBMSNAP_INTENTSEQ) = 'I' THEN 3
        WHEN IBMSNAP_OPERATION = 'I' AND LAG(IBMSNAP_OPERATION, 1, 'X') OVER (PARTITION BY IBMSNAP_COMMITSEQ ORDER BY IBMSNAP_INTENTSEQ) = 'D' THEN 4
        WHEN IBMSNAP_OPERATION = 'D' THEN 1
        WHEN IBMSNAP_OPERATION = 'I' THEN 2
        WHEN IBMSNAP_OPERATION = 'U' THEN 4 -- Handled as an After-Image Update
        ELSE 0 -- Unknown/Error state
    END AS OPCODE,
    cdc.*
FROM ASNCDC.CDC_DB2INST1_ACCOUNTS cdc
-- WHERE IBMSNAP_COMMITSEQ >= ? AND IBMSNAP_COMMITSEQ <= ?
ORDER BY IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ;

SELECT iu.*
FROM ASNCDC.IBMSNAP_UOW iu;
```


How to start the Capture process manually from the container (if it fails, you might need that for a cold start):
```
[root@0e5243ac9fec /]# su - db2inst1
Last login: Wed Jan 28 17:08:46 UTC 2026
[db2inst1@0e5243ac9fec ~]$ . /database/config/db2inst1/sqllib/db2profile
[db2inst1@0e5243ac9fec ~]$  /database/config/db2inst1/sqllib/bin/asncap capture_schema=asncdc capture_server=TESTDB startmode=cold
```

Re-adding tables
```
VALUES ASNCDC.ASNCDCSERVICES('stop','asncdc');

CALL ASNCDC.REMOVETABLE('DB2INST1', 'ACCOUNTS');
CALL ASNCDC.REMOVETABLE('DB2INST1', 'AGENTS');
CALL ASNCDC.REMOVETABLE('DB2INST1', 'CUSTOMERS');
CALL ASNCDC.REMOVETABLE('DB2INST1', 'CLAIMS');
CALL ASNCDC.REMOVETABLE('DB2INST1', 'POLICIES');
CALL ASNCDC.REMOVETABLE('DB2INST1', 'TRANSACTIONS');

CALL ASNCDC.ADDTABLE('DB2INST1', 'CLAIMS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'TRANSACTIONS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'POLICIES');
CALL ASNCDC.ADDTABLE('DB2INST1', 'ACCOUNTS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'AGENTS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'CUSTOMERS');

VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');
VALUES ASNCDC.ASNCDCSERVICES('start','asncdc');
```
