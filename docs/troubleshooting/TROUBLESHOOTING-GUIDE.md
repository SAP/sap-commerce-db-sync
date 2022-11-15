# SAP Commerce DB Sync - Troubleshooting Guide

## Duplicate values for indexes

Symptom:

Pipeline aborts during copy process with message like:
```
FAILED! Reason: The CREATE UNIQUE INDEX statement terminated because a duplicate key was found for the object name 'dbo.cmtmedias' and the index name 'cmtcodeVersionIDX_30'. The duplicate key value is (DefaultCronJobFinishNotificationTemplate_de, <NULL>).
```

Solution:

This can happen if you are using a case sensitive collation on the source database either at database level or table/column level.
The commerce cloud target database is case insensitive by default and will treat values like 'ABC'/'abc' as equal during index creation.
If possible, remove the duplicate rows before any migration activities. In case this is not possible consult Support.

> **Note**: Mysql doesn't take into account NULL values for index checks. SQL Server does and will thus fail with duplicates.

## Migration fails for unknown reason

Symptom:

If you were overloading the system for a longer period of time, you may encounted one of the nodes was restarting in the background without notice.


Solution:

In any case, check the logs (Kibana).
Check in dynatrace whether a process crash log exists for the node.
In case the process crashed, throttle the performance by changing the respective properties.


## MySQL: xy table does not exist error

Symptom:

`java.sql.SQLSyntaxErrorException: Table '<schema.table>' doesn't exist`
even though the table should exist.

Solution:

This is a changed behaviour in the driver 8x vs 5x used before. In case there are multiple catalogs in the database, the driver distorts the reading of the table information...

... add the url parameter 

`nullCatalogMeansCurrent=true`

... to your JDBC connection URL and the error should disappear.

## MySQL: java.sql.SQLException: HOUR_OF_DAY ...

Symptom:


```
Caused by: java.sql.SQLException: HOUR_OF_DAY: 2 -> 3
at com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:129) ~[mysql-connector-java-8.0.19.jar:8.0.19]
at com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:97) ~[mysql-connector-java-8.0.19.jar:8.0.19]
at com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:89) ~[mysql-connector-java-8.0.19.jar:8.0.19]
at com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:63) ~[mysql-connector-java-8.0.19.jar:8.0.19]
at com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:73) ~[mysql-connector-java-8.0.19.jar:8.0.19]
at com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping.translateException(SQLExceptionsMapping.java:85) ~[mysql-connector-java-8.0.19.jar:8.0.19]
at com.mysql.cj.jdbc.result.ResultSetImpl.getTimestamp(ResultSetImpl.java:903) ~[mysql-connector-java-8.0.19.jar:8.0.19]
at com.mysql.cj.jdbc.result.ResultSetImpl.getObject(ResultSetImpl.java:1243) ~[mysql-connector-java-8.0.19.jar:8.0.19]
```

Solution:

Known issue on MySQL when dealing with time/date objects. Workaround is to add...

`&useTimezone=true&serverTimezone=UTC`

...to your source connection string.


## Backoffice does not load

Symptom: 

Backoffice does not load properly after the migration.

Solution:

- use F4 mode (admin user) and reset the backoffice settings on the fly.
- browser cache reload

## Proxy error in Hac

Symptom: 

Hac throws / displays proxy errors when using migration features.

Solution:

Change the default proxy value in the Commerce Cloud Portal to a higher value.
This can be done on the edit view of the respective endpoint.

## MSSQL: Boolean type

The boolean type in MSSQL is a bit data type storing 0/1 values.
In case you were using queries including TRUE/FALSE values, you may have to change or convert the queries in your code to use the bit values. 

## Sudden increase of memory

Symptom:

The memory consumption is more or less stable throughout the copy process, but then suddenly increases for certain table(s).

Solution:

If batching of reading and writing is not possible due to the definition of the source table, the copy process falls back to a non-batched mechanism.
This requires loading the full table in memory at once which, depending on the table size, may lead to unhealthy memory consumption.
For small tables this is typically not an issue, but for large tables it should be mitigated by looking at the indexes for example.

## Some tables are copied over very slowly

Symptom:

While some tables are running smoothly, others seem to suffer from low throughput.
This may happen for the props table for example.

Solution:

The copy process tries to apply batching for reading and writing where possible.
For this, the source table is scanned for either a 'PK' column (normal Commerce table) or an 'ID' column (audit tables).
Some tables don't have such a column, like the props table. In this case the copy process tries to identify the smallest unique (compound) index and uses it for batching.
If a table is slow, check the following:
- ID or PK column exist?
- ID or PK column are unique indexes?
- Any other unique index exists?

If the smallest compound unique index consists of too many columns, the reading may impose high processing load on the source database due to the sort buffer running full.
Depending on the source database, you may have to tweak some db settings to efficiently process the query.
Alternatively you may have to think about adding a custom unique index manually.
