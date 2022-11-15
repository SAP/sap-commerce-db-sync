# SAP Commerce DB Sync - Configuration Guide

## Configuration reference

[Configuration Reference](CONFIGURATION-REFERENCE.md) To get an overview of the configurable properties.

## Configure incremental data migration

For large tables, it often makes sense to copy the bulk of data before the cutover, and then only copy the rows that have changed in a given time frame. This helps to reduce the cutover window for production systems.
To configure the incremental copy, set the following properties:
```
migration.data.incremental.enabled=<true|false>
migration.data.incremental.tables=<list of tables names>
migration.data.incremental.timestamp=<ISO_ZONED_DATE_TIME>
migration.data.truncate.enabled=<true|false>
```
example:
```
migration.data.incremental.enabled=true
migration.data.incremental.tables=orders,orderentries
migration.data.incremental.timestamp=2020-07-28T18:44:00+01:00[Europe/Zurich]
migration.data.truncate.enabled=false
```

> **LIMITATION**: Tables must have the following columns: modifiedTS, PK. Furthermore, this is an incremental approach... only modified and inserted rows are taken into account. Deletions on the source side are not handled.

The timestamp refers to whatever timezone the source database is using (make sure to include the timezone).

During the migration, the data copy process is using an UPSERT command to make sure new records are inserted and modified records are updated. Also make sure to disable truncation as this is not desired for incremental copy.

Only tables configured for incremental will be taken into consideration, as long as they are not already excluded by the general filter properties. All other tables will be ignored.

After the incremental migration you may have to migrate the numberseries table again, to ensure the PK generation will be aligned.
For this, disable incremental mode and use the property migration.data.tables.included to only migrate that one table.

## Configure logging

Use the following property to configure the log level:

log4j2.logger.migrationToolkit.level

Default value is INFO.
