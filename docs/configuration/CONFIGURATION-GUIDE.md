# SAP Commerce DB Sync - Configuration Guide

## Configuration reference

[Configuration Reference](CONFIGURATION-REFERENCE.md) To get an overview of the configurable properties.

## Configure incremental data migration

For large tables, it often makes sense to copy the bulk of data before the cutover, and then only copy the rows that have changed in a given time frame. This helps to reduce the cutover window for production systems.
To configure the incremental copy, set the following properties:
```properties
migration.data.incremental.enabled=<true|false>
migration.data.incremental.tables=<list of tables names>
migration.data.incremental.timestamp=<ISO_ZONED_DATE_TIME>
migration.data.truncate.enabled=<true|false>
```
example:
```properties
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

`log4j2.logger.migrationToolkit.level`

Default value is `INFO`.

## Dynamic View Generation

It is possible to automatically generate a view for a table without explicitly designing DDL `VIEW` definition.
Using dynamic configuration definition in your `*.properties` Commerce configuration it is possible to generate a view.

View generation is optional. By default, it is not enabled. To generate that for the particular table it is required to create the property:

```properties
migration.data.view.t.{table}.enabled=true

# for example
migration.data.view.t.medias.enabled=true
```

That property enables the system will generate the default structure of the view

```sql
CREATE OR ALTER VIEW v_medias -- view name is generated based on other property `migration.db.view.name.pattern`
AS
SELECT * FROM medias
```

### Customize Source of Data/Table

By default, migration aims to transfer data one-to-one table by table, without transforming or fetching data from other tables.
Although sometimes it is required to filter out data as we did in the example above. That is also possible with configuration.

For example with media filtered just for the images folder is possible to achieve by below 3 properties:

```properties
migration.data.view.t.medias.enabled=true # enable view generation
# If you are joining more than one tables in the where clause then use a columnPrefix label
migration.data.view.t.medias.columnPrefix=item_t1
# The joinWhereClause is used within view definition. i.e. CREATE VIEW v_media AS SELECT * FROM {joinWhereClause} 
# name `v_medias` is generated due to default name pattern value v_%s as the view name so no need to configure it for joinWhereClause
migration.data.view.t.medias.joinWhereClause=medias item_t1 JOIN mediafolders item_t2 ON item_t1.p_folder = item_t2.PK WHERE (item_t2.p_qualifier like 'images')
```

Output for that will be like this:

```sql
CREATE OR ALTER VIEW v_medias
AS
SELECT
    item_t1.hjmpTS,
    item_t1.createdTS,
    item_t1.modifiedTS,
    item_t1.TypePkString,
    item_t1.OwnerPkString,
    item_t1.PK,
    item_t1.sealed,
    item_t1.p_mime,
    item_t1.p_size,
    item_t1.p_datapk,
    item_t1.p_location,
    item_t1.p_locationhash,
    item_t1.p_realfilename,
    item_t1.p_code,
    item_t1.p_internalurl,
    item_t1.p_description,
    item_t1.p_alttext,
    item_t1.p_removable,
    item_t1.p_mediaformat,
    item_t1.p_folder,
    item_t1.p_subfolderpath,
    item_t1.p_mediacontainer,
    item_t1.p_catalog,
    item_t1.p_catalogversion,
    item_t1.aCLTS,
    item_t1.propTS,
    item_t1.p_outputmimetype,
    item_t1.p_inputmimetype,
    item_t1.p_itemtimestamp,
    item_t1.p_format,
    item_t1.p_sourceitem,
    item_t1.p_fieldseparator,
    item_t1.p_quotecharacter,
    item_t1.p_commentcharacter,
    item_t1.p_encoding,
    item_t1.p_linestoskip,
    item_t1.p_removeonsuccess,
    item_t1.p_zipentry,
    item_t1.p_extractionid,
    item_t1.p_auditrootitem,
    item_t1.p_auditreportconfig,
    item_t1.p_scheduledcount,
    item_t1.p_cronjobpos,
    item_t1.p_cronjob
FROM medias JOIN mediafolders item_t1 ON p_folder = item_t1.PK WHERE (item_t1.p_qualifier like 'images')
```

With the above example, we were able to extract 1:1 data from the table limiting the number of rows to media, which are related to folder with id `images`.

### Customize Columns in View

Additional functionality introduced above is the possibility to use custom functions to e.g. obfuscate values for columns, but that function can be anything, e.g. date `GETDATE()`.

For that it is possible to use an additional parameter, which is column customisation:

```properties
migration.data.view.t.{table}.columnTransformation.{column}=<value>

# e.g. replace for the table: medias, column: p_datapk, by: mask_email(p_datapk)
migration.data.view.t.medias.columnTransformation.p_datapk=mask_email(p_datapk)

# replace for table: medias, column: p_location, by: mask_custom(1, 'xxxxx', 2, p_location)
migration.data.view.t.medias.columnTransformation.p_location=mask_custom(1, 'xxxxx', 2, p_location)
```

With only these values, the view will look like the one below:

```sql
CREATE OR ALTER VIEW v_medias
AS
SELECT
    hjmpTS,
    createdTS,
    modifiedTS,
    TypePkString,
    OwnerPkString,
    PK,
    sealed,
    p_mime,
    p_size,
    mask_email(p_datapk) as p_datapk,
    mask_custom(1, 'xxxxx', 2, p_location) as p_location,
    p_locationhash,
    p_realfilename,
    p_code,
    p_internalurl,
    p_description,
    p_alttext,
    p_removable,
    p_mediaformat,
    p_folder,
    p_subfolderpath,
    p_mediacontainer,
    p_catalog,
    p_catalogversion,
    aCLTS,
    propTS,
    p_outputmimetype,
    p_inputmimetype,
    p_itemtimestamp,
    p_format,
    p_sourceitem,
    p_fieldseparator,
    p_quotecharacter,
    p_commentcharacter,
    p_encoding,
    p_linestoskip,
    p_removeonsuccess,
    p_zipentry,
    p_extractionid,
    p_auditrootitem,
    p_auditreportconfig,
    p_scheduledcount,
    p_cronjobpos,
    p_cronjob
FROM medias
```

## Gather Type Information

There might be cases where type PKs are needed to filter out certain records for migration. For that you can use TypeInfo collector

```properties
# PK of Customer type will be fetched from active typesystem and will be inserted into MIGRATIONTOOLKIT_TF_TYPEINFO table
migration.data.t.typeinfo.Customer.enabled=true

# Then you can use this information for filtering certain records, for example exclude all Customers from users table except anonymous user
migration.data.view.t.users.enabled=true 
migration.data.view.t.users.columnPrefix=item_t1
migration.data.view.t.users.joinWhereClause=users item_t1 LEFT JOIN MIGRATIONTOOLKIT_TF_TYPEINFO item_t2 ON item_t1.typepkstring = item_t2.pk WHERE item_t2.internalcode != 'Customer' OR item_t2.internalcode IS NULL OR item_t1.p_uid = 'anonymous'
```

## Data Anonymization 

There might be cases where you need the data to be anonymized in the target system.
Please reference [Anonymization Configuration](../anonymizer/README.md)