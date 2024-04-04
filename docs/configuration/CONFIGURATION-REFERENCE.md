
# SAP Commerce DB Sync - Configuration Reference


| Property | Description | Default   | values      | optional    | dependency   |
| --- | --- | ---   | ---      | ---    | ---   |
| migration.cluster.enabled | Run migration in the cluster (based on commerce cluster config). The 'HAC' node will be the primary one.  A scheduling algorithm decides which table will run on which node. Nodes are notified using cluster events.|  `false`    | true or false      | true    |    |
| migration.data.columns.excluded.attributedescriptors | Specifies the columns to be excluded|    | migration.data.columns.excluded.[tablename]=[comma separated list of column names]      | true    |    |
| migration.data.columns.nullify.attributedescriptors | Specifies the columns to be nullified. Whatever value there was will be replaced with NULL in the target column.|    | migration.data.columns.nullify.[tablename]=[comma separated list of column names]      | true    |    |
| migration.data.export.enabled | Activate data export to external DB via cron jobs|  `false`    | true or false      | true    |    |
| migration.data.failonerror.enabled | If set to true, the migration will abort as soon as an error occured.  If set to false, the migration will try to continue if the state of the runtime allows.|  `true`    | true or false      | true    |    |
| migration.data.filestorage.container.name | Specifies the name of the container where the tool will store the files related to migration in the blob storage pointed by the property {migration.data.report.connectionstring}|  `migration`    | any string      | migration    |    |
| migration.data.fulldatabase.enabled | |  `true`    |       |     |    |
| migration.data.incremental.enabled | If set to true, the migration will run in incremental mode. Only rows that were modified after a given timestamp  will be taken into account.|  `false`    | true or false      | true    |    |
| migration.data.incremental.tables | Only these tables will be taken into account for incremental migration.|    | comma separated list of tables.      | true    | migration.data.incremental.enabled   |
| migration.data.incremental.timestamp | Records created or modified after this timestamp will be copied only.|    | The timestamp in ISO-8601 ISO_ZONED_DATE_TIME format      | true    | migration.data.incremental.enabled   |
| migration.data.indices.disable.enabled | If set to true, all indices in the target table will be disabled (NOT removed) before copying over the data.  After the data copy the indices will be enabled and rebuilt again.|  `false`    | true of false      | true    |    |
| migration.data.indices.disable.included | If disabling of indices is enabled, this property specifies the tables that should be included.  If no tables specified, indices for all tables will be disabled.|    | comma separated list of tables      | true    | migration.data.indices.disable.enabled   |
| migration.data.indices.drop.enabled | If set to true, all indices in the target table will be removed before copying over the data.|  `false`    | true of false      | true    |    |
| migration.data.indices.drop.recreate.exclude | do not recreate following indices after the migration. Comma separated values|    | comma separated values      | true    |    |
| migration.data.maxparalleltablecopy | Specifies the number of tables that are copied over in parallel.|  `2`    | integer value      | true    |    |
| migration.data.pipe.capacity | Specifies the capacity of the data pipe.|  `100`    | integer value      | true    |    |
| migration.data.pipe.timeout | Specifies the timeout of the data pipe.|  `7200`    | integer value      | true    |    |
| migration.data.reader.batchsize | Specifies the batch size for reading data from source. |  `1000`    |   integer value    |  true   |    |
| migration.data.reader.batchsize.{table} | Table individual batch size for reading data from source enabling tuning on read speed vs. memory usage. Replace the {table} with the source table name without prefix. |     |   integer value    |  true   |    |
| migration.data.report.connectionstring | Specifies blob storage connection string for storing reporting files.|  `${media.globalSettings.cloudAzureBlobStorageStrategy.connection}`    | any azure blob storage connection string      | true    |    |
| migration.data.tables.audit.enabled | Flag to enable the migration of audit tables.|  `true`    | true or false      | true    |    |
| migration.data.tables.custom | Specifies a list of custom tables to migrate. Custom tables are tables that are not part of the commerce type system.|    | comma separated list of table names.      | true    |    |
| migration.data.tables.excluded | Tables to exclude from migration (use table names name without prefix)|  `SYSTEMINIT,StoredHttpSessions,itemdeletionmarkers`    | comma separated list of table names.      | true    |    |
| migration.data.tables.included | Tables to include (use table names name without prefix)|    | comma separated list of table names.      | true    |    |
| migration.data.truncate.enabled | Specifies if the target tables should be truncated before data is copied over.|  `true`    | true or false      | true    |    |
| migration.data.truncate.excluded | If truncation of target tables is enabled, this property specifies tables that should be excluded from truncation.|    | comma separated list of table names      | true    | migration.data.truncate.enabled   |
| migration.data.view.name.pattern | Support views during data migration. String pattern for view naming convention with `'%s'` as table name. e.g. `v_%s`|  `v_%s`    | any string      | true    |    |
| migration.data.view.t.TABLE.columnTransformation.COLUMN | Possibility to use custom functions to obfuscate values for specific columns|  `GETDATE()`    | any valid SQL function call      | true    | migration.data.view.t.TABLE.enabled   |
| migration.data.view.t.TABLE.enabled | Activate DDL view generation for specific|  `false`    | any string      | true    |    |
| migration.data.view.t.TABLE.joinWhereClause | Activate DDL view generation for specific _TABLE_, with additional `JOIN` clausule|  `{table}`    | any string      | true    | migration.data.view.t.TABLE.enabled   |
| migration.data.workers.reader.maxtasks | Specifies the number of threads used per table to read data from source.  Note that this value applies per table, so in total the number of threads will depend on  'migration.data.maxparalleltablecopy'.  [total number of reader threads] = [migration.data.workers.reader.maxtasks] * [migration.data.maxparalleltablecopy]|  `3`    | integer value      | true    | migration.data.maxparalleltablecopy   |
| migration.data.workers.retryattempts | Specifies the number of retries in case a worker task fails.|  `0`    | integer value      | true    |    |
| migration.data.workers.writer.maxtasks | Specifies the number of threads used per table to write data to target.  Note that this value applies per table, so in total the number of threads will depend on  'migration.data.maxparalleltablecopy'.  [total number of writer threads] = [migration.data.workers.writer.maxtasks] * [migration.data.maxparalleltablecopy]|  `10`    | integer value      | true    | migration.data.maxparalleltablecopy   |
| migration.ds.source.db.connection.pool.size.active.max | Specifies maximum amount of active connections in the source db pool|  `${db.pool.maxActive}`    | integer value      | false    |    |
| migration.ds.source.db.connection.pool.size.idle.max | Specifies maximum amount of connections in the source db pool|  `${db.pool.maxIdle}`    | integer value      | false    |    |
| migration.ds.source.db.connection.pool.size.idle.min | Specifies minimum amount of idle connections available in the source db pool|  `${db.pool.minIdle}`    | integer value      | false    |    |
| migration.ds.source.db.driver | Specifies the driver class for the source jdbc connection|    | any valid jdbc driver class      | false    |    |
| migration.ds.source.db.password | Specifies the password for the source jdbc connection|    | any valid password for the jdbc connection      | false    |    |
| migration.ds.source.db.schema | Specifies the schema the respective commerce installation is deployed to.|    | any valid schema name for the commerce installation      | false    |    |
| migration.ds.source.db.tableprefix | Specifies the table prefix used on the source commerce database.  This may be relevant if a commerce installation was initialized using 'db.tableprefix'.|    | any valid commerce database table prefix.      | true    |    |
| migration.ds.source.db.typesystemname | Specifies the name of the type system that should be taken into account|  `DEFAULT`    | any valid type system name      | true    |    |
| migration.ds.source.db.typesystemsuffix | Specifies the suffix which is used for the source typesystem|    | the suffix used for typesystem. I.e, 'attributedescriptors1' means the suffix is '1'      | true    | migration.ds.source.db.typesystemname   |
| migration.ds.source.db.url | Specifies the url for the source jdbc connection|    | any valid jdbc url      | false    |    |
| migration.ds.source.db.username | Specifies the user name for the source jdbc connection|    | any valid user name for the jdbc connection      | false    |    |
| migration.ds.target.db.catalog | |    |       |     |    |
| migration.ds.target.db.connection.pool.size.active.max | Specifies maximum amount of connections in the target db pool|  `${db.pool.maxActive}`    | integer value      | false    |    |
| migration.ds.target.db.connection.pool.size.idle.max | Specifies maximum amount of idle connections available in the target db pool|  `${db.pool.maxIdle}`    | integer value      | false    |    |
| migration.ds.target.db.connection.pool.size.idle.min | Specifies minimum amount of idle connections available in the target db pool|  `${db.pool.minIdle}`    | integer value      | false    |    |
| migration.ds.target.db.driver | Specifies the driver class for the target jdbc connection|  `${db.driver}`    | any valid jdbc driver class      | false    |    |
| migration.ds.target.db.max.stage.migrations | When using the staged approach, multiple sets of commerce tables may exists (each having its own tableprefix).  To prevent cluttering the db, this property specifies the maximum number of table sets that can exist,  if exceeded the schema migrator will complain and suggest a cleanup.|  `5`    | integer value      | true    |    |
| migration.ds.target.db.password | Specifies the password for the target jdbc connection|  `${db.password}`    | any valid password for the jdbc connection      | false    |    |
| migration.ds.target.db.schema | Specifies the schema the target commerce installation is deployed to.|  `dbo`    | any valid schema name for the commerce installation      | false    |    |
| migration.ds.target.db.tableprefix | Specifies the table prefix used on the target commerce database.  This may be relevant if a commerce installation was initialized using `${db.tableprefix}` / staged approach.|  `${db.tableprefix}`    | any valid commerce database table prefix.      | true    |    |
| migration.ds.target.db.typesystemname | Specifies the name of the type system that should be taken into account|  `DEFAULT`    | any valid type system name      | true    |    |
| migration.ds.target.db.typesystemsuffix | Specifies the suffix which is used for the target typesystem|    | the suffix used for typesystem. I.e, 'attributedescriptors1' means the suffix is '1'      | true    | migration.ds.source.db.typesystemname   |
| migration.ds.target.db.url | Specifies the url for the target jdbc connection|  `${db.url}`    | any valid jdbc url      | false    |    |
| migration.ds.target.db.username | Specifies the user name for the target jdbc connection|  `${db.username}`    | any valid user name for the jdbc connection      | false    |    |
| migration.input.profiles | Specifies the profile name of data source that serves as migration input|  `source`    | name of the data source profile      | true    |    |
| migration.locale.default | Specifies the default locale used.|  `en-US`    | any locale      | true    |    |
| migration.log.sql | If set to true, the JDBC queries ran against the source and target data sources will be logged in the storage pointed by the property {migration.data.report.connectionstring}|  `false`    | true or false      | false    |    |
| migration.log.sql.memory.flush.threshold.nbentries | Specifies the number of log entries to add to the in-memory collection of JDBC log entries of a JDBC queries store before flushing the collection contents into the blob file storage associated with the JDBC store's data souce and clearing the in-memory collection to free memory|  `10000000`    | an integer number      | 10,000,000    |    |
| migration.log.sql.source.showparameters | If set to true, the values of the parameters of the JDBC queries ran against the source data source will be logged in the JDBC queries logs (migration.log.sql has to be true to enable this type of logging). For security reasons, the tool will never log parameter values for the queries ran against the target datasource.|  `true`    | true or false      | true    |    |
| migration.output.profiles | Specifies the profile name of data sources that serves as migration output|  `target`    | name of the data source profile      | true    |    |
| migration.properties.masked | Specifies the properties that should be masked in HAC.|  `migration.data.report.connectionstring,migration.ds.source.db.password,migration.ds.target.db.password`    | any property key      | true    |    |
| migration.scheduler.resume.enabled | If set to true, the migration will resume from where it stopped (either due to errors or cancellation).|  `false`    | true or false      | true    |    |
| migration.schema.autotrigger.enabled | Specifies if the schema migrator should be automatically triggered before data copy process is started|  `false`    | true or false      | true    | migration.schema.enabled   |
| migration.schema.enabled | Globally enables / disables schema migration. If set to false, no schema changes will be applied.|  `true`    | true or false      | true    |    |
| migration.schema.target.columns.add.enabled | Specifies if columns which are missing in the target tables should be added by schema migration.|  `true`    | true or false      | true    | migration.schema.enabled   |
| migration.schema.target.columns.remove.enabled | Specifies if extra columns in target tables (compared to source schema) should be removed by schema migration.|  `true`    | true or false      | true    | migration.schema.enabled   |
| migration.schema.target.tables.add.enabled | Specifies if tables which are missing in the target should be added by schema migration.|  `true`    | true or false      | true    | migration.schema.enabled   |
| migration.schema.target.tables.remove.enabled | Specifies if extra tables in target (compared to source schema) should be removed by schema migration.|  `false`    | true or false      | true    | migration.schema.enabled   |
| migration.stalled.timeout | Specifies the timeout of the migration monitor.  If there was no activity for too long the migration will be marked as 'stalled' and aborted.|  `7200`    | integer value      | true    |    |
| migration.trigger.updatesystem | Specifies whether the data migration shall be triggered by the 'update running system' operation.|  `false`    | true or false      | true    |    |
| migration.profiling | If set to true, logs the memory usage of each Batch | false |  true or false | true  | |
| migration.memory.min | If free memory is below this threshold, the Reader Task will wait | 5000000 | integer value | true  | |
| migration.memory.attempts | If not enough free memory is available, the reader task will wait the specified amount of attempts. | 300 | integer value | true | migration.memory.min  |
| migration.memory.wait | Amount of milliseconds to wait for free memory | 2000 | integer value | true | migration.memory.min  |