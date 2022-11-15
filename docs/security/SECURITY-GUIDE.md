# SAP Commerce DB Sync - Security Guide

Before you proceed, please make sure you acknowledge the security recommendations below:

## VPN access to the source database is mandatory
  * The data transfer over a non-authenticated JDBC channel can lead to illegitimate access and undesired data leak or manipulation.
  * Therefore, access to the database through a VPN is mandatory to block unauthorised access.
  * To setup a VPN connection, use the VPN self-service functionality provided by SAP Commerce Cloud Portal.

## Transmission of data over non-encrypted channel

  * It is mandatory to enforce TLS on the source DB server.
  * It is mandatory to enforce the usage of TLS v1.2 or v1.3, and to support only strong cipher suites.

## Accounts and Credentials

  * Use a dedicated read only database user for the data migration on the source database. Don't forget to remove this user once the migration activities are finished.
  * Use a dedicated HAC account during the migration. Create the account on both the source and target system. Remove the account once the migration activities are finished.
  * 'Users' table will be overwritten during the migration. Reset admin users's passwords after the migration.

## System Availability

  * The data migration increases the load on the source infrastructure (database), therefore it is mandatory to stop the applications on the source environment.
  * This is especially the case if you run multiple migrations in parallel. For that reason, be sure to avoid multiple migrations concurrently.
  * When using the staged approach, you could end up with many staged copies in the target database, which can impact the availability of the target database. Therefore the number of staged copies is limited to 1 by default (See property: `migration.ds.target.db.max.stage.migrations`)

## Cleanup

It is mandatory to leave the system in a clean state:
  * Remove the migration extensions after the migration. This applies to all environments once you have finished the migration activities, including the production environment.
  * Delete the tables that are resulting from the staged migrations and not required for the functioning of the application.
  * You may want to use the following to support cleanup: [Support Cleanup](../support/SUPPORT-GUIDE.md)
  * Be aware that it eventually is your responsibility, what data is stored in the target database.


## Audit and Logging

All actions triggered from Commerce DB Sync will be logged:
  * validate data source
  * preview schema migration
  * create schema script
  * execute schema script
  * run migration
  * stop migration

The format is: `CMT Action: <Action> - User: <UserId> - Time: <Timestamp>`

Example:

```
CMT Action: Data sources tab clicked - User:admin - Time:2021-03-10T10:27:29.675351
CMT Action: Validate connections button clicked - User:admin - Time:2021-03-10T10:27:32.258041
CMT Action: Validate connections button clicked - User:admin - Time:2021-03-10T10:27:36.223859
CMT Action: Schema migration tab clicked - User:admin - Time:2021-03-10T10:27:38.188141
CMT Action: Preview schema migration changes button clicked - User:admin - Time:2021-03-10T10:27:40.492816
Starting preview of source and target db diff...
....
CMT Action: Data migration tab clicked - User:admin - Time:2021-03-10T10:28:31.993621
CMT Action: Start data migration executed - User:admin - Time:2021-03-10T10:28:33.710384
0/1 tables migrated. 0 failed. State: RUNNING
173/173 processed. Completed in {223.6 ms}. Last Update: {2021-03-10T09:28:34.153}
1/1 tables migrated. 0 failed. State: PROCESSED
Migration finished (PROCESSED) in 00:00:00.296
Migration finished on Node 0 with result false
DefaultMigrationPostProcessor Finished
Finished writing database migration report
```

## Security of the external database

For the use case for which the customer replicates data across its own database, due deligence is required to secure the external database:
* Customer should secure the customer DB with proper configuration so that it doesn’t lead to DOS or buffer overflow attack.
* Dedicated user who would be having access to external DB should have minimum privilege. 
* Personal data should clean up external DB after the retention period is reached as per GDPR.
*	Customer needs to be aware of the size of the data they are migrating and need to manage the limits of the DB accordingly.

## Reporting

In order to be able to track past activities, the tool creates reports against the following actions:

  * SQL statements executed during schema migration (file name: timestamp of execution);
  * Summary of the migration copy process (file name: migration id)

The reports are automatically written to the hotfolder blob storage ('migration' folder).
Sensitive data is not written to the reports (i.e.: passwords).
