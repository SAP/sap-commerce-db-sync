# SAP Commerce DB Sync - Developer Guide

## Quick Start

To install SAP Commmerce DB Sync, follow these steps:

Add the following extensions to your localextensions.xml:
```
<extension name="commercemigration"/>
<extension name="commercemigrationhac"/>
```

Make sure you add the source db driver to commercemigration/lib if necessary.

Use the following sample configuration and add it to your local.properties file:

```
migration.ds.source.db.driver=com.mysql.jdbc.Driver
migration.ds.source.db.url=jdbc:mysql://localhost:3600/localdev?useConfigs=maxPerformance&characterEncoding=utf8&useTimezone=true&serverTimezone=UTC&nullCatalogMeansCurrent=true
migration.ds.source.db.username=[user]
migration.ds.source.db.password=[password]
migration.ds.source.db.tableprefix=
migration.ds.source.db.schema=localdev

migration.ds.target.db.driver=${db.driver}
migration.ds.target.db.url=${db.url}
migration.ds.target.db.username=${db.username}
migration.ds.target.db.password=${db.password}
migration.ds.target.db.tableprefix=${db.tableprefix}
migration.ds.target.db.catalog=${db.catalog}
migration.ds.target.db.schema=dbo

```

## Running Integration Tests

Make sure the junit tenant is installed
- set 'installed.tenants=junit' in local.properties
- run 'ant yunitinit' from platformhome

Go to the commercemigrationtest extension, like so:

```
>cd commercemigrationtest
>ant all integrationtests
```

Alternatively go to the platformhome, and trigger it from there:

```
platformhome>ant all integrationtests -Dtestclasses.packages=com.sap.cx.boosters.commercedbsynctest.integration.*
```

The integration tests are parameterized with predefined combinations of source and target databases.
Running the integration tests will bootstrap several database containers using docker and run tests annotated with '@Test', once for each parameter combination.

> **PREREQUISITE**: Make sure docker is installed on your local machine and allocate sufficient memory (~6gb). Also ensure you provide all necessary jdbc drivers for the test execution.

## Connect to existing DB servers for integration tests

If env var `CI` is set, the integration tests will not start a Docker container for every DB, but
connect to existing servers instead.

You can use the `*_HOST`, `*_USR` and `*_PSW` env vars to configure server and user credentials.\
**User / password must be of an admin user! (that is allowed to create schemas/DBs, users etc.)**

(Check out [direnv](https://direnv.net/) to automate setting up those environment variables for
local development)


```sh
export CI=true
# do not drop schemas after each test class -> faster CI runs
# only enable this property if you kill your DB containers regularly
# export CI_SKIP_DROP=true
export MSSQL_HOST=localhost:1433
export MSSQL_USR=sa
export MSSQL_PSW=localSAPassw0rd

export MYSQL_HOST=localhost:3306
export MYSQL_USR=root
export MYSQL_PSW=root

export ORACLE_HOST=localhost:1521
export ORACLE_USR=system
export ORACLE_PSW=oracle

export HANA_HOST=localhost:39017
export HANA_USR=SYSTEM
export HANA_PSW=HXEHana1
```


## Contributing to the Commerce Migration Toolkit

To contribute to the Commerce Migration Toolkit, follow these steps:

1. Fork this repository;
2. Create a branch: `git checkout -b <branch_name>`;
3. Make your changes and commit them: `git commit -m '<commit_message>'`;
4. Push to the original branch: `git push origin <project_name>/<location>`;
5. Create the pull request.

Alternatively, see the GitHub documentation on [creating a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request).
