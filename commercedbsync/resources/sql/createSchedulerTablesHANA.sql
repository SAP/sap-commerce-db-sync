

CREATE OR REPLACE PROCEDURE MIGRATION_PROCEDURE (IN tablename VARCHAR(1000))
  LANGUAGE SQLSCRIPT AS
BEGIN
	DECLARE found INT=0;
SELECT count(*) INTO found FROM OBJECTS WHERE OBJECT_TYPE='TABLE' AND OBJECT_NAME=:tablename;
IF tablename = 'MIGRATIONTOOLKIT_TABLECOPYTASKS' AND :found > 0
	THEN
DROP TABLE MIGRATIONTOOLKIT_TABLECOPYTASKS;
END IF;

IF tablename = 'MIGRATIONTOOLKIT_TABLECOPYSTATUS' AND :found > 0
	THEN
DROP TABLE MIGRATIONTOOLKIT_TABLECOPYSTATUS;
END IF;

IF tablename = 'MIGRATIONTOOLKIT_TABLECOPYBATCHES' AND :found > 0
	THEN
DROP TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES;
END IF;
END;
#
CALL MIGRATION_PROCEDURE('MIGRATIONTOOLKIT_TABLECOPYTASKS');
#

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYTASKS (
                                                 targetnodeId int NOT NULL,
                                                 migrationId NVARCHAR(255) NOT NULL,
                                                 pipelinename NVARCHAR(255) NOT NULL,
                                                 sourcetablename NVARCHAR(255) NOT NULL,
                                                 targettablename NVARCHAR(255) NOT NULL,
                                                 columnmap NVARCHAR(5000) NULL,
                                                 duration NVARCHAR (255) NULL,
                                                 sourcerowcount int NOT NULL DEFAULT 0,
                                                 targetrowcount int NOT NULL DEFAULT 0,
                                                 failure char(1) NOT NULL DEFAULT '0',
                                                 error NVARCHAR(5000) NULL,
                                                 published char(1) NOT NULL DEFAULT '0',
                                                 truncated char(1) NOT NULL DEFAULT '0',
                                                 lastupdate Timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
                                                 avgwriterrowthroughput numeric(10,2) NULL DEFAULT 0,
                                                 avgreaderrowthroughput numeric(10,2) NULL DEFAULT 0,
                                                 copymethod NVARCHAR(255) NULL,
                                                 keycolumns NVARCHAR(255) NULL,
                                                 durationinseconds numeric(10,2) NULL DEFAULT 0,
                                                 PRIMARY KEY (migrationid, targetnodeid, pipelinename)
);

#

CALL MIGRATION_PROCEDURE('MIGRATIONTOOLKIT_TABLECOPYBATCHES');
#

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES (
                                                 migrationId NVARCHAR(255) NOT NULL,
                                                 batchId int NOT NULL DEFAULT 0,
                                                 pipelinename NVARCHAR(255) NOT NULL,
                                                 lowerBoundary NVARCHAR(255) NOT NULL,
                                                 upperBoundary NVARCHAR(255) NULL,
                                                 PRIMARY KEY (migrationid, batchId, pipelinename)
);

#

CALL MIGRATION_PROCEDURE('MIGRATIONTOOLKIT_TABLECOPYSTATUS');
#

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYSTATUS (
                                                  migrationId NVARCHAR(255) NOT NULL,
                                                  startAt Timestamp NOT NULL DEFAULT CURRENT_UTCDATE,
                                                  endAt Timestamp,
                                                  lastUpdate Timestamp,
                                                  total int NOT NULL DEFAULT 0,
                                                  completed int NOT NULL DEFAULT 0,
                                                  failed int NOT NULL DEFAULT 0,
                                                  status NVARCHAR(255) NOT NULL DEFAULT 'RUNNING',
                                                  PRIMARY KEY (migrationid)
);

#


CREATE OR REPLACE TRIGGER MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update_trigger
AFTER UPDATE
                 ON MIGRATIONTOOLKIT_TABLECOPYTASKS
                 REFERENCING OLD ROW AS old, NEW ROW AS new
                 FOR EACH ROW
BEGIN
    /* ORIGSQL: PRAGMA AUTONOMOUS_TRANSACTION; */
  --  BEGIN AUTONOMOUS TRANSACTION
        DECLARE var_pipeline_count DECIMAL(38,10);  /* ORIGSQL: var_pipeline_count NUMBER ; */

        /* ORIGSQL: CURSOR cur_count_pipeline IS select COUNT(pipelinename) countpipelines from MIGR(...) */
        DECLARE CURSOR cur_count_pipeline
        FOR
SELECT   /* ORIGSQL: SELECT COUNT(pipelinename) countpipelines from MIGRATIONTOOLKIT_TABLECOPYTASKS w(...) */
    COUNT(pipelinename) AS countpipelines
FROM
    MIGRATIONTOOLKIT_TABLECOPYTASKS
WHERE
        failure = '1'
   OR duration IS NOT NULL;

/* RESOLVE: Trigger declaration: Additional conversion may be required */

/* ORIGSQL: OPEN cur_count_pipeline; */
OPEN cur_count_pipeline;

/* ORIGSQL: FETCH cur_count_pipeline INTO var_pipeline_count; */
FETCH cur_count_pipeline INTO var_pipeline_count;

IF (:var_pipeline_count > 0)
        THEN
            -- completed count
            /* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET COMPLETED = NVL((SELECT count(*) (...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST
SET
    /* ORIGSQL: COMPLETED = */
    COMPLETED = IFNULL(  /* ORIGSQL: NVL((SELECT count(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK WHERE ST.migrationi(...) */
            (
                SELECT   /* ORIGSQL: (SELECT COUNT(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK WHERE ST.migrationid = (...) */
                    COUNT(*)
                FROM
                    MIGRATIONTOOLKIT_TABLECOPYTASKS TK
                WHERE
                        ST.migrationid = TK.migrationid
                  AND duration IS NOT NULL
                GROUP BY
                    migrationid
            )
        ,0);

-- failed count
/* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET failed = NVL((SELECT count(*) FRO(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST
SET
    /* ORIGSQL: failed = */
    failed = IFNULL(  /* ORIGSQL: NVL((SELECT count(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK WHERE ST.migrationi(...) */
            (
                SELECT   /* ORIGSQL: (SELECT COUNT(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK WHERE ST.migrationid = (...) */
                    COUNT(*)
                FROM
                    MIGRATIONTOOLKIT_TABLECOPYTASKS TK
                WHERE
                        ST.migrationid = TK.migrationid
                  AND failure = '1'
                GROUP BY
                    migrationid
            )
        ,0);
END IF;
        -- this takes care of THIS ROW, for which trigger is fired
        IF   /* ORIGSQL: IF UPDATING AND */
:new.failure = '1'
        AND :old.failure = '0'
        THEN
            /* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET failed = failed + 1 WHERE migrati(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST
SET
    /* ORIGSQL: failed = */
    failed = failed + 1
WHERE
        migrationid = :new.migrationid;

--INSERT INTO EVENT_LOG_CMT (DESCRIPTION, COUNTS) VALUES ('Updating failed', 1);
END IF;

        -- this takes care of THIS ROW,l for which trigger is fired
        IF  /* ORIGSQL: IF UPDATING AND */
:new.duration IS NOT NULL
        AND :old.duration IS NULL
        THEN
            /* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET completed = completed + 1 WHERE m(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST
SET
    /* ORIGSQL: completed = */
    completed = completed + 1
WHERE
        migrationid = :new.migrationid
  AND total > completed;

--INSERT INTO EVENT_LOG_CMT (DESCRIPTION, COUNTS) VALUES ('Updating completed', 1);
END IF;

        -- this sQL is slightly diff from the SQL server one
        /* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS SET lastupdate = sys_extract_utc(systime(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
SET
    /* ORIGSQL: lastupdate = */
    lastupdate = CURRENT_UTCTIMESTAMP   /* ORIGSQL: sys_extract_utc(systimestamp) */
WHERE
        migrationid = :new.migrationid;

/* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS SET endAt = sys_extract_utc(systimestamp(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
SET
    /* ORIGSQL: endAt = */
    endAt = CURRENT_UTCTIMESTAMP   /* ORIGSQL: sys_extract_utc(systimestamp) */
WHERE
        total = completed
  AND endAt IS NULL;

/* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS SET status = 'PROCESSED' WHERE status = (...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
SET
    /* ORIGSQL: status = */
    status = 'PROCESSED'
WHERE
        status = 'RUNNING'
  AND total = completed;

/* ORIGSQL: COMMIT; */
/* RESOLVE: Statement 'COMMIT' not currently supported in HANA SQL trigger objects */
-- COMMIT;; /* NOT CONVERTED! */
-- END;
END;

#

CREATE OR REPLACE TRIGGER MIGRATIONTOOLKIT_TABLECOPYSTATUS_Insert_trigger
AFTER INSERT
ON MIGRATIONTOOLKIT_TABLECOPYTASKS
REFERENCING OLD ROW AS old, NEW ROW AS new
FOR EACH ROW
BEGIN
    /* ORIGSQL: PRAGMA AUTONOMOUS_TRANSACTION; */
  --  BEGIN AUTONOMOUS TRANSACTION
        DECLARE var_pipeline_count DECIMAL(38,10);  /* ORIGSQL: var_pipeline_count NUMBER ; */

        /* ORIGSQL: CURSOR cur_count_pipeline IS select COUNT(pipelinename) countpipelines from MIGR(...) */
        DECLARE CURSOR cur_count_pipeline
        FOR
SELECT   /* ORIGSQL: SELECT COUNT(pipelinename) countpipelines from MIGRATIONTOOLKIT_TABLECOPYTASKS w(...) */
    COUNT(pipelinename) AS countpipelines
FROM
    MIGRATIONTOOLKIT_TABLECOPYTASKS
WHERE
        failure = '1'
   OR duration IS NOT NULL;

/* RESOLVE: Trigger declaration: Additional conversion may be required */

/* ORIGSQL: OPEN cur_count_pipeline; */
OPEN cur_count_pipeline;

/* ORIGSQL: FETCH cur_count_pipeline INTO var_pipeline_count; */
FETCH cur_count_pipeline INTO var_pipeline_count;

IF (:var_pipeline_count > 0)
        THEN
            -- completed count
            /* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET COMPLETED = NVL((SELECT count(*) (...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST
SET
    /* ORIGSQL: COMPLETED = */
    COMPLETED = IFNULL(  /* ORIGSQL: NVL((SELECT count(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK WHERE ST.migrationi(...) */
            (
                SELECT   /* ORIGSQL: (SELECT COUNT(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK WHERE ST.migrationid = (...) */
                    COUNT(*)
                FROM
                    MIGRATIONTOOLKIT_TABLECOPYTASKS TK
                WHERE
                        ST.migrationid = TK.migrationid
                  AND duration IS NOT NULL
                GROUP BY
                    migrationid
            )
        ,0);

-- failed count
/* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET failed = NVL((SELECT count(*) FRO(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST
SET
    /* ORIGSQL: failed = */
    failed = IFNULL(  /* ORIGSQL: NVL((SELECT count(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK WHERE ST.migrationi(...) */
            (
                SELECT   /* ORIGSQL: (SELECT COUNT(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK WHERE ST.migrationid = (...) */
                    COUNT(*)
                FROM
                    MIGRATIONTOOLKIT_TABLECOPYTASKS TK
                WHERE
                        ST.migrationid = TK.migrationid
                  AND failure = '1'
                GROUP BY
                    migrationid
            )
        ,0);
END IF;


        -- this takes care of THIS ROW,l for which trigger is fired
        IF   /* ORIGSQL: IF INSERTING AND */
:new.failure = '1'
        THEN
            /* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET failed = failed + 1 WHERE migrati(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST
SET
    /* ORIGSQL: failed = */
    failed = failed + 1
WHERE
        migrationid = :new.migrationid;

--INSERT INTO EVENT_LOG_CMT (DESCRIPTION, COUNTS) VALUES ('INSERTING failed', 1);
END IF;

        -- this takes care of THIS ROW,l for which trigger is fired
        IF  /* ORIGSQL: IF INSERTING AND */
:new.duration IS NOT NULL
        THEN
            /* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET completed = completed + 1 WHERE m(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST
SET
    /* ORIGSQL: completed = */
    completed = completed + 1
WHERE
        migrationid = :new.migrationid
  AND total > completed;

/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

--INSERT INTO EVENT_LOG_CMT (DESCRIPTION, COUNTS) VALUES ('INSERTING completed', 1);
END IF;

        -- this sQL is slightly diff from the SQL server one
        /* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS SET lastupdate = sys_extract_utc(systime(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
SET
    /* ORIGSQL: lastupdate = */
    lastupdate = CURRENT_UTCTIMESTAMP   /* ORIGSQL: sys_extract_utc(systimestamp) */
WHERE
        migrationid = :new.migrationid;

/* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS SET endAt = sys_extract_utc(systimestamp(...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
SET
    /* ORIGSQL: endAt = */
    endAt = CURRENT_UTCTIMESTAMP   /* ORIGSQL: sys_extract_utc(systimestamp) */
WHERE
        total = completed
  AND endAt IS NULL;

/* ORIGSQL: UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS SET status = 'PROCESSED' WHERE status = (...) */
UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
SET
    /* ORIGSQL: status = */
    status = 'PROCESSED'
WHERE
        status = 'RUNNING'
  AND total = completed;

/* ORIGSQL: COMMIT; */
/* RESOLVE: Statement 'COMMIT' not currently supported in HANA SQL trigger objects */
-- COMMIT;; /* NOT CONVERTED! */
-- END;
END;

#