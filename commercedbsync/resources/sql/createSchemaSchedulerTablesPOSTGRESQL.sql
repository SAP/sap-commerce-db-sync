DROP TABLE IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFS;
#
CREATE TABLE MIGRATIONTOOLKIT_SCHEMADIFFS (
    pk SERIAL PRIMARY KEY,
    schemaDifferenceId VARCHAR(255) NOT NULL,
    referenceDatabase VARCHAR(255) NOT NULL,
    missingTableLeftName VARCHAR(255) NOT NULL,
    missingTableRightName VARCHAR(255) NOT NULL,
    missingColumnName VARCHAR(255) NULL
);
#
DROP TABLE IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFTASKS;
#
CREATE TABLE MIGRATIONTOOLKIT_SCHEMADIFFTASKS (
    schemaDifferenceId VARCHAR(255) NOT NULL,
    pipelinename VARCHAR(255) NOT NULL,
    duration VARCHAR(255) NULL,
    failure INT NOT NULL DEFAULT 0,
    error TEXT NULL,
    lastUpdate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    durationinseconds NUMERIC(10,2) NULL DEFAULT 0,
    PRIMARY KEY (schemaDifferenceId, pipelinename)
);
#
DROP TABLE IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFSTATUS;
#
CREATE TABLE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS (
    schemaDifferenceId VARCHAR(255) NOT NULL PRIMARY KEY,
    startAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    endAt TIMESTAMP,
    lastUpdate TIMESTAMP,
    total INT NOT NULL DEFAULT 4,
    completed INT NOT NULL DEFAULT 0,
    failed INT NOT NULL DEFAULT 0,
    status VARCHAR(255) NOT NULL DEFAULT 'RUNNING',
    sqlScript TEXT NULL
);
#
DROP TRIGGER  IF EXISTS  MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Update ON MIGRATIONTOOLKIT_SCHEMADIFFTASKS  CASCADE;
#
DROP FUNCTION IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_proc;
#
CREATE FUNCTION MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_proc() RETURNS trigger AS $$

DECLARE relevant_count integer default 0;
BEGIN

    UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS AS s
    SET lastUpdate = t.latestUpdate
    FROM ( SELECT NEW.schemaDifferenceId, MAX(NEW.lastUpdate) AS latestUpdate
    GROUP BY schemaDifferenceId
        ) AS t
    WHERE s.schemaDifferenceId = t.schemaDifferenceId;

     relevant_count = COUNT(NEW.pipelinename)
    WHERE NEW.failure = '1'
       OR NEW.duration IS NOT NULL;

     IF relevant_count > 0 then
    -- updated completed count when tasks completed
    UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS AS s
    SET completed = t.completed
    FROM ( SELECT schemaDifferenceId, COUNT(pipelinename) AS completed
           FROM MIGRATIONTOOLKIT_SCHEMADIFFTASKS
           WHERE duration IS NOT NULL
           GROUP BY schemaDifferenceId
         ) AS t
    WHERE s.schemaDifferenceId = t.schemaDifferenceId;

    -- update failed count when tasks failed
    UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS AS s
    SET failed = t.failed
    FROM ( SELECT schemaDifferenceId, COUNT(pipelinename) AS failed
           FROM MIGRATIONTOOLKIT_SCHEMADIFFTASKS
           WHERE failure = '1'
           GROUP BY schemaDifferenceId
         ) AS t
    WHERE s.schemaDifferenceId = t.schemaDifferenceId;

    UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS
    SET endAt = NOW()
    WHERE total = completed
      AND endAt IS NULL;

    UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS
    SET status = 'COMPLETED'
    WHERE status = 'RUNNING'
      AND total = completed;
END if;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
#
CREATE TRIGGER MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Update
    AFTER INSERT OR UPDATE ON MIGRATIONTOOLKIT_SCHEMADIFFTASKS
    FOR EACH ROW EXECUTE PROCEDURE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_proc();
#