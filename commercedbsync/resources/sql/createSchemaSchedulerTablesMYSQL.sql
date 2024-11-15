DROP TABLE IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFS;
#
CREATE TABLE MIGRATIONTOOLKIT_SCHEMADIFFS (
    pk INT AUTO_INCREMENT PRIMARY KEY,
    schemaDifferenceId   VARCHAR(255) NOT NULL,
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
    lastUpdate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    durationinseconds DECIMAL(10,2) NULL DEFAULT 0,
    PRIMARY KEY (schemaDifferenceId, pipelinename)
);
#
DROP TABLE IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFSTATUS;
#
CREATE TABLE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS (
    schemaDifferenceId VARCHAR(255) NOT NULL PRIMARY KEY,
    startAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    endAt DATETIME,
    lastUpdate DATETIME,
    total INT NOT NULL DEFAULT 4,
    completed INT NOT NULL DEFAULT 0,
    failed INT NOT NULL DEFAULT 0,
    status VARCHAR(255) NOT NULL DEFAULT 'RUNNING',
    sqlScript TEXT NULL
);
#
DROP TRIGGER IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Insert;
DROP TRIGGER IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Update;
#
CREATE TRIGGER MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Insert
    AFTER INSERT
    ON MIGRATIONTOOLKIT_SCHEMADIFFTASKS
    FOR EACH ROW
BEGIN
    -- latest update overall = latest update timestamp of updated tasks
    UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS s
    SET s.lastUpdate = NEW.lastUpdate
    WHERE s.schemaDifferenceId = NEW.schemaDifferenceId;
END;
#
CREATE TRIGGER MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Update
    AFTER UPDATE
    ON MIGRATIONTOOLKIT_SCHEMADIFFTASKS
    FOR EACH ROW
BEGIN
    -- latest update overall = latest update timestamp of updated tasks
    UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS s
    SET s.lastUpdate = NEW.lastUpdate
    WHERE s.schemaDifferenceId = OLD.schemaDifferenceId;

    IF NEW.failure = '1' OR NEW.duration IS NOT NULL THEN
        UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS s
        INNER JOIN (
            SELECT schemaDifferenceId, COUNT(pipelinename) AS completed
            FROM MIGRATIONTOOLKIT_SCHEMADIFFTASKS
            WHERE duration IS NOT NULL
            GROUP BY schemaDifferenceId
        ) AS t
        ON s.schemaDifferenceId = t.schemaDifferenceId
            SET s.completed = t.completed;

        -- update failed count when tasks failed
        UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS s
        INNER JOIN (
            SELECT schemaDifferenceId, COUNT(pipelinename) AS failed
            FROM MIGRATIONTOOLKIT_SCHEMADIFFTASKS
            WHERE failure = '1'
            GROUP BY schemaDifferenceId
            ) AS t
        ON s.schemaDifferenceId = t.schemaDifferenceId
            SET s.failed = t.failed;

        UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS
        SET endAt = UTC_TIMESTAMP()
        WHERE schemaDifferenceId = OLD.schemaDifferenceId
          AND total = completed
          AND endAt IS NULL;

        UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS
        SET status = 'COMPLETED'
        WHERE schemaDifferenceId = OLD.schemaDifferenceId
          AND status = 'RUNNING'
          AND total = completed;
    END IF;
END;
