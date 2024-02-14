DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYTASKS;
#
CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYTASKS
(
    targetnodeId           int          NOT NULL,
    migrationId            VARCHAR(255) NOT NULL,
    pipelinename           VARCHAR(255) NOT NULL,
    sourcetablename        VARCHAR(255) NOT NULL,
    targettablename        VARCHAR(255) NOT NULL,
    columnmap              TEXT NULL,
    duration               VARCHAR(255) NULL,
    sourcerowcount         int          NOT NULL DEFAULT 0,
    targetrowcount         int          NOT NULL DEFAULT 0,
    failure                char(1)      NOT NULL DEFAULT '0',
    error                  TEXT NULL,
    published              char(1)      NOT NULL DEFAULT '0',
    truncated              char(1)      NOT NULL DEFAULT '0',
    lastupdate             DATETIME     NOT NULL DEFAULT '0001-01-01 00:00:00',
    avgwriterrowthroughput numeric(10, 2) NULL DEFAULT 0,
    avgreaderrowthroughput numeric(10, 2) NULL DEFAULT 0,
    copymethod             VARCHAR(255) NULL,
    keycolumns             VARCHAR(255) NULL,
    durationinseconds      numeric(10, 2) NULL DEFAULT 0,
    batchsize              int          NOT NULL DEFAULT 1000,
    PRIMARY KEY (migrationid, targetnodeid, pipelinename)
);
#
DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYBATCHES;
#
CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES
(
    migrationId   VARCHAR(255) NOT NULL,
    batchId       int          NOT NULL DEFAULT 0,
    pipelinename  VARCHAR(255) NOT NULL,
    lowerBoundary VARCHAR(255) NOT NULL,
    upperBoundary VARCHAR(255) NULL,
    PRIMARY KEY (migrationid, batchId, pipelinename)
);
#
DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYSTATUS;
#
CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYSTATUS
(
    migrationId VARCHAR(255) NOT NULL,
    startAt     datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    endAt       datetime,
    lastUpdate  datetime,
    total       int          NOT NULL DEFAULT 0,
    completed   int          NOT NULL DEFAULT 0,
    failed      int          NOT NULL DEFAULT 0,
    status      VARCHAR(255) NOT NULL DEFAULT 'RUNNING',
    PRIMARY KEY (migrationid)
);
#
DROP TRIGGER IF EXISTS MIGRATIONTOOLKIT_TABLECOPYSTATUS_Insert;
DROP TRIGGER IF EXISTS MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update;
#
CREATE TRIGGER MIGRATIONTOOLKIT_TABLECOPYSTATUS_Insert
    AFTER INSERT
    ON MIGRATIONTOOLKIT_TABLECOPYTASKS
    FOR EACH ROW
BEGIN
    -- latest update overall = latest update timestamp of updated tasks
    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS s
    SET s.lastUpdate = NEW.lastUpdate
    WHERE s.migrationId = NEW.migrationId;
END;
#
CREATE TRIGGER MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update
    AFTER UPDATE
    ON MIGRATIONTOOLKIT_TABLECOPYTASKS
    FOR EACH ROW
BEGIN
    -- latest update overall = latest update timestamp of updated tasks
    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS s
    SET s.lastUpdate = NEW.lastUpdate
    WHERE s.migrationId = OLD.migrationId;

    IF NEW.failure = '1' OR NEW.duration IS NOT NULL THEN
        UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS s
        INNER JOIN (
            SELECT migrationId, COUNT(pipelinename) AS completed
            FROM MIGRATIONTOOLKIT_TABLECOPYTASKS
            WHERE duration IS NOT NULL
            GROUP BY migrationId
        ) AS t
        ON s.migrationId = t.migrationId
            SET s.completed = t.completed;

        -- update failed count when tasks failed
        UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS s
        INNER JOIN (
            SELECT migrationId, COUNT(pipelinename) AS failed
            FROM MIGRATIONTOOLKIT_TABLECOPYTASKS
            WHERE failure = '1'
            GROUP BY migrationId
            ) AS t
        ON s.migrationId = t.migrationId
            SET s.failed = t.failed;

        UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
        SET endAt = UTC_TIMESTAMP()
        WHERE migrationId = OLD.migrationId
          AND total = completed
          AND endAt IS NULL;

        UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
        SET status = 'PROCESSED'
        WHERE migrationId = OLD.migrationId
          AND status = 'RUNNING'
          AND total = completed;
    END IF;
END;
