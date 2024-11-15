DROP TABLE IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFS

CREATE TABLE MIGRATIONTOOLKIT_SCHEMADIFFS (
    pk INT IDENTITY(1,1) PRIMARY KEY,
    schemaDifferenceId NVARCHAR(255) NOT NULL,
    referenceDatabase NVARCHAR(255) NOT NULL,
    missingTableLeftName NVARCHAR(255) NOT NULL,
    missingTableRightName NVARCHAR(255) NOT NULL,
    missingColumnName NVARCHAR(255) NULL
);

DROP TABLE IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFTASKS

CREATE TABLE MIGRATIONTOOLKIT_SCHEMADIFFTASKS (
    schemaDifferenceId NVARCHAR(255) NOT NULL,
    pipelinename NVARCHAR(255) NOT NULL,
    duration NVARCHAR (255) NULL,
    failure INT NOT NULL DEFAULT 0,
    error NVARCHAR(MAX) NULL,
    lastUpdate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    durationinseconds NUMERIC(10,2) NULL DEFAULT 0,
    PRIMARY KEY (schemaDifferenceId, pipelinename)
);

DROP TABLE IF EXISTS MIGRATIONTOOLKIT_SCHEMADIFFSTATUS

CREATE TABLE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS (
    schemaDifferenceId NVARCHAR(255) NOT NULL PRIMARY KEY,
    startAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    endAt DATETIME2,
    lastUpdate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    total INT NOT NULL DEFAULT 4,
    completed INT NOT NULL DEFAULT 0,
    failed INT NOT NULL DEFAULT 0,
    status NVARCHAR(255) NOT NULL DEFAULT 'RUNNING',
    sqlScript NVARCHAR(MAX) NULL
);

IF OBJECT_ID ('MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Update','TR') IS NOT NULL
    DROP TRIGGER MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Update;

CREATE TRIGGER MIGRATIONTOOLKIT_SCHEMADIFFSTATUS_Update
ON MIGRATIONTOOLKIT_SCHEMADIFFTASKS
AFTER INSERT, UPDATE
AS
BEGIN
    DECLARE @relevant_count integer = 0
    SET NOCOUNT ON
    -- latest update overall = latest update timestamp of updated tasks
    UPDATE s
    SET s.lastUpdate = t.latestUpdate
    FROM MIGRATIONTOOLKIT_SCHEMADIFFSTATUS s
    INNER JOIN (
        SELECT schemaDifferenceId, MAX(lastUpdate) AS latestUpdate
        FROM inserted
        GROUP BY schemaDifferenceId
    ) AS t
    ON s.schemaDifferenceId = t.schemaDifferenceId

    SELECT @relevant_count = COUNT(pipelinename)
    FROM inserted
    WHERE failure = '1'
       OR duration IS NOT NULL

    IF @relevant_count > 0
    BEGIN
        -- updated completed count when tasks completed
        UPDATE s
        SET s.completed = t.completed
        FROM MIGRATIONTOOLKIT_SCHEMADIFFSTATUS s
        INNER JOIN (
            SELECT schemaDifferenceId, COUNT(pipelinename) AS completed
            FROM MIGRATIONTOOLKIT_SCHEMADIFFTASKS
            WHERE duration IS NOT NULL
            GROUP BY schemaDifferenceId
        ) AS t
        ON s.schemaDifferenceId = t.schemaDifferenceId
        -- update failed count when tasks failed
        UPDATE s
        SET s.failed = t.failed
        FROM MIGRATIONTOOLKIT_SCHEMADIFFSTATUS s
        INNER JOIN (
            SELECT schemaDifferenceId, COUNT(pipelinename) AS failed
            FROM MIGRATIONTOOLKIT_SCHEMADIFFTASKS
            WHERE failure = '1'
            GROUP BY schemaDifferenceId
        ) AS t
        ON s.schemaDifferenceId = t.schemaDifferenceId

        UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS
        SET endAt = GETUTCDATE()
        WHERE total = completed
        AND endAt IS NULL

        UPDATE MIGRATIONTOOLKIT_SCHEMADIFFSTATUS
        SET status = 'COMPLETED'
        WHERE status = 'RUNNING'
          AND total = completed
    END
END;
