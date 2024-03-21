
DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYTASKS;

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYTASKS (
    targetnodeId int NOT NULL,
    migrationId NVARCHAR(255) NOT NULL,
    pipelinename NVARCHAR(255) NOT NULL,
    sourcetablename NVARCHAR(255) NOT NULL,
    targettablename NVARCHAR(255) NOT NULL,
    columnmap NVARCHAR(MAX) NULL,
    duration NVARCHAR (255) NULL,
    sourcerowcount int NOT NULL DEFAULT 0,
    targetrowcount int NOT NULL DEFAULT 0,
    failure char(1) NOT NULL DEFAULT '0',
    error NVARCHAR(MAX) NULL,
    published char(1) NOT NULL DEFAULT '0',
    truncated char(1) NOT NULL DEFAULT '0',
    lastupdate DATETIME2 NOT NULL DEFAULT '0001-01-01 00:00:00',
    avgwriterrowthroughput numeric(10,2) NULL DEFAULT 0,
    avgreaderrowthroughput numeric(10,2) NULL DEFAULT 0,
    copymethod NVARCHAR(255) NULL,
    keycolumns NVARCHAR(255) NULL,
    durationinseconds numeric(10,2) NULL DEFAULT 0,
    batchsize int NOT NULL DEFAULT 1000,
    PRIMARY KEY (migrationid, targetnodeid, pipelinename)
);

DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYBATCHES;

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES (
    migrationId NVARCHAR(255) NOT NULL,
    batchId int NOT NULL DEFAULT 0,
    pipelinename NVARCHAR(255) NOT NULL,
    lowerBoundary NVARCHAR(255) NOT NULL,
    upperBoundary NVARCHAR(255) NULL,
    PRIMARY KEY (migrationid, batchId, pipelinename)
);

DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYSTATUS;

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYSTATUS (
    migrationId NVARCHAR(255) NOT NULL,
    startAt datetime2 NOT NULL DEFAULT GETUTCDATE(),
    endAt datetime2,
    lastUpdate datetime2,
    total int NOT NULL DEFAULT 0,
    completed int NOT NULL DEFAULT 0,
    failed int NOT NULL DEFAULT 0,
    status NVARCHAR(255) NOT NULL DEFAULT 'RUNNING'
    PRIMARY KEY (migrationid)
);

IF OBJECT_ID ('MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update','TR') IS NOT NULL
    DROP TRIGGER MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update;

CREATE TRIGGER MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update
ON MIGRATIONTOOLKIT_TABLECOPYTASKS
AFTER INSERT, UPDATE
AS
BEGIN
    DECLARE @relevant_count integer = 0
    SET NOCOUNT ON
    -- latest update overall = latest update timestamp of updated tasks
    UPDATE s
    SET s.lastUpdate = t.latestUpdate
    FROM MIGRATIONTOOLKIT_TABLECOPYSTATUS s
    INNER JOIN (
        SELECT migrationId, MAX(lastUpdate) AS latestUpdate
        FROM inserted
        GROUP BY migrationId
    ) AS t
    ON s.migrationId = t.migrationId

    SELECT @relevant_count = COUNT(pipelinename)
    FROM inserted
    WHERE failure = '1'
       OR duration IS NOT NULL

    IF @relevant_count > 0
    BEGIN
        -- updated completed count when tasks completed
        UPDATE s
        SET s.completed = t.completed
        FROM MIGRATIONTOOLKIT_TABLECOPYSTATUS s
        INNER JOIN (
            SELECT migrationId, COUNT(pipelinename) AS completed
            FROM MIGRATIONTOOLKIT_TABLECOPYTASKS
            WHERE duration IS NOT NULL
            GROUP BY migrationId
        ) AS t
        ON s.migrationId = t.migrationId
        -- update failed count when tasks failed
        UPDATE s
        SET s.failed = t.failed
        FROM MIGRATIONTOOLKIT_TABLECOPYSTATUS s
        INNER JOIN (
            SELECT migrationId, COUNT(pipelinename) AS failed
            FROM MIGRATIONTOOLKIT_TABLECOPYTASKS
            WHERE failure = '1'
            GROUP BY migrationId
        ) AS t
        ON s.migrationId = t.migrationId

        UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
        SET endAt = GETUTCDATE()
        WHERE total = completed
        AND endAt IS NULL

        UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
        SET status = 'PROCESSED'
        WHERE status = 'RUNNING'
          AND total = completed
    END
END;
