DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYTASKS;

#

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYTASKS (
    targetnodeId int NOT NULL,
    migrationId VARCHAR(255) NOT NULL,
    pipelinename VARCHAR(255) NOT NULL,
    sourcetablename VARCHAR(255) NOT NULL,
    targettablename VARCHAR(255) NOT NULL,
    columnmap text NULL,
    duration VARCHAR (255) NULL,
    sourcerowcount bigint NOT NULL DEFAULT 0,
    targetrowcount bigint NOT NULL DEFAULT 0,
    failure char(1) NOT NULL DEFAULT '0',
    error text NULL,
    published char(1) NOT NULL DEFAULT '0',
    truncated char(1) NOT NULL DEFAULT '0',
    lastupdate timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
    avgwriterrowthroughput numeric(10,2) NULL DEFAULT 0,
    avgreaderrowthroughput numeric(10,2) NULL DEFAULT 0,
    copymethod VARCHAR(255) NULL,
    keycolumns VARCHAR(255) NULL,
    durationinseconds numeric(10,2) NULL DEFAULT 0,
    batchsize int NOT NULL DEFAULT 1000,
    chunked char(1) NOT NULL DEFAULT '0',
    chunknumber int,
    chunksize BIGINT,
    PRIMARY KEY (migrationid, targetnodeid, pipelinename)
);

#

DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYBATCHES;

#

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES (
    migrationId VARCHAR(255) NOT NULL,
    batchId int NOT NULL DEFAULT 0,
    pipelinename VARCHAR(255) NOT NULL,
    lowerBoundary VARCHAR(255) NOT NULL,
    upperBoundary VARCHAR(255) NULL,
    PRIMARY KEY (migrationid, batchId, pipelinename)
);

#

DROP TABLE IF EXISTS MIGRATIONTOOLKIT_TABLECOPYSTATUS;

#

CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYSTATUS (
    migrationId VARCHAR(255) NOT NULL,
    startAt timestamp NOT NULL DEFAULT NOW(),
    endAt timestamp,
    lastUpdate timestamp,
    total int NOT NULL DEFAULT 0,
    completed int NOT NULL DEFAULT 0,
    failed int NOT NULL DEFAULT 0,
    status VARCHAR(255) NOT NULL DEFAULT 'RUNNING',
    PRIMARY KEY (migrationid)
);

#

DROP TRIGGER  IF EXISTS  MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update ON MIGRATIONTOOLKIT_TABLECOPYTASKS  CASCADE;

#

DROP FUNCTION IF EXISTS MIGRATIONTOOLKIT_TABLECOPYSTATUS_proc;

#

CREATE FUNCTION MIGRATIONTOOLKIT_TABLECOPYSTATUS_proc() RETURNS trigger AS $$

DECLARE relevant_count integer default 0;
BEGIN

    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS AS s
    SET lastUpdate = t.latestUpdate
    FROM ( SELECT NEW.migrationId, MAX(NEW.lastUpdate) AS latestUpdate
    GROUP BY migrationId
        ) AS t
    WHERE s.migrationId = t.migrationId;

     relevant_count = COUNT(NEW.pipelinename)
    WHERE NEW.failure = '1'
       OR NEW.duration IS NOT NULL;

     IF relevant_count > 0 then
    -- updated completed count when tasks completed
    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS AS s
    SET completed = t.completed
    FROM ( SELECT migrationId, COUNT(pipelinename) AS completed
           FROM MIGRATIONTOOLKIT_TABLECOPYTASKS
           WHERE duration IS NOT NULL
           GROUP BY migrationId
         ) AS t
    WHERE s.migrationId = t.migrationId;

    -- update failed count when tasks failed
    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS AS s
    SET failed = t.failed
    FROM ( SELECT migrationId, COUNT(pipelinename) AS failed
           FROM MIGRATIONTOOLKIT_TABLECOPYTASKS
           WHERE failure = '1'
           GROUP BY migrationId
         ) AS t
    WHERE s.migrationId = t.migrationId;

    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
    SET endAt = NOW()
    WHERE total = completed
      AND endAt IS NULL;

    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
    SET status = 'PROCESSED'
    WHERE status = 'RUNNING'
      AND total = completed;
END if;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

#

CREATE TRIGGER MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update
    AFTER INSERT OR UPDATE ON MIGRATIONTOOLKIT_TABLECOPYTASKS
    FOR EACH ROW EXECUTE PROCEDURE MIGRATIONTOOLKIT_TABLECOPYSTATUS_proc();

#