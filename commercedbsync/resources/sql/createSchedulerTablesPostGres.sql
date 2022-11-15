/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */



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
    sourcerowcount int NOT NULL DEFAULT 0,
    targetrowcount int NOT NULL DEFAULT 0,
    failure char(1) NOT NULL DEFAULT '0',
    error text NULL,
    published char(1) NOT NULL DEFAULT '0',
    lastupdate timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
    avgwriterrowthroughput numeric(10,2) NULL DEFAULT 0,
    avgreaderrowthroughput numeric(10,2) NULL DEFAULT 0,
    durationinseconds numeric(10,2) NULL DEFAULT 0,
    PRIMARY KEY (migrationid, targetnodeid, pipelinename)
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