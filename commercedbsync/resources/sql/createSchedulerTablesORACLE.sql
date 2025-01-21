BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE MIGRATIONTOOLKIT_TABLECOPYTASKS';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/


	
CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYTASKS (
    targetnodeId number(10) NOT NULL,
    migrationId NVARCHAR2(255) NOT NULL,
    pipelinename NVARCHAR2(255) NOT NULL,
    itemorder int DEFAULT 0 NOT NULL,
    sourcetablename NVARCHAR2(255) NOT NULL,
    targettablename NVARCHAR2(255) NOT NULL,
    columnmap CLOB NULL,
    duration NVARCHAR2 (255) NULL,
    sourcerowcount number(20,0) DEFAULT 0 NOT NULL,
    targetrowcount number(20,0) DEFAULT 0 NOT NULL,
    failure char(1) DEFAULT '0' NOT NULL,
    error CLOB NULL,
    published char(1) DEFAULT '0' NOT NULL,
    truncated char(1) DEFAULT '0' NOT NULL,
    lastupdate Timestamp  NOT NULL,
    avgwriterrowthroughput number(10,2) DEFAULT 0 NULL,
    avgreaderrowthroughput number(10,2) DEFAULT 0 NULL,
    copymethod NVARCHAR2(255) NULL,
    keycolumns NVARCHAR2(255) NULL,
    durationinseconds number(10,2) DEFAULT 0 NULL,
    batchsize number(10) DEFAULT 1000 NOT NULL,
    chunked char(1) NOT NULL DEFAULT '0',
    chunknumber int,
    chunksize BIGINT,
    PRIMARY KEY (migrationid, targetnodeid, pipelinename)
)
/



BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/


CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES (
    migrationId NVARCHAR2(255) NOT NULL,
    batchId number(10) DEFAULT 0 NOT NULL,
    pipelinename NVARCHAR2(255) NOT NULL,
    lowerBoundary NVARCHAR2(255) NOT NULL,
    upperBoundary NVARCHAR2(255) NULL,
    PRIMARY KEY (migrationid, batchId, pipelinename)
)
/



BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES_PART';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/


CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYBATCHES_PART (
    migrationId NVARCHAR2(255) NOT NULL,
    batchId number(10) DEFAULT 0 NOT NULL,
    pipelinename NVARCHAR2(255) NOT NULL,
    lowerBoundary NVARCHAR2(255) NOT NULL,
    upperBoundary NVARCHAR2(255) NULL,
    partition VARCHAR(128) NOT NULL,
    PRIMARY KEY (migrationid, batchId, pipelinename, partition)
)
/




BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE MIGRATIONTOOLKIT_TABLECOPYSTATUS';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/


CREATE TABLE MIGRATIONTOOLKIT_TABLECOPYSTATUS (
    migrationId NVARCHAR2(255) NOT NULL,
    startAt TimeStamp DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    endAt Timestamp,
    lastUpdate Timestamp,
    total number(10) DEFAULT 0 NOT NULL,
    completed number(10) DEFAULT 0 NOT NULL,
    failed number(10) DEFAULT 0 NOT NULL,
    status NVARCHAR2(255) DEFAULT 'RUNNING' NOT NULL,
    PRIMARY key(migrationid)
)
/




CREATE OR REPLACE TRIGGER MIGRATIONTOOLKIT_TABLECOPYSTATUS_Update
    AFTER INSERT  OR UPDATE
    ON MIGRATIONTOOLKIT_TABLECOPYTASKS
    FOR EACH ROW   
DECLARE
	PRAGMA AUTONOMOUS_TRANSACTION;

	var_pipeline_count NUMBER ;

	CURSOR cur_count_pipeline
	IS select count(pipelinename) countpipelines from MIGRATIONTOOLKIT_TABLECOPYTASKS where failure='1' OR duration is not NULL;
	
BEGIN	 
    
 
    OPEN cur_count_pipeline;
 	FETCH cur_count_pipeline INTO var_pipeline_count;
 	IF (var_pipeline_count > 0 ) THEN
 		-- completed count
	    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET COMPLETED = 
			NVL
			((SELECT count(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK
				WHERE 
					ST.migrationid = TK.migrationid
					AND duration IS NOT NULL
				GROUP BY migrationid
			),0);
	
	    -- failed count
		UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET failed = 
			NVL
			((SELECT count(*) FROM MIGRATIONTOOLKIT_TABLECOPYTASKS TK
				WHERE 
					ST.migrationid = TK.migrationid
					AND failure='1'
				GROUP BY migrationid
			),0);
		
    END IF;
    -- this takes care of THIS ROW, for which trigger is fired
	IF UPDATING AND :NEW.failure='1' AND :OLD.failure='0' THEN
    	UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET failed = failed + 1 WHERE migrationid = :NEW.migrationid;
    	--INSERT INTO EVENT_LOG_CMT (DESCRIPTION, COUNTS) VALUES ('Updating failed', 1);
    END IF;
   
   	-- this takes care of THIS ROW,l for which trigger is fired
    IF UPDATING AND :NEW.duration IS NOT NULL AND :OLD.duration IS NULL THEN
    	UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET completed = completed + 1 WHERE migrationid = :NEW.migrationid;
    	--INSERT INTO EVENT_LOG_CMT (DESCRIPTION, COUNTS) VALUES ('Updating completed', 1);
    END IF;
   
   	 -- this takes care of THIS ROW,l for which trigger is fired
    IF INSERTING AND :NEW.failure='1' THEN
    	UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET failed = failed + 1 WHERE migrationid = :NEW.migrationid;
    	--INSERT INTO EVENT_LOG_CMT (DESCRIPTION, COUNTS) VALUES ('INSERTING failed', 1);
    END IF;
   
   	-- this takes care of THIS ROW,l for which trigger is fired
    IF INSERTING AND :NEW.duration IS NOT NULL THEN
    	UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS ST SET completed = completed + 1 WHERE migrationid = :NEW.migrationid;
    	--INSERT INTO EVENT_LOG_CMT (DESCRIPTION, COUNTS) VALUES ('INSERTING completed', 1);
    END IF;
 	
    -- this sQL is slightly diff from the SQL server one
	UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS 
		SET lastupdate = sys_extract_utc(systimestamp) 
		WHERE migrationid = :NEW.migrationid;
   
    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
        SET endAt = sys_extract_utc(systimestamp)
        WHERE total = completed
        AND endAt IS NULL;
    
    UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS
        SET status = 'PROCESSED'
        WHERE status = 'RUNNING'
          AND total = completed;
    COMMIT;
END;

/

