DROP SCHEMA IF EXISTS TIMESERIES;
CREATE SCHEMA TIMESERIES;
CREATE TABLE TIMESERIES.MASTERDATA (clust varchar(128) not null, ln integer NOT NULL, id VARCHAR(128) NOT NULL, freq VARCHAR(128), datatype varchar(128), indexes varchar(4096), startts TIMESTAMP NOT NULL, endts TIMESTAMP NOT NULL);
CREATE TABLE TIMESERIES.DOUBLES (clust varchar(128) not null, ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off bigint, val double, CONSTRAINT DOUBLES_PK PRIMARY KEY (clust,ln,id,ts,off));
CREATE TABLE TIMESERIES.INTEGERS (clust varchar(128) not null, ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off bigint, val integer, CONSTRAINT INTEGERS_PK PRIMARY KEY (clust,ln,id,ts,off));
