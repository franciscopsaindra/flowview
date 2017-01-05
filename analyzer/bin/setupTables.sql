CREATE TABLE CDRSTATS (
	timeMillis BIGINT,
	bras VARCHAR(20),
	dslam VARCHAR(40),
	cdrRate FLOAT,
	cdrRateChange FLOAT,
	startCDRRatio FLOAT,
	shortStopCDRRatio FLOAT
);

CREATE TABLE SESSIONS (
	timeMillis BIGINT,
	bras VARCHAR(20),
	dslam VARCHAR(40),
	sessions BIGINT
);

DROP TABLE CDRSTATS;
DROP TABLE SESSIONS;
