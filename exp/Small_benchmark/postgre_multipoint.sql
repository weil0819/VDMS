/* ---------------------------------------------------- */
/*  Created On : 22-Sept-2020 	             			*/
/*  DBMS       : PostgreSQL    			     			*/
/*  Function   : benchmark 								*/
/*  Usage      : \i D:/Wei/VDMS/flat.sql 				*/
/*  Author     : Wesley      			     			*/
/* ---------------------------------------------------- */


/* PostgreSQL MULTIPOINT */
CREATE EXTENSION IF NOT EXISTS postgis;

-- Creating table
DROP TABLE IF EXISTS small_multipoint CASCADE;
CREATE TABLE small_multipoint(
	id SERIAL PRIMARY KEY,
	geom geometry(MULTIPOINTZ, 28356)
);

-- Loading voxels
CREATE TEMP TABLE tmp(
	id SERIAL PRIMARY KEY,
	x DOUBLE PRECISION,
	y DOUBLE PRECISION,
	z DOUBLE PRECISION,
	objID SMALLINT
);

\COPY tmp(x,y,z,objID) FROM 'D:\Wei\RawVoxelDataset\build\bld3.xyz' DELIMITER ' ';
-- https://postgis.net/docs/ST_Collect.html
INSERT INTO small_multipoint (geom) 
SELECT ST_Collect(T.pt) FROM (
		SELECT ST_SetSRID(ST_MakePoint(x*0.2+336000, y*0.2+6245250, z*0.2+20), 28356) AS pt, 
		row_number() OVER (order by id) AS n 
		FROM tmp
	) AS T 
	GROUP BY ((n-1)/5000)
	ORDER BY ((n-1)/5000)
;
DROP TABLE tmp;

-- Creating index
DROP INDEX IF EXISTS small_multipoint_xy_idx CASCADE;
CREATE INDEX small_multipoint_xy_idx on small_multipoint USING GIST (geom);

VACUUM FULL ANALYZE small_multipoint;


