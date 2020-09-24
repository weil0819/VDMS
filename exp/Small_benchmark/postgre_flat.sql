/* ---------------------------------------------------- */
/*  Created On : 22-Sept-2020 	             			*/
/*  DBMS       : PostgreSQL    			     			*/
/*  Function   : benchmark 								*/
/*  Usage      : \i D:/Wei/VDMS/flat.sql 				*/
/*  Author     : Wesley      			     			*/
/* ---------------------------------------------------- */


/* PostgreSQL flat */
CREATE EXTENSION IF NOT EXISTS postgis;

-- Creating table
DROP TABLE IF EXISTS small_flat CASCADE;
CREATE TABLE small_flat(
	x DOUBLE PRECISION,
	y DOUBLE PRECISION,
	z DOUBLE PRECISION
);

-- Loading voxels
-- \COPY small_flat(x, y, z) FROM 'D:\Wei\RawVoxelDataset\build\bld3.xyz' DELIMITER ' ';
CREATE TEMP TABLE tmp AS SELECT * FROM small_flat LIMIT 0;
ALTER TABLE tmp ADD COLUMN objectID SMALLINT;
\COPY tmp FROM 'D:\Wei\RawVoxelDataset\build\bld3.xyz' DELIMITER ' ';
INSERT INTO small_flat (x, y, z) SELECT x*0.2+336000, y*0.2+6245250, z*0.2+20 FROM tmp;
DROP TABLE tmp;

-- Creating view
CREATE VIEW small_flat_view AS 
SELECT ST_SetSRID(ST_MakePoint(x,y), 28356) AS xy, x, y, z FROM small_flat;

-- Creating index
DROP INDEX IF EXISTS small_flat_xy_idx CASCADE;
CREATE INDEX small_flat_xy_idx on small_flat USING GIST (ST_SetSRID(ST_MakePoint(x,y), 28356));

VACUUM FULL ANALYZE small_flat;


