/* ---------------------------------------------------- */
/*  Created On : 22-Sept-2020 	             			*/
/*  DBMS       : PostgreSQL    			     			*/
/*  Function   : benchmark 								*/
/*  Usage      : \i D:/Wei/VDMS/flat.sql 				*/
/*  Author     : Wesley      			     			*/
/* ---------------------------------------------------- */


/* PostgreSQL MULTIPOINT */
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pointcloud;
CREATE EXTENSION IF NOT EXISTS pointcloud_postgis;

DELETE FROM pointcloud_formats WHERE pcid=1;
INSERT INTO pointcloud_formats (pcid, srid, schema) VALUES (1, 28356,
'<?xml version="1.0" encoding="UTF-8"?>
<pc:PointCloudSchema xmlns:pc="http://pointcloud.org/schemas/PC/1.1"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <pc:dimension>
    <pc:position>1</pc:position>
    <pc:size>4</pc:size>
    <pc:description>X coordinate as a double. There is no need for
                    scale and offset information of the header.
    </pc:description>
    <pc:name>X</pc:name>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>2</pc:position>
    <pc:size>4</pc:size>
    <pc:description>Y coordinate as a double. There is no need for
                    scale and offset information of the header.
    </pc:description>
    <pc:name>Y</pc:name>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>3</pc:position>
    <pc:size>4</pc:size>
    <pc:description>Z coordinate as a double. There is no need for
                    scale and offset information of the header.
    </pc:description>
    <pc:name>Z</pc:name>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:metadata>
    <Metadata name="compression">dimensional</Metadata>
  </pc:metadata>
</pc:PointCloudSchema>');


-- Creating table
DROP TABLE IF EXISTS small_pcpatch CASCADE;
CREATE TABLE small_pcpatch(
	id SERIAL PRIMARY KEY,
	pa PCPATCH
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

INSERT INTO small_pcpatch (pa) 
SELECT PC_Patch(T.pt) FROM (
		SELECT PC_MakePoint(1, ARRAY[x*0.2+336000, y*0.2+6245250, z*0.2+20]) AS pt, 
		row_number() OVER (order by id) AS n 
		FROM tmp
	) AS T 
	GROUP BY ((n-1)/5000)
	ORDER BY ((n-1)/5000)
;

DROP TABLE tmp;

-- Creating index
DROP INDEX IF EXISTS small_pcpatch_xy_idx CASCADE;
CREATE INDEX small_pcpatch_xy_idx on small_pcpatch USING GIST (geometry(pa));

VACUUM FULL ANALYZE small_pcpatch;


