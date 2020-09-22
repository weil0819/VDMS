/* ---------------------------------------------------- */
/*  Created On : 22-Sept-2020 	             			*/
/*  DBMS       : MongoDB    			     			*/
/*  Function   : Transfer geographical coordinates with */
/*               offset and translation into EPSG:4326  */
/*  Input      : tables in EPSG:28356                   */
/*  Output     : csv files in EPSG:4326                 */
/*  Usage      : ???             */
/*  Author     : Wesley      			     			*/
/* ---------------------------------------------------- */

/* If table coordinate is already in EPSG:28356 from client side */
\COPY (select ST_X(ST_Transform(geom,4326)) AS x, 
	ST_Y(ST_Transform(geom,4326)) AS y, 
	ST_Z(ST_Transform(geom,4326)) AS z 
	FROM voxelptnumeric) TO 'C:\Users\z5039792\Documents\Vox3DMod\MongoDB\voxelptnumeric.csv' DELIMITER ',' CSV HEADER;




