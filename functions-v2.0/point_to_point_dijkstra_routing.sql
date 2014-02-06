-------------------------------------------------------------------
--Tested with Postgresql8.4, postgis1.5, pgrouting1.05 for 9.1
-------------------------------------------------------------------

-------------------------------------------------------------------
--prerequisites for using the functions written in this file
--tbl should have the following columns
--start_id
--end_id
--the_geom
--osm_name
--speed_in_kmh
--length 
--reverse_length
--row_flag-default value = false
-------------------------------------------------------------------


--CREATE TYPE path_result AS (vertex_id integer, edge_id integer, cost float8);

-------------------------------------------------------------------
-- This function delete all the virtual edge and the 
-- tbl - table name return to oroginal format
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cleanup_virtual_values_in_table(tbl varchar) 
	RETURNS void AS 
	$$
    DECLARE
         row record;
         
    BEGIN
    	
        EXECUTE
	        'DELETE FROM '||tbl||' where row_flag = true';
    END;
$$ 
LANGUAGE 'plpgsql' VOLATILE STRICT;
-------------------------------------------------------------------
-- This function calculates shortest path between two point geometry along the edges specified in 
-- tbl - table name 
-------------------------------------------------------------------


CREATE OR REPLACE FUNCTION point_to_point_shortest_path(source_point_geom varchar, target_point_geom varchar, tbl varchar, distance double precision, directed boolean, has_reverse_cost boolean) 
	 RETURNS SETOF path_result AS
	$$
    DECLARE
    	 sql text;
         row record;
         result path_result%rowtype;
         srid int;
         closest_linestring_id_from_source_geom int;
         closest_linestring_id_from_target_geom int;
         fraction_at_which_source_geom_on_closest_line_substring double precision;
         fraction_at_which_target_geom_on_closest_line_substring double precision;
         first_part_road_geom_where_source_geom_snapped text;
         first_part_road_geom_where_source_geom_snapped_geom geometry;
         second_part_road_geom_where_source_geom_snapped text;
         second_part_road_geom_where_source_geom_snapped_geom geometry;
         first_part_road_geom_where_target_geom_snapped text;
         first_part_road_geom_where_target_geom_snapped_geom geometry;
         second_part_road_geom_where_target_geom_snapped text;
         second_part_road_geom_where_target_geom_snapped_geom geometry;
         start_id int;
         end_id int;
         virtual_start_id int;
         virtual_gid int;
         virtual_end_id int;
         is_source_edge_unidirectional int;
         is_target_edge_unidirectional int;
         vertex_id integer; 
         edge_id integer; 
         cost float8;
         
         
    BEGIN
    
		EXECUTE 'select * from cleanup_virtual_values_in_table('''||tbl||''')';
    
		EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' INTO srid;
		
		EXECUTE 'select fetch_closest_edge_within_distance_xy('''||source_point_geom||''', '||distance||', '''||tbl||''') as gid' INTO closest_linestring_id_from_source_geom;
		     
		EXECUTE 'select fetch_closest_edge_within_distance_xy('''||target_point_geom||''', '||distance||', '''||tbl||''') as gid' INTO closest_linestring_id_from_target_geom;
		
		EXECUTE 'select fetch_snapped_loc_fraction_of_point_on_edge('||closest_linestring_id_from_source_geom||','''||source_point_geom||''', '''||tbl||''') as fraction'  INTO  fraction_at_which_source_geom_on_closest_line_substring;
	 
		EXECUTE 'select fetch_snapped_loc_fraction_of_point_on_edge('||closest_linestring_id_from_target_geom||','''||target_point_geom||''', '''||tbl||''') as fraction' INTO fraction_at_which_target_geom_on_closest_line_substring;
		
		EXECUTE 'select fetch_first_half_of_the_geom_based_on_a_fraction('||closest_linestring_id_from_source_geom||', '||fraction_at_which_source_geom_on_closest_line_substring||', '''||tbl||''') as the_geom' INTO first_part_road_geom_where_source_geom_snapped;
	 
		EXECUTE 'select GeometryFromText('''||first_part_road_geom_where_source_geom_snapped||''', '||srid||') as geom' INTO first_part_road_geom_where_source_geom_snapped_geom;
	 
		EXECUTE 'select fetch_second_half_of_the_geom_based_on_a_fraction('||closest_linestring_id_from_source_geom||', '||fraction_at_which_source_geom_on_closest_line_substring||', '''||tbl||''') as the_geom' INTO second_part_road_geom_where_source_geom_snapped;
	 
		EXECUTE 'select GeometryFromText('''||second_part_road_geom_where_source_geom_snapped||''', '||srid||') as geom' INTO second_part_road_geom_where_source_geom_snapped_geom;
	 
		EXECUTE 'select fetch_first_half_of_the_geom_based_on_a_fraction('||closest_linestring_id_from_target_geom||', '||fraction_at_which_target_geom_on_closest_line_substring||', '''||tbl||''') as the_geom' INTO first_part_road_geom_where_target_geom_snapped;
		
		EXECUTE 'select GeometryFromText('''||first_part_road_geom_where_target_geom_snapped||''', '||srid||') as geom' INTO first_part_road_geom_where_target_geom_snapped_geom;
	 	
		EXECUTE 'select fetch_second_half_of_the_geom_based_on_a_fraction('||closest_linestring_id_from_target_geom||', '||fraction_at_which_target_geom_on_closest_line_substring||', '''||tbl||''') as the_geom' INTO second_part_road_geom_where_target_geom_snapped;
		
		EXECUTE 'select GeometryFromText('''||second_part_road_geom_where_target_geom_snapped||''', '||srid||') as geom' INTO second_part_road_geom_where_target_geom_snapped_geom;
		
		EXECUTE 'select has_reverse_length('''||tbl||''', '||closest_linestring_id_from_source_geom||')  as val' INTO is_source_edge_unidirectional;
	 
		EXECUTE 'select has_reverse_length('''||tbl||''', '||closest_linestring_id_from_target_geom||')  as val' INTO is_target_edge_unidirectional;

	--virtual segments creation of the source point geom
		EXECUTE 'select fetch_start_id_of_an_edge('||closest_linestring_id_from_source_geom||', '''||tbl||''') as start_id' INTO start_id;
		
		EXECUTE 'select fetch_end_id_of_an_edge('||closest_linestring_id_from_source_geom||', '''||tbl||''') as end_id' INTO end_id;
		
		EXECUTE 'select fetch_virtual_start_id_or_end_id('''||tbl||''') as virtual_start_id' INTO virtual_start_id;
		
		EXECUTE 'select fetch_virtual_gid('''||tbl||''') as virtual_gid' INTO virtual_gid;
	 
	 --update first part of the road for source
	IF is_source_edge_unidirectional = 1  THEN
		EXECUTE 'select create_update_virtual_edge('||virtual_gid||','||closest_linestring_id_from_source_geom||', '''||first_part_road_geom_where_source_geom_snapped||''', '||start_id||', '||virtual_start_id||','''||tbl||''','||is_source_edge_unidirectional||' )';
	ELSE
		EXECUTE 'select create_update_virtual_edge('||virtual_gid||','||closest_linestring_id_from_source_geom||', '''||first_part_road_geom_where_source_geom_snapped||''', '||virtual_start_id||', '||start_id||','''||tbl||''', '||is_source_edge_unidirectional||')';
	END IF;
	 
	 --update second part of the road for source
		EXECUTE 'select fetch_virtual_gid('''||tbl||''') as virtual_gid' INTO virtual_gid;
		
		EXECUTE 'select create_update_virtual_edge('||virtual_gid||','||closest_linestring_id_from_source_geom||', '''||second_part_road_geom_where_source_geom_snapped||''', '||virtual_start_id||', '||end_id||','''||tbl||''', '||is_source_edge_unidirectional||')';
	
	
	 --virtual segments creation of the target point geom
	 
		EXECUTE 'select fetch_start_id_of_an_edge('||closest_linestring_id_from_target_geom||', '''||tbl||''') as start_id' INTO start_id;

		EXECUTE 'select fetch_end_id_of_an_edge('||closest_linestring_id_from_target_geom||', '''||tbl||''') as end_id' INTO end_id;

		EXECUTE 'select fetch_virtual_start_id_or_end_id('''||tbl||''') as virtual_end_id' INTO virtual_end_id;

		EXECUTE 'select fetch_virtual_gid('''||tbl||''') as virtual_gid' INTO virtual_gid;

	 --update first part of the road for target
		EXECUTE 'select create_update_virtual_edge('||virtual_gid||','||closest_linestring_id_from_target_geom||', '''||first_part_road_geom_where_target_geom_snapped||''', '||start_id||', '||virtual_end_id||','''||tbl||''', '||is_target_edge_unidirectional||')';
          
		EXECUTE 'select fetch_virtual_gid('''||tbl||''') as virtual_gid' INTO virtual_gid;
         
	--update second part of the road for target	 
    IF is_target_edge_unidirectional = 1  THEN
		EXECUTE 'select create_update_virtual_edge('||virtual_gid||','||closest_linestring_id_from_target_geom||', '''||second_part_road_geom_where_target_geom_snapped||''', '||virtual_end_id||', '||end_id||','''||tbl||''', '||is_target_edge_unidirectional||')';
	ELSE
		EXECUTE 'select create_update_virtual_edge('||virtual_gid||','||closest_linestring_id_from_target_geom||', '''||second_part_road_geom_where_target_geom_snapped||''', '||end_id||','||virtual_end_id||', '''||tbl||''', '||is_target_edge_unidirectional||')';
	END IF;
	
 
	FOR result IN EXECUTE 'SELECT * FROM '||
						  'shortest_path(''SELECT gid AS id, 
								  start_id::int4 AS source, 
								  end_id::int4 AS target, 
								  length::float8 AS cost,
								  reverse_length::float8 as reverse_cost
								  FROM '||tbl||''',
								 '||virtual_start_id||',
								'||virtual_end_id||',
								'||directed||',
								'||has_reverse_cost||')'||''
								LOOP
									
		 	 						  RETURN NEXT result;
		 	 	 				      END LOOP;
		 	 	
					      RETURN;
	 
	 
        
    END;
$$ 
LANGUAGE 'plpgsql' VOLATILE STRICT;
-------------------------------------------------------------------
-- This function will check whether a row have a reverse_length column in
-- tbl - table name return to oroginal format
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION has_reverse_length(tbl varchar, gid int) 
	RETURNS int AS 
	$$
    DECLARE
         row record;
         rownum int;
         
    BEGIN
       rownum:= 0;
       FOR row IN EXECUTE 'SELECT true as unidirectional FROM '||tbl||' WHERE gid = '||gid||' AND reverse_length = 1000000.0' LOOP
       rownum:=rownum+1;
       END LOOP;
       
       Return rownum;
    END;
$$ 
LANGUAGE 'plpgsql' VOLATILE STRICT;

-------------------------------------------------------------------
-- This function insert a virtual edge values in table
-- tbl - table name 
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_update_virtual_edge(gid int, edge_id int, virtual_geom varchar, start_id int, end_id int, tbl varchar, is_unidirectional int) 
	RETURNS void AS 
	$$
    DECLARE
         row record;
         osm_name varchar;
         speed_in_kmh int;
         srid int;
         length double precision;
         reverse_length double precision;
         geom geometry;
         
    BEGIN
    
		EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' INTO srid;
    
    	EXECUTE 'select osm_name as osm_name from '||tbl||' where gid = '||edge_id||'' INTO osm_name;
	
		EXECUTE 'select speed_in_kmh as speed_in_kmh from '||tbl||' where gid = '||edge_id||'' INTO speed_in_kmh;
	
		EXECUTE 'select ST_Length(GeometryFromText('''||virtual_geom||''', '||srid||')) as length' INTO length;
		
		EXECUTE 'select GeometryFromText('''||virtual_geom||''', '||srid||') as virtual_multigeom' INTO geom;
	
	IF is_unidirectional THEN
		reverse_length:= 100000;
	elSE
		reverse_length:= length;
	END IF;
	
        EXECUTE 'INSERT INTO '||tbl||' (gid, the_geom, start_id, end_id, osm_name, speed_in_kmh, length, row_flag, reverse_length) VALUES ('||gid||', ST_GeomFromText('''||virtual_geom||''', '||srid||') ,'||start_id||','||end_id||', '''||osm_name||''', '||speed_in_kmh||', '||length||', true, '||reverse_length||')';
        
    END;
$$ 
LANGUAGE 'plpgsql' VOLATILE STRICT;
-------------------------------------------------------------------
-- This function fetch the max of gid in
-- tbl - table name 
-- and increment it by 1
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fetch_virtual_gid(tbl varchar)
	RETURNS INT AS
$$
DECLARE
    row record;
	gid int;
    
BEGIN
	
    -- get start id of specified edge

		EXECUTE 'select (max(gid)+1) as gid FROM '||tbl||'' INTO gid;

    IF gid IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN gid;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
-------------------------------------------------------------------
-- This function fetch the max +1  of start_id and end_id column values in
-- tbl - table name 
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fetch_virtual_start_id_or_end_id(tbl varchar)
	RETURNS INT AS
$$
DECLARE
    row record;
    maxVal int;
BEGIN
	
    -- get start id of specified edge

		EXECUTE 'SELECT GREATEST(max(start_id), max(end_id))+1 as maxVal FROM '||tbl||'' INTO maxVal;

    IF maxVal IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN maxVal;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

-------------------------------------------------------------------
-- This function fetch the end id of a specified edge
-- edge_id - existing in 
-- tbl - table name 
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fetch_end_id_of_an_edge(edge_id int, tbl varchar)
	RETURNS INT AS
$$
DECLARE
    row record;
    end_id int;
BEGIN
	
    -- get start id of specified edge

		EXECUTE 'SELECT end_id as end_id FROM '||tbl||' WHERE gid = '||edge_id||'' INTO end_id;

    IF end_id IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN end_id;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
-------------------------------------------------------------------
-- This function fetch the start id of a specified edge
-- edge_id - existing in 
-- tbl - table name 
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fetch_start_id_of_an_edge(edge_id int, tbl varchar)
	RETURNS INT AS
$$
DECLARE
    row record;
    start_id int;
BEGIN
	
    -- get start id of specified edge

		EXECUTE 'SELECT start_id as start_id FROM '||tbl||' WHERE gid = '||edge_id||'' INTO start_id;

    IF start_id IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN start_id;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

-------------------------------------------------------------------
-- This function gets the second half of the geom of an edge with
-- edge_id - existing in 
-- tbl - table name at which 
-- fraction - divides the edge
-------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fetch_second_half_of_the_geom_based_on_a_fraction(edge_id int, fraction double precision, tbl varchar)
	RETURNS text AS
$$
DECLARE
    row record;
    srid integer;
	geom_text text;
    
BEGIN

		EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' INTO srid;
	
    -- getting the fraction at which a point lie on an edge

		EXECUTE 'SELECT ST_AsText(ST_GeomFromEWKT(ST_Line_Substring(the_geom, '||fraction||', 1))) as geom_text
						  FROM '||tbl||'
						  WHERE gid = '||edge_id||'' INTO geom_text;

    IF geom_text IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN geom_text;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

-------------------------------------------------------------------
-- This function gets the first half of the geom of an edge with
-- edge_id - existing in 
-- tbl - table name at which 
-- fraction - divides the edge
-------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fetch_first_half_of_the_geom_based_on_a_fraction(edge_id int, fraction double precision, tbl varchar)
	RETURNS text AS
$$
DECLARE
    row record;
    srid integer;
	geom_text text;
    
BEGIN

		EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' INTO srid;
	
    -- getting the fraction at which a point lie on an edge

		EXECUTE 'SELECT ST_AsText(ST_GeomFromEWKT(ST_Line_Substring(the_geom, 0, '||fraction||'))) as geom_text
						  FROM '||tbl||'
						  WHERE gid = '||edge_id||'' INTO geom_text;

    IF geom_text IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN geom_text;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

-------------------------------------------------------------------
-- This function finds the fraction at which a node
-- point - text representation of point lie on an edge with an 
-- edge_id - existing in a 
-- tbl - table name
-------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fetch_snapped_loc_fraction_of_point_on_edge(edge_id int, point varchar, tbl varchar)
	RETURNS double precision AS
$$
DECLARE
    row record;
    srid integer;
    snapped_fraction double precision;
BEGIN

		EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' INTO srid;
	
    -- getting the fraction at which a point lie on an edge

		EXECUTE 'SELECT ST_Line_Locate_Point(ST_LineMerge(the_geom),GeometryFromText('''||point||''', '||srid||')) as snapped_fraction
						  FROM 
						  '||tbl||'
						  WHERE gid = '||edge_id||'' INTO snapped_fraction;

    IF snapped_fraction IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN snapped_fraction;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;


-------------------------------------------------------------------
-- This function finds nearest edge to a given node
-- point - text representation of point
-- distance - function will search for an edge within this distance
-- tbl - table name
-------------------------------------------------------------------


CREATE OR REPLACE FUNCTION fetch_closest_edge_within_distance_xy(point varchar,  distance double precision, tbl varchar)
	RETURNS INT AS
$$
DECLARE
    row record;
    srid integer;
    gid int;
BEGIN

		EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' INTO srid;
	
    -- Searching for a link within the distance

		EXECUTE 'SELECT t1.gid  FROM  '||tbl||' as t1 WHERE
						 ST_Expand(GeometryFromText('''||point||''', '||srid||'),'||distance||') && t1.the_geom 
						 ORDER BY
						 ST_Distance(GeometryFromText('''||point||''', '||srid||'),t1.the_geom)
						 ASC LIMIT 1' INTO gid;

    IF gid IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN gid;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;



