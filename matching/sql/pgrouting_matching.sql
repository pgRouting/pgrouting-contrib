-------------------------------------------------------------------------------
-- pgRouting Contrib
-- Copyright(c) pgRouting Contributors
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy 
-- of this software and associated documentation files (the "Software"), to 
-- deal in the Software without restriction, including without limitation the 
-- rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
-- sell copies of the Software, and to permit persons to whom the Software is 
-- furnished to do so, subject to the following conditions:
-- 
-- The above copyright notice and this permission notice shall be included in 
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
-- FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
-- IN THE SOFTWARE.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- This function finds nearest link to a given node
-- point - text representation of point
-- distance - function will search for a link within this distance
-- tbl - table name
-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgr_findNearestLinkDwithin(point varchar, distance double precision, tbl varchar)
    RETURNS integer AS
$$
DECLARE
    row record;
    x float8;
    y float8;
    srid integer;
    
BEGIN
    EXECUTE 'SELECT ST_SRID(the_geom) AS srid from ' || tbl || ' WHERE gid = (SELECT min(gid) FROM ' || tbl || ')' INTO row;
    srid:= row.srid;
    
    -- Getting x and y of the point
    EXECUTE 'SELECT ST_X(ST_GeometryFromText(''' || point ||''', ' || srid || ')) AS x' INTO row;
    x:=row.x;

    EXECUTE 'SELECT ST_Y(ST_GeometryFromText(''' || point || ''', ' || srid || ')) AS y' INTO row;
    y:=row.y;

    -- Searching for a link within the distance
    EXECUTE 'SELECT gid, ST_Distance(the_geom, ST_GeometryFromText(''' || point || ''', ' || srid || ')) AS dist FROM ' || tbl || ' WHERE ST_SetSRID(''BOX3D(' || x-distance || ' ' || y-distance || ', ' || x+distance || ' ' || y+distance || ')''::BOX3D, ' || srid || ')&&the_geom ORDER BY dist ASC LIMIT 1' INTO row;

    IF row.gid IS NULL THEN
        --RAISE EXCEPTION 'Data cannot be matched';
        RETURN NULL;
    END IF;

    RETURN row.gid;
END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

-------------------------------------------------------------------------------
-- This function finds nearest node to a given node
-- point - text representation of point
-- distance - function will search for a link within this distance
-- tbl - table name
-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgr_findNearestNodeDwithin(point varchar, distance double precision, tbl varchar)
    RETURNS integer AS
$$
DECLARE
    row record;
    x float8;
    y float8;
    d1 double precision;
    d2 double precision;
    d  double precision;
    field varchar;
    node integer;
    source integer;
    target integer;
    srid integer;
    
BEGIN

    EXECUTE 'SELECT ST_SRID(the_geom) AS srid FROM ' || tbl || ' WHERE gid = (SELECT min(gid) FROM ' || tbl || ')' INTO row;
    srid:= row.srid;

    -- Getting x and y of the point
    EXECUTE 'SELECT ST_X(ST_GeometryFromText(''' || point || ''', ' || srid || ')) AS x' INTO row;
    x:=row.x;

    EXECUTE 'SELECT ST_Y(ST_GeometryFromText(''' || point || ''', ' || srid ||')) AS y' INTO row;
    y:=row.y;

    -- Getting nearest source

    EXECUTE 'SELECT source, ST_Distance(ST_StartPoint(the_geom), ' || 'ST_GeometryFromText(''' || point || ''', ' || srid || ')) AS dist FROM ' || tbl || ' WHERE ST_SetSRID(''BOX3D(' || x-distance || ' ' || y-distance || ', ' || x+distance || ' ' || y+distance || ')''::BOX3D, ' || srid || ')&&the_geom ORDER BY dist ASC LIMIT 1' INTO row;
    
    d1:=row.dist;
    source:=row.source;

    -- Getting nearest target
    EXECUTE 'SELECT target, ST_Distance(ST_EndPoint(the_geom), ' || 'ST_GeometryFromText(''' || point || ''', ' || srid || ')) AS dist FROM ' || tbl || ' WHERE ST_SetSRID(''BOX3D(' || x-distance || ' ' || y-distance || ', ' || x+distance || ' ' || y+distance || ')''::BOX3D, ' || srid || ')&&the_geom ORDER BY dist ASC LIMIT 1' INTO row;

    -- Checking what is nearer - source or target
    d2:=row.dist;
    target:=row.target;
    IF d1<d2 THEN
        node:=source;
        d:=d1;
    ELSE
        node:=target;
        d:=d2;
    END IF;

    IF d=NULL OR d>distance THEN
        node:=NULL;
    END IF;

    RETURN node;
END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

-------------------------------------------------------------------------------
-- This function finds nearest node as a source or target of the
-- nearest link
-- point - text representation of point
-- distance - function will search for a link within this distance
-- tbl - table name
-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgr_findNodeByNearestLinkDwithin(point varchar, distance double precision, tbl varchar, OUT id integer, OUT name varchar)
AS
$$
DECLARE
    row record;
    link integer;
    d1 double precision;
    d2 double precision;
    field varchar;
    srid integer;
BEGIN

    EXECUTE 'SELECT ST_SRID(the_geom) AS srid FROM ' || tbl || ' WHERE gid = (SELECT min(gid) FROM '||tbl||')' INTO row;
    srid:= row.srid;

    -- Searching for a nearest link    
    EXECUTE 'SELECT id FROM pgr_findNearestLinkDwithin(''' || point || ''', ' || distance || ', ''' || tbl || ''') AS id' INTO row;
    IF row.id is null THEN
        id := -1;
        RETURN;
    END IF;
    link:=row.id;

    -- Check what is nearer - source or target
    EXECUTE 'SELECT ST_Distance((SELECT ST_StartPoint(the_geom) FROM ' || tbl || ' WHERE gid=' || link || '), ST_GeometryFromText(''' || point || ''', ' || srid || ')) AS dist' INTO row;
    d1:=row.dist;

    EXECUTE 'SELECT ST_Distance((SELECT ST_EndPoint(the_geom) FROM '|| tbl || ' WHERE gid=' || link || '), ST_GeometryFromText(''' || point || ''', ' || srid || ')) AS dist' INTO row;
    d2:=row.dist;

    IF d1<d2 THEN
        field:='source';
    ELSE
        field:='target';
    END IF;
    
    EXECUTE 'SELECT ' || field || ' AS id, ''' || field || ''' AS f FROM ' || tbl || ' WHERE gid=' || link INTO row;
        
    id := row.id;
    name := row.f;
    
    RETURN;
END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

-------------------------------------------------------------------------------
-- This function matches given line to the existing network.
-- Returns set of edges.
-- tbl - table name
-- line - line to match
-- distance - distance for nearest node search
-- distance2 - distance for shortest path search
-- dir - true if your network graph is directed
-- rc - true if you have a reverse_cost column
-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgr_matchLine(tbl varchar, line geometry, distance double precision, distance2 double precision, dir boolean, rc boolean)
    RETURNS SETOF pgr_costResult AS
$$
DECLARE
    row record;
    num integer;
    i integer;
    j integer;
    z integer;
    t integer;
    prev integer;
    query text;
    path pgr_costResult;
    edges integer[];
    vertices integer[];
    costs double precision[];
    srid integer;
    points integer[];

BEGIN

    EXECUTE 'SELECT ST_SRID(the_geom) AS srid FROM ' || tbl || ' WHERE gid = (SELECT min(gid) FROM ' || tbl || ')' INTO row;
    srid:= row.srid;

    EXECUTE 'SELECT geometryType(ST_GeometryFromText(''' || astext(line) || ''', ' || srid || ')) AS type' INTO row;
    
    IF row.type <> 'LINESTRING' THEN
        RAISE EXCEPTION 'Geometry should be a linestring.';
    END IF;

    num:=ST_NumPoints(line);
    i:= 0;
    z:= 0;
    prev := -1;
        
    -- Searching through all points in given line
    LOOP
        i:=i+1;

        -- Getting nearest node to the current point
        EXECUTE 'SELECT * FROM pgr_findNearestNodeDwithin(''POINT(' || ST_X(ST_PointN(line, i)) || ' ' || ST_Y(ST_PointN(line, i)) || ')'',' || distance || ', ''' || tbl || ''') AS id' INTO row;

        IF row.id IS NOT NULL THEN
            points[i-1]:=row.id;
        ELSE 
            -- If there is no nearest node within given distance,
            -- let's try another algorithm
            EXECUTE 'SELECT * FROM pgr_findNodeByNearestLinkDwithin(''POINT(' || ST_X(ST_PointN(line, i)) || ' ' ||
                ST_Y(ST_PointN(line, i)) || ')'',' || distance2 || ', ''' || tbl || ''') AS id' INTO row;

            points[i-1]:=row.id;
            IF row.id = -1 THEN
                return;
            END IF;

        END IF;

        IF i>1 AND points[i-2] <> points[i-1] THEN
            -- We could find existing edge, so let's construct the
            -- main query now
            query := 'SELECT * FROM ' ||
                'pgr_dijkstra(''SELECT gid as id, source::integer, target::integer, length::double precision AS cost, x1, x2, y1, y2';
                
            IF rc THEN
                query := query || ', reverse_cost'; 
            END IF;
        
            query := query || ' from ' || pgr_quote_ident(tbl) || ' WHERE ST_SetSRID(''''BOX3D(' || ST_X(ST_PointN(line, i-1))-distance2*2 || ' ' || ST_Y(ST_PointN(line, i-1))-distance2*2 || ', ' || ST_X(ST_PointN(line, i))+distance2*2 || ' ' || ST_Y(ST_PointN(line, i))+distance2*2 || ')''''::BOX3D, ' || srid || ')&&the_geom'', ' || points[i-1] || ', ' || points[i-2] || ', ''' || dir || ''', ''' || rc || ''')';

            BEGIN  -- EXCEPTION
        
            FOR row IN EXECUTE query LOOP
                IF row IS NULL THEN
                    RAISE NOTICE 'Cannot find a path between % and %',
                        points[i-1], points[i-2];
                    RETURN;
                END IF;

                vertices[z] := row.id1;
                edges[z]    := row.id2;
                costs[z]    := row.cost;

                IF edges[z] = -1 THEN
                    t := 0;
                    -- Ordering edges
                    FOR t IN (prev+1)..z-1 LOOP
                        path.id1  := vertices[t];
                        path.id2  := edges[t];
                        path.cost := costs[t];
                        
                        vertices[t] := vertices[z-t+prev+1];
                        edges[t] := edges[z-t+prev+1];
                        costs[t] := costs[z-t+prev+1];

                        vertices[z-t+prev+1] := path.id1;
                        edges[z-t+prev+1]    := path.id2;
                        costs[z-t+prev+1]    := path.cost;
                    END LOOP;
            
                    prev := z;
                END IF;    
        
                z := z+1;
        
            END LOOP;
        
            EXCEPTION
                WHEN containing_sql_not_permitted THEN RETURN;
            END;
        END IF;                                                            

        EXIT WHEN i=num;
    END LOOP;    

    FOR t IN 0..array_upper(edges, 1) LOOP
    
        IF edges[array_upper(edges, 1)-t] > 0
            OR (edges[array_upper(edges, 1)-t] < 0
                AND t = array_upper(edges, 1)) THEN

            path.seq  := t;
            path.id1  := vertices[array_upper(edges, 1)-t];
            path.id2  := edges[array_upper(edges, 1)-t];
            path.cost := costs[array_upper(edges, 1)-t];
            RETURN NEXT path;    
        END IF;
    END LOOP;
    
    RETURN;
END;
$$

LANGUAGE 'plpgsql' VOLATILE STRICT;
