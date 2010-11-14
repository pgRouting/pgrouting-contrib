CREATE TYPE link_point AS (id integer, name varchar);
CREATE TYPE links AS (f geometry, l geometry);

CREATE TYPE edge AS ( gid integer,
			 target       integer,
			 source       integer,
			 x1           double precision,
			 y1           double precision,
			 x2           double precision, 
			 y2           double precision, 
			 length       double precision, 
			 reverse_cost double precision, 
--			 id           integer,
--			 class_id     smallint,
			 to_cost      double precision,
			 rule         text,
			 the_geom     geometry );

CREATE TYPE edge_array AS ( gid integer[6],
			 target       integer[6],
			 source       integer[6],
			 x1           double precision[6],
			 y1           double precision[6],
			 x2           double precision[6], 
			 y2           double precision[6], 
			 length       double precision[6], 
			 reverse_cost double precision[6], 
--			 id           integer[6],
--			 class_id     smallint[6],
			 to_cost      double precision[6],
			 rule         text[6],
			 the_geom     geometry[6] );


CREATE OR REPLACE FUNCTION add_network_info(src varchar) RETURNS void AS
$$
DECLARE
  m_gid integer;
  m_v_id integer;
  longest_l double precision;
  l_num integer;  
  
  r record;
  ex boolean;
  
BEGIN
  FOR r IN EXECUTE 'SELECT  count(*) as l_num, max(length(the_geom)) as longest_l, max(gid) AS max_gid, '''||src||''' as tname, greatest(max(source), max(target)) AS max_vertex_id FROM '||src
  LOOP
  END LOOP;
  
  m_gid := r.max_gid;
  m_v_id := r.max_vertex_id;
  longest_l := r.longest_l;
  l_num := r.l_num;
  
  select (select relname from pg_class where relname='network_info') is null INTO ex;
  
  IF ex THEN
  CREATE TABLE network_info(tname text, max_gid integer, max_vertex_id integer, longest_link_length double precision, link_num integer);
  END IF;
  
  EXECUTE 'DELETE FROM network_info where tname='''||src||'''';
  INSERT INTO network_info VALUES(src, m_gid, m_v_id, longest_l, l_num);
END;
$$
LANGUAGE 'plpgsql';

-------------------------------------------------------------------
-- This function finds nearest link to a given node
-- point - text representation of point
-- distance - function will search for a link within this distance
-- tbl - table name
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION find_nearest_link_within_distance(point varchar, 
	distance double precision, tbl varchar)
	RETURNS INT AS
$$
DECLARE
    row record;
    x float8;
    y float8;
    
    srid integer;
    
BEGIN

    FOR row IN EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' LOOP
    END LOOP;
	srid:= row.srid;
    
    -- Getting x and y of the point
    
    FOR row in EXECUTE 'select x(GeometryFromText('''||point||''', '||srid||')) as x' LOOP
    END LOOP;
	x:=row.x;

    FOR row in EXECUTE 'select y(GeometryFromText('''||point||''', '||srid||')) as y' LOOP
    END LOOP;
	y:=row.y;

    -- Searching for a link within the distance

    FOR row in EXECUTE 'select gid, distance(the_geom, GeometryFromText('''||point||''', '||srid||')) as dist from '||tbl||
			    ' where setsrid(''BOX3D('||x-distance||' '||y-distance||', '||x+distance||' '||y+distance||')''::BOX3D, '||srid||')&&the_geom order by dist asc limit 1'
    LOOP
    END LOOP;

    IF row.gid IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN row.gid;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

CREATE OR REPLACE FUNCTION find_nearest_link_within_distance_xy(x double precision, 
	y double precision, distance double precision, tbl varchar)
	RETURNS INT AS
$$
DECLARE
    row record;
        
    srid integer;
    
BEGIN

    FOR row IN EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' LOOP
    END LOOP;
	srid:= row.srid;
    
    -- Searching for a link within the distance

    FOR row in EXECUTE 'select gid, distance(the_geom, GeometryFromText(''POINT('||x||' '||y||')'', '||srid||')) as dist from '||tbl||
			    ' where setsrid(''BOX3D('||x-distance||' '||y-distance||', '||x+distance||' '||y+distance||')''::BOX3D, '||srid||')&&the_geom order by dist asc limit 1'
    LOOP
    END LOOP;

    IF row.gid IS NULL THEN
	    --RAISE EXCEPTION 'Data cannot be matched';
	    RETURN NULL;
    END IF;

    RETURN row.gid;

END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;


CREATE OR REPLACE FUNCTION locate_point_as_geometry(tbl varchar, edge integer, px double precision, py double precision, col boolean)
	RETURNS LINKS AS
$$
DECLARE
    row record;
    num integer;
    i integer;
    geom geoms;
    
    l links;
    pos double precision;
    
    srid integer;
    
BEGIN

    FOR row IN EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' LOOP
    END LOOP;
	srid:= row.srid;

--    RAISE NOTICE 'select * from line_locate_point((select linemerge(the_geom) from % where gid=%), geometryfromtext(''POINT(% %)'', %)) as pos', tbl, edge, px, py, srid;
	
    FOR row in EXECUTE 'select distinct * from line_locate_point((select distinct linemerge(the_geom) from '||tbl||' where gid='||edge||'), geometryfromtext(''POINT('
			    ||px||' '||py||')'', '||srid||')) as pos'
    LOOP
    END LOOP;
	
    pos:=row.pos;
    
    -- Creating new geometries
	
    FOR row in EXECUTE 'select distinct * from line_substring((select distinct linemerge(the_geom) from '||tbl||' where gid='||edge||'), 0, '||pos||') as link'
    LOOP
    END LOOP;
    
    l.f:=row.link;
    IF geometrytype(l.f) = 'POINT' THEN
      --RAISE NOTICE 'POINT >>> %', astext(l.f);
      l.f := geometryfromtext('LINESTRING('||x(l.f)||' '||y(l.f)||','||x(l.f)||' '||y(l.f)||')');
    END IF;
    
    IF col THEN l.f = collect(l.f);
    END IF;
	

    FOR row in EXECUTE 'select distinct * from line_substring((select distinct linemerge(the_geom) from '||tbl||' where gid='||edge||'), '||pos||', 1) as link'
    LOOP
    END LOOP;
	
    l.l:=row.link;
    IF geometrytype(l.l) = 'POINT' THEN
      --RAISE NOTICE 'POINT >>> %', astext(l.l);
      l.l := geometryfromtext('LINESTRING('||x(l.l) ||' '||y(l.l)||','||x(l.l)||' '||y(l.l)||')');
    END IF;

    IF col THEN l.l = collect(l.l);
    END IF;
    
    RETURN l;

END;
$$

LANGUAGE 'plpgsql' VOLATILE STRICT;


CREATE OR REPLACE FUNCTION get_middle(tbl varchar, edge integer, px1 double precision, py1 double precision, px2 double precision, py2 double precision, col boolean)
	RETURNS GEOMETRY AS
$$
DECLARE
    row record;
    num integer;
    i integer;
    geom geoms;
    
    l geometry;
    pos1 double precision;
    pos2 double precision;
    
    srid integer;
    query text;
    
BEGIN

    FOR row IN EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' LOOP
    END LOOP;
	srid:= row.srid;

--    RAISE NOTICE 'select * from line_locate_point((select linemerge(the_geom) from % where gid=%), geometryfromtext(''POINT(% %)'', %)) as pos', tbl, edge, px, py, srid;
	
    FOR row in EXECUTE 'select distinct * from line_locate_point((select distinct linemerge(the_geom) from '||tbl||' where gid='||edge||'), geometryfromtext(''POINT('
			    ||px1||' '||py1||')'', '||srid||')) as pos'
    LOOP
    END LOOP;	
    pos1:=row.pos;

    FOR row in EXECUTE 'select distinct * from line_locate_point((select distinct linemerge(the_geom) from '||tbl||' where gid='||edge||'), geometryfromtext(''POINT('
			    ||px2||' '||py2||')'', '||srid||')) as pos'
    LOOP
    END LOOP;	
    pos2:=row.pos;
    
    query := 'select line_substring((select distinct linemerge(the_geom) from '||tbl||' where gid='||edge||'), ';
    
    IF pos1 < pos2 THEN query:= query||pos1||', '||pos2;
    ELSE query:= query || pos2||', '||pos1;
    END IF;
    
    query := query ||') as link';
    
    -- Creating new geomety
	
    FOR row in EXECUTE query
    LOOP
    END LOOP;
    
    l:= row.link;
    
    IF col THEN l = collect(l);
    END IF;
    
    RETURN l;

END;
$$


LANGUAGE 'plpgsql' VOLATILE STRICT;


CREATE OR REPLACE FUNCTION connected_substring_as_geometry(tbl varchar, edge integer, next_edge integer, x double precision, y double precision)
	RETURNS GEOMETRY AS
$$
DECLARE
    row record;
    num integer;
    i integer;
    geom geoms;
    
    l geometry;
    l1 geometry;
    l2 geometry;
    pos double precision;
    
    cp geometry;
    
    pnt integer;
    
    srid integer;
    
    query text;
    
    cont boolean;
    
BEGIN

    FOR row IN EXECUTE 'select getsrid(the_geom) as srid from '||tbl||' where gid = (select min(gid) from '||tbl||')' LOOP
    END LOOP;
	srid:= row.srid;

--    FOR row in EXECUTE 'select * from intersection((select linemerge(the_geom) from '||tbl||' where gid='||edge||'), '||
--			'(select linemerge(the_geom) from '||tbl||' where gid='||next_edge||')) as cp'
--    LOOP
--    END LOOP;
    
    FOR row in EXECUTE 'select case when (select source from '||tbl||' where gid='||edge||')=(select source from '||tbl||
			' where gid='||next_edge||') then (select startpoint(the_geom) from '||tbl||' where gid='||next_edge||') when (select source from '
			||tbl||' where gid='||edge||')=(select target from '||tbl||' where gid='||next_edge||') then (select endpoint(the_geom) from '
			||tbl||' where gid='||next_edge||')  when (select target from '||tbl||' where gid='||edge||')=(select source from '
			||tbl||' where gid='||next_edge||') then (select startpoint(the_geom) from '||tbl||
			' where gid='||next_edge||') when (select target from '||tbl||' where gid='||edge||')=(select target from '||tbl||
			' where gid='||next_edge||') then (select endpoint(the_geom) from '||tbl||' where gid='||next_edge||') end as cp'

    LOOP
    END LOOP;

    cp:=row.cp;

    FOR row in EXECUTE 'select * from line_locate_point((select linemerge(the_geom) from '||tbl||' where gid='||edge||'), geometryfromtext(''POINT('
			    ||x||' '||y||')'', '||srid||')) as pos'
    LOOP
    END LOOP;
	
    pos:=row.pos;
    
--    FOR row IN EXECUTE 'select case when astext(startpoint(the_geom)) = '''||astext(cp)||''' then 0 else 1 end as pnt from '||tbl||' where gid='||edge
--    LOOP
--    END LOOP;
--    
--    pnt=row.pnt;

    -- Creating new geometries
	
    FOR row in EXECUTE 'select * from line_substring((select linemerge(the_geom) from '||tbl||' where gid='||edge||'), 0, '||pos||') as link'
    LOOP
    END LOOP;

    l1:=row.link;

    FOR row in EXECUTE 'select * from line_substring((select linemerge(the_geom) from '||tbl||' where gid='||edge||'), '||pos||', 1) as link'
    LOOP
    END LOOP;

    l2:=row.link;


    IF cp=startpoint(l1) OR cp=endpoint(l1) THEN
	l:=l1;
    ELSE
	l:=l2;
    END IF;
    
    RETURN l;

END;
$$

LANGUAGE 'plpgsql' VOLATILE STRICT;


CREATE OR REPLACE FUNCTION shootingstar_sp_smart(
       geom_table varchar, source_x float8, source_y float8, target_x float8, target_y float8, delta float8, cost_column varchar, dir boolean, rc boolean) 
       RETURNS SETOF GEOMS AS
$$
DECLARE
r record;
g geoms;
BEGIN
  FOR r IN EXECUTE 'SELECT id, gid, the_geom from shootingstar_sp_smart('''||geom_table||''', '||source_x||', '||source_y||', '||target_x||
                   ', '||target_y||', '||delta||', '''||cost_column||''', ''reverse_cost'', ''to_cost'', '||text(dir)||', '||text(rc)||')'
  LOOP
    g.id := r.id;
    g.gid := r.gid;
    g.the_geom := r.the_geom;
    RETURN NEXT g;
  END LOOP;
  
END;
$$

LANGUAGE 'plpgsql' VOLATILE STRICT;


CREATE OR REPLACE FUNCTION shootingstar_sp_smart(
       geom_table varchar, source_x float8, source_y float8, target_x float8, target_y float8, delta float8, cost_column varchar, reverse_cost_column varchar, dir boolean, rc boolean) 
       RETURNS SETOF GEOMS AS
$$
DECLARE
r record;
g geoms;
BEGIN
  FOR r IN EXECUTE 'SELECT id, gid, the_geom from shootingstar_sp_smart('''||geom_table||''', '||source_x||', '||source_y||', '||target_x||
                   ', '||target_y||', '||delta||', '''||cost_column||''', '''||reverse_cost_column||''', ''to_cost'', '||text(dir)||', '||text(rc)||')'
  LOOP
    g.id := r.id;
    g.gid := r.gid;
    g.the_geom := r.the_geom;
    RETURN NEXT g;
  END LOOP;
  
END;
$$

LANGUAGE 'plpgsql' VOLATILE STRICT;


CREATE OR REPLACE FUNCTION shootingstar_sp_smart(
       geom_table varchar, source_x float8, source_y float8, target_x float8, target_y float8, delta float8, cost_column varchar, reverse_cost_column varchar, to_cost_column varchar, dir boolean, rc boolean) 
       RETURNS SETOF GEOMS AS
$$
DECLARE 
        rec record;
        r record;
        path_result record;
        v_id integer;
        e_id integer;
        geom geoms;
	
	intersection text;

        srid integer;
	
	s_gid integer;
	t_gid integer;

	max_gid integer;
	max_vertex_id integer;
	
	l_pair links;
	middle geometry;

        ll_x float8;
        ll_y float8;
        ur_x float8;
        ur_y float8;

        query text;
	i integer;

        id integer;
	
	seqname text;
	
	source edge;
	target edge;
	
	curr edge;
	tmp edge;
	
	extra_edges edge_array;
BEGIN

        id :=0;
        FOR rec IN EXECUTE
            'select srid(the_geom) from ' ||
            quote_ident(geom_table) || ' limit 1'
        LOOP
        END LOOP;
        srid := rec.srid;

	FOR rec IN EXECUTE 'SELECT CASE WHEN '||source_x||'<'||target_x||
           ' THEN '||source_x||' ELSE '||target_x||
           ' END as ll_x, CASE WHEN '||source_x||'>'||target_x||
           ' THEN '||source_x||' ELSE '||target_x||' END as ur_x'
        LOOP
        END LOOP;

        ll_x := rec.ll_x;
        ur_x := rec.ur_x;

        FOR rec IN EXECUTE 'SELECT CASE WHEN '||source_y||'<'||
            target_y||' THEN '||source_y||' ELSE '||
            target_y||' END as ll_y, CASE WHEN '||
            source_y||'>'||target_y||' THEN '||
            source_y||' ELSE '||target_y||' END as ur_y'
        LOOP
        END LOOP;

        ll_y := rec.ll_y;
        ur_y := rec.ur_y;
	
	-- Searching for the source and target edges
	SELECT find_nearest_link_within_distance_xy(source_x, source_y, delta, geom_table) INTO s_gid;
	SELECT find_nearest_link_within_distance_xy(target_x, target_y, delta, geom_table) INTO t_gid;

--        RAISE NOTICE 'SELECT gid,source,target,x1,y1,x2,y2,length,reverse_cost,id,class_id,to_cost::double precision,rule,the_geom FROM % WHERE gid = %', quote_ident(geom_table), s_gid;
--        RAISE NOTICE 'SELECT gid,source,target,x1,y1,x2,y2,length,reverse_cost,id,class_id,to_cost::double precision,rule,the_geom FROM % WHERE gid = %', quote_ident(geom_table), t_gid;

        FOR rec IN EXECUTE 'SELECT gid,source,target,x1,y1,x2,y2,'||cost_column||' as length, '||reverse_cost_column||' as reverse_cost,'||to_cost_column||'||''.0'' as to_cost,rule,the_geom FROM ' || quote_ident(geom_table) || ' WHERE gid = ' || s_gid
	LOOP
	END LOOP;
	
        source.gid          := rec.gid;
	source.target       := rec.target;
	source.source       := rec.source;
	source.x1           := rec.x1;
	source.y1           := rec.y1;
	source.x2           := rec.x2;
	source.y2           := rec.y2;
	source.length       := rec.length;
	source.reverse_cost := rec.reverse_cost;
--	source.id           := rec.id;
--	source.class_id     := rec.class_id;
	source.to_cost      := rec.to_cost;
	
--	RAISE NOTICE 'source.to_cost = %', source.to_cost::double precision;
	
	source.rule         := rec.rule;
	source.the_geom     := rec.the_geom;

        FOR rec IN EXECUTE 'SELECT gid,source,target,x1,y1,x2,y2,'||cost_column||' as length, '||reverse_cost_column||' as reverse_cost,'||to_cost_column||' as to_cost,rule,the_geom FROM ' || quote_ident(geom_table) || ' WHERE gid = ' || t_gid
	LOOP
	END LOOP;
	
        target.gid          := rec.gid;
	target.target       := rec.target;
	target.source       := rec.source;
	target.x1           := rec.x1;
	target.y1           := rec.y1;
	target.x2           := rec.x2;
	target.y2           := rec.y2;
	target.length       := rec.length;
	target.reverse_cost := rec.reverse_cost;
--	target.id           := rec.id;
--	target.class_id     := rec.class_id;
	target.to_cost      := rec.to_cost;
	target.rule         := rec.rule;
	target.the_geom     := rec.the_geom;

--        FOR rec IN EXECUTE 'SELECT max(gid) AS max_gid, greatest(max(source), max(target)) AS max_vertex_id FROM ' 
--	  || quote_ident(geom_table) || ' where setSRID(''BOX3D('||
--          ll_x-delta||' '||ll_y-delta||','||ur_x+delta||' '||
--          ur_y+delta||')''::BOX3D, ' || srid || ') && the_geom'
--	LOOP
--	END LOOP;

        FOR rec IN EXECUTE 'SELECT max_gid, max_vertex_id FROM network_info WHERE tname = ''' || quote_ident(geom_table) || ''''
	LOOP
	END LOOP;
	
	max_gid:=rec.max_gid;
	max_vertex_id:=rec.max_vertex_id;
	
--	RAISE NOTICE 'max_gid=%, max_vertex_id=%', max_gid, max_vertex_id;

        -- Locate source and target points
	-- extra_edges[1] - source first
	-- extra_edges[2] - source last
        SELECT DISTINCT l, f FROM locate_point_as_geometry(geom_table, s_gid, source_x, source_y, true) INTO l_pair.l, l_pair.f;
        
	extra_edges.the_geom[1] := l_pair.f;
	extra_edges.gid[1] := max_gid+1;
	extra_edges.source[1] := source.source;
	-- New target vertex (max_vertex_id+1)
	extra_edges.target[1] := max_vertex_id+1;
	extra_edges.x1[1] := source.x1;
	extra_edges.y1[1] := source.y1;
	extra_edges.x2[1] := x(startpoint(l_pair.f));
	extra_edges.y2[1] := y(startpoint(l_pair.f));
--	extra_edges.x2[1] := x(PointN(l_pair.f, NumPoints(l_pair.f)));
--	extra_edges.y2[1] := y(PointN(l_pair.f, NumPoints(l_pair.f)));
	extra_edges.length[1] := source.length*(length(l_pair.f)/length(source.the_geom));
	extra_edges.reverse_cost[1] := source.reverse_cost*(length(l_pair.f)/length(source.the_geom));
--	extra_edges.id[1] := source.id;
--	extra_edges.class_id[1] := source.class_id;
--	extra_edges.to_cost[1] := source.to_cost;
	extra_edges.rule[1] := source.rule;
	--extra_edges[1]:=curr;
	
	extra_edges.the_geom[2] := l_pair.l;
	extra_edges.gid[2] := max_gid+2;
	extra_edges.target[2] := source.target;
	-- New target vertex (max_vertex_id+1)
	extra_edges.source[2] := max_vertex_id+1;
	extra_edges.x2[2] := source.x2;
	extra_edges.y2[2] := source.y2;
--	extra_edges.x1[2] := x(startpoint(l_pair.f));
--	extra_edges.y1[2] := y(startpoint(l_pair.f));
	extra_edges.x1[2] := x(PointN(l_pair.f, NumPoints(l_pair.f)));
	extra_edges.y1[2] := y(PointN(l_pair.f, NumPoints(l_pair.f)));
	extra_edges.length[2] := source.length*(length(l_pair.l)/length(source.the_geom));
	extra_edges.reverse_cost[2] := source.reverse_cost*(length(l_pair.l)/length(source.the_geom));
--	extra_edges.id[2] := source.id;
--	extra_edges.class_id[2] := source.class_id;
	extra_edges.to_cost[2] := source.to_cost;
	extra_edges.rule[2] := source.rule;
	--extra_edges[2]:=curr;
	
	-- extra_edges[3] - target first
	-- extra_edges[4] - target last
        SELECT DISTINCT l, f FROM locate_point_as_geometry(geom_table, t_gid, target_x, target_y, true) INTO l_pair.l, l_pair.f;
	extra_edges.the_geom[3] := l_pair.f;
	extra_edges.gid[3] := max_gid+3;
	extra_edges.source[3] := target.source;
	-- New target vertex (max_vertex_id+2)
	extra_edges.target[3] := max_vertex_id+2;
	extra_edges.x1[3] := target.x1;
	extra_edges.y1[3] := target.y1;
	extra_edges.x2[3] := x(endpoint(l_pair.f));
	extra_edges.y2[3] := y(endpoint(l_pair.f));
	extra_edges.length[3] := target.length*(length(l_pair.f)/length(target.the_geom));
	extra_edges.reverse_cost[3] := target.reverse_cost*(length(l_pair.f)/length(target.the_geom));
--	extra_edges.id[3] := target.id;
--	extra_edges.class_id[3] := target.class_id;
	extra_edges.to_cost[3] := target.to_cost;
	extra_edges.rule[3] := target.rule;
	--extra_edges[3]:=curr;

	extra_edges.the_geom[4] := l_pair.l;
	extra_edges.gid[4] := max_gid+4;
	extra_edges.target[4] := target.target;
	-- New target vertex (max_vertex_id+2)
	extra_edges.source[4] := max_vertex_id+2;
	extra_edges.x2[4] := target.x2;
	extra_edges.y2[4] := target.y2;
	extra_edges.x1[4] := x(endpoint(l_pair.f));
	extra_edges.y1[4] := y(endpoint(l_pair.f));
	extra_edges.length[4] := target.length*(length(l_pair.l)/length(target.the_geom));
	extra_edges.reverse_cost[4] := target.reverse_cost*(length(l_pair.l)/length(target.the_geom));
--	extra_edges.id[4] := target.id;
--	extra_edges.class_id[4] := target.class_id;
	extra_edges.to_cost[4] := target.to_cost;
	extra_edges.rule[4] := target.rule;
	--extra_edges[4]:=curr;

	-- extra_edges[5] - extra source edge
	-- extra_edges[6] - extra target edge

	--tmp := extra_edges[1];
	
	extra_edges.the_geom[5] := geometryfromtext('MULTILINESTRING(('||source_x||' '||source_y||','||extra_edges.x1[2]||' '||extra_edges.y1[2]||'))', srid);

--	RAISE NOTICE 'source.rule = %', source.rule;

	extra_edges.gid[5] := max_gid+5;
	-- New target vertex (max_vertex_id+3)
	extra_edges.source[5] := max_vertex_id+3;
	extra_edges.target[5] := extra_edges.target[1];
	extra_edges.x1[5] := source_x;
	extra_edges.y1[5] := source_y;
	extra_edges.x2[5] := extra_edges.x2[2];
	extra_edges.y2[5] := extra_edges.y2[2];
	extra_edges.length[5] := length(extra_edges.the_geom[5]);
	extra_edges.reverse_cost[5] := 1000000.0;
--	extra_edges.id[5] := 0;
--	extra_edges.class_id[5] := source.class_id;
--	RAISE NOTICE 'class_id[5] = %', extra_edges.class_id[5];
	extra_edges.to_cost[5] := NULL;--source.to_cost;
--	RAISE NOTICE 'to_cost[5] = %', extra_edges.to_cost[5];
	extra_edges.rule[5] := NULL;--source.rule;
--	RAISE NOTICE 'rule[5] = %', extra_edges.rule[5];
	--extra_edges[5]:=curr;

	--tmp := extra_edges[3];
	
	extra_edges.the_geom[6] := geometryfromtext('MULTILINESTRING(('||extra_edges.x2[3]||' '||extra_edges.y2[3]||','||target_x||' '||target_y||'))', srid);

	extra_edges.gid[6] := max_gid+6;
	
--	RAISE NOTICE 'the_geom[6] = %', extra_edges.the_geom[6];
	
	-- New target vertex (max_vertex_id+4)
	extra_edges.source[6] := max_vertex_id+4;
	extra_edges.target[6] := extra_edges.target[3];
	extra_edges.x2[6] := target_x;
	extra_edges.y2[6] := target_y;
	extra_edges.x1[6] := extra_edges.x2[3];
	extra_edges.y1[6] := extra_edges.y2[3];
	extra_edges.length[6] := length(extra_edges.the_geom[6]);
	extra_edges.reverse_cost[6] := 1000000.0;
--	extra_edges.id[6] := 0;
--	extra_edges.class_id[6] := target.class_id;
	extra_edges.to_cost[6] := target.to_cost;
	extra_edges.rule[6] := target.rule;
	--extra_edges[6]:=curr;

	select relname INTO seqname from pg_class where relname='rownum';
	
	IF seqname IS NOT NULL THEN
	EXECUTE 'drop sequence rownum';
	END IF;

	EXECUTE 'create sequence rownum';
	
        IF s_gid = t_gid THEN
	  
	  SELECT * FROM get_middle(geom_table, s_gid, source_x, source_y, target_x, target_y, true) INTO middle;
	  
	  geom.gid := extra_edges.gid[5];
          geom.the_geom := extra_edges.the_geom[5];
	  geom.id := 0;    
          RETURN NEXT geom;

	  geom.gid := extra_edges.gid[1];
          geom.the_geom := middle;
	  geom.id := 1;
          RETURN NEXT geom;

	  geom.gid := extra_edges.gid[6];
          geom.the_geom := extra_edges.the_geom[6];
	  geom.id := 2;
          RETURN NEXT geom;
	  
	  RETURN;

	END IF;
		
	
        -- Shooting* search query
	-- Need to search for new geometries in the array instead of the table
	query := 'select distinct a.rownum as id, a.edge_id, b.gid, b.the_geom from (select nextval(''rownum'') as rownum, edge_id from ' || 
          'shortest_path_shooting_star(''SELECT gid as id, source::integer, ' || 
          'target::integer, '||cost_column||'::double precision as cost, ' || 
          'x1::double precision, y1::double precision, x2::double ' ||
          'precision, y2::double precision, rule::varchar, ' ||
          to_cost_column||'::double precision as to_cost ';
          
        IF rc THEN query := query || ' , '||reverse_cost_column||'::double precision as reverse_cost ';  
        END IF;
          
        query := query || 'FROM ' || quote_ident(geom_table) || ' where setSRID(''''BOX3D('||
          ll_x-delta||' '||ll_y-delta||','||ur_x+delta||' '||
          ur_y+delta||')''''::BOX3D, ' || srid || ') && the_geom';

--	RAISE NOTICE 'Query: %', query;

	-- Newly created edges should be appended here
        FOR i IN 1..6 LOOP
	  --curr := extra_edges[i];
--	  RAISE NOTICE 'i=%', i;
	  IF extra_edges.rule[i] IS NULL THEN extra_edges.rule[i]:='NULL';
	  ELSE extra_edges.rule[i]:=''''''||extra_edges.rule[i]||'''''';
	  END IF;
	  IF extra_edges.to_cost[i] IS NULL THEN extra_edges.to_cost[i]:=0;
	  END IF;
	  
	  query := query || ' UNION ALL SELECT ' || extra_edges.gid[i]::integer || ', ' || extra_edges.source[i]::integer ||
	           ', ' || extra_edges.target[i]::integer || ', ' || extra_edges.length[i]::double precision ||
	           ', ' || extra_edges.x1[i]::double precision || ', ' || extra_edges.y1[i]::double precision ||
	           ', ' || extra_edges.x2[i]::double precision || ', ' || extra_edges.y2[i]::double precision ||
	           ', ' || extra_edges.rule[i]::varchar || ', ' || extra_edges.to_cost[i]::double precision;
                   IF rc THEN query := query || ' , ' || extra_edges.reverse_cost[i]::double precision;
                   END IF;
--	RAISE NOTICE 'Query: %', query;
	END LOOP;
	
        -- Need to use new ids as source and target

	  --curr := extra_edges[5];
	  --tmp := extra_edges[6];

--	RAISE NOTICE 'Query: %', query;

	query := query || ' ORDER BY id'', ' || 	  
          quote_literal(extra_edges.gid[5]) || ' , ' || 
          quote_literal(extra_edges.gid[6]) || ' , '''||text(dir)||''', '''||text(rc)||''' ) ) a LEFT JOIN ' || 
          quote_ident(geom_table) || ' b ON (a.edge_id=b.gid) ';
          
	--RAISE NOTICE 'max_gid: %, max_vertex_id: %', max_gid, max_vertex_id;
--	RAISE NOTICE 'Query: %', query;
	
        --geom.gid := extra_edges.gid[5];
        --geom.the_geom := extra_edges.the_geom[5];
	--geom.id := 0;
        
        --RETURN NEXT geom;
        
        FOR path_result IN EXECUTE query
        LOOP
                 geom.gid      := path_result.edge_id;
--	         RAISE NOTICE ' -- gid: %', geom.gid;
	         
		 -- Need to search for new geometries in the array instead of the table
		 IF geom.gid > max_gid THEN
		   --curr := extra_edges[geom.gid-max_gid];
		   geom.the_geom := extra_edges.the_geom[geom.gid-max_gid];
		 ELSE
                   geom.the_geom := path_result.the_geom;
		 END IF;
                 
		 id := id+1;
--                 geom.id       := id;
                 geom.id       := path_result.id;
                 
                 RETURN NEXT geom;

        END LOOP;
        
        RETURN;
END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT; 


CREATE OR REPLACE FUNCTION sp_smart_directed(
       geom_table varchar, heuristic boolean, source_x float8, source_y float8, target_x float8, target_y float8, 
       delta float8, cost_column varchar, reverse_cost_column varchar, dir boolean, rc boolean) 
       RETURNS SETOF GEOMS AS
$$
DECLARE 
        rec record;
        r record;
        path_result record;
        v_id integer;
        e_id integer;
        geom geoms;

        srid integer;
	
	s_gid integer;
	t_gid integer;

	max_gid integer;
	max_vertex_id integer;
	
	l_pair links;
	middle geometry;

        ll_x float8;
        ll_y float8;
        ur_x float8;
        ur_y float8;

        query text;
	i integer;
	
	fname text;
	seqname text;

        id integer;
	
	source edge;
	target edge;
	
	curr edge;
	tmp edge;
	
	extra_edges edge_array;
BEGIN

        id :=0;
	
	IF heuristic THEN fname = 'shortest_path_astar';
	ELSE fname = 'shortest_path';
	END IF;
	
	
        FOR rec IN EXECUTE
            'select srid from geometry_columns where f_table_name= ''' ||
            quote_ident(geom_table)||''''
        LOOP
        END LOOP;
        srid := rec.srid;

--	RAISE NOTICE 'SRID is set';


	FOR rec IN EXECUTE 'SELECT CASE WHEN '||source_x||'<'||target_x||
           ' THEN '||source_x||' ELSE '||target_x||
           ' END as ll_x, CASE WHEN '||source_x||'>'||target_x||
           ' THEN '||source_x||' ELSE '||target_x||' END as ur_x'
        LOOP
        END LOOP;

        ll_x := rec.ll_x;
        ur_x := rec.ur_x;

        FOR rec IN EXECUTE 'SELECT CASE WHEN '||source_y||'<'||
            target_y||' THEN '||source_y||' ELSE '||
            target_y||' END as ll_y, CASE WHEN '||
            source_y||'>'||target_y||' THEN '||
            source_y||' ELSE '||target_y||' END as ur_y'
        LOOP
        END LOOP;

        ll_y := rec.ll_y;
        ur_y := rec.ur_y;
	
	-- Searching for the source and target edges
	
--	RAISE NOTICE 'Searching for the source and target edges';
	
	SELECT find_nearest_link_within_distance_xy(source_x, source_y, delta, geom_table) INTO s_gid;
	SELECT find_nearest_link_within_distance_xy(target_x, target_y, delta, geom_table) INTO t_gid;

--	RAISE NOTICE 'Nearest links were found';

        FOR rec IN EXECUTE 'SELECT DISTINCT gid,source,target,x1,y1,x2,y2,'||cost_column||' as length, '||reverse_cost_column||' as reverse_cost,the_geom FROM ' || quote_ident(geom_table) || ' WHERE gid = ' || s_gid
	LOOP
	END LOOP;
	
        source.gid          := rec.gid;
	source.target       := rec.target;
	source.source       := rec.source;
	source.x1           := rec.x1;
	source.y1           := rec.y1;
	source.x2           := rec.x2;
	source.y2           := rec.y2;
	source.length       := rec.length;
	source.reverse_cost := rec.reverse_cost;
	source.the_geom     := rec.the_geom;

        FOR rec IN EXECUTE 'SELECT DISTINCT gid,source,target,x1,y1,x2,y2,'||cost_column||' as length, '||reverse_cost_column||' as reverse_cost,the_geom FROM ' || quote_ident(geom_table) || ' WHERE gid = ' || t_gid
	LOOP
	END LOOP;
	
        target.gid          := rec.gid;
	target.target       := rec.target;
	target.source       := rec.source;
	target.x1           := rec.x1;
	target.y1           := rec.y1;
	target.x2           := rec.x2;
	target.y2           := rec.y2;
	target.length       := rec.length;
	target.reverse_cost := rec.reverse_cost;
	target.the_geom     := rec.the_geom;

--	RAISE NOTICE 'Searching for max gid and node id';
	
--        FOR rec IN EXECUTE 'SELECT max(gid) AS max_gid, greatest(max(source), max(target)) AS max_vertex_id FROM ' 
--	  || quote_ident(geom_table) || ' where setSRID(''BOX3D('||
--          ll_x-delta||' '||ll_y-delta||','||ur_x+delta||' '||
--          ur_y+delta||')''::BOX3D, ' || srid || ') && the_geom'
--	LOOP
--	END LOOP;

        FOR rec IN EXECUTE 'SELECT max_gid, max_vertex_id FROM network_info WHERE tname = ''' || quote_ident(geom_table) || ''''
	LOOP
	END LOOP;
	
	max_gid:=rec.max_gid;
	max_vertex_id:=rec.max_vertex_id;

--	RAISE NOTICE 'Max gid and node id were found';

        -- Locate source and target points
	-- extra_edges[1] - source first
	-- extra_edges[2] - source last
        SELECT DISTINCT l, f FROM locate_point_as_geometry(geom_table, s_gid, source_x, source_y, false) INTO l_pair.l, l_pair.f;

--        RAISE NOTICE 'Creating fake edges';
        
	extra_edges.the_geom[1] := l_pair.f;
	extra_edges.gid[1] := max_gid+1;
	extra_edges.source[1] := source.source;
	-- New target vertex (max_vertex_id+1)
	extra_edges.target[1] := max_vertex_id+1;
	extra_edges.x1[1] := source.x1;
	extra_edges.y1[1] := source.y1;
	extra_edges.x2[1] := x(startpoint(l_pair.f));
	extra_edges.y2[1] := y(startpoint(l_pair.f));
	extra_edges.length[1] := source.length*(length(l_pair.f)/length(source.the_geom));
	extra_edges.reverse_cost[1] := source.reverse_cost*(length(l_pair.f)/length(source.the_geom));
	
	extra_edges.the_geom[2] := l_pair.l;
	extra_edges.gid[2] := max_gid+2;
	extra_edges.target[2] := source.target;
	-- New target vertex (max_vertex_id+1)
	extra_edges.source[2] := max_vertex_id+1;
	extra_edges.x2[2] := source.x2;
	extra_edges.y2[2] := source.y2;
	extra_edges.x1[2] := x(PointN(l_pair.f, NumPoints(l_pair.f)));
	extra_edges.y1[2] := y(PointN(l_pair.f, NumPoints(l_pair.f)));
	extra_edges.length[2] := source.length*(length(l_pair.l)/length(source.the_geom));
	extra_edges.reverse_cost[2] := source.reverse_cost*(length(l_pair.l)/length(source.the_geom));
	
	-- extra_edges[3] - target first
	-- extra_edges[4] - target last
        SELECT DISTINCT l, f FROM locate_point_as_geometry(geom_table, t_gid, target_x, target_y, false) INTO l_pair.l, l_pair.f;
	extra_edges.the_geom[3] := l_pair.f;
	extra_edges.gid[3] := max_gid+3;
	extra_edges.source[3] := target.source;
	-- New target vertex (max_vertex_id+2)
	extra_edges.target[3] := max_vertex_id+2;
	extra_edges.x1[3] := target.x1;
	extra_edges.y1[3] := target.y1;
	extra_edges.x2[3] := x(endpoint(l_pair.f));
	extra_edges.y2[3] := y(endpoint(l_pair.f));
	extra_edges.length[3] := target.length*(length(l_pair.f)/length(target.the_geom));
	extra_edges.reverse_cost[3] := target.reverse_cost*(length(l_pair.f)/length(target.the_geom));

	extra_edges.the_geom[4] := l_pair.l;
	extra_edges.gid[4] := max_gid+4;
	extra_edges.target[4] := target.target;
	-- New target vertex (max_vertex_id+2)
	extra_edges.source[4] := max_vertex_id+2;
	extra_edges.x2[4] := target.x2;
	extra_edges.y2[4] := target.y2;
	extra_edges.x1[4] := x(endpoint(l_pair.f));
	extra_edges.y1[4] := y(endpoint(l_pair.f));
	extra_edges.length[4] := target.length*(length(l_pair.l)/length(target.the_geom));
	extra_edges.reverse_cost[4] := target.reverse_cost*(length(l_pair.l)/length(target.the_geom));

	extra_edges.the_geom[5] := geometryfromtext('LINESTRING('||source_x||' '||source_y||','||extra_edges.x1[2]||' '||extra_edges.y1[2]||')', srid);

	extra_edges.gid[5] := max_gid+5;
	-- New target vertex (max_vertex_id+3)
	extra_edges.source[5] := max_vertex_id+3;
	extra_edges.target[5] := extra_edges.target[1];
	extra_edges.x1[5] := source_x;
	extra_edges.y1[5] := source_y;
	extra_edges.x2[5] := extra_edges.x2[2];
	extra_edges.y2[5] := extra_edges.y2[2];
	extra_edges.length[5] := length(extra_edges.the_geom[5]);
	extra_edges.reverse_cost[5] := 1000000.0;

	extra_edges.the_geom[6] := geometryfromtext('LINESTRING('||extra_edges.x2[3]||' '||extra_edges.y2[3]||','||target_x||' '||target_y||')', srid);

	extra_edges.gid[6] := max_gid+6;
	
	-- New target vertex (max_vertex_id+4)
	extra_edges.source[6] := max_vertex_id+4;
	extra_edges.target[6] := extra_edges.target[3];
	extra_edges.x2[6] := target_x;
	extra_edges.y2[6] := target_y;
	extra_edges.x1[6] := extra_edges.x2[3];
	extra_edges.y1[6] := extra_edges.y2[3];
	extra_edges.length[6] := length(extra_edges.the_geom[6]);
	extra_edges.reverse_cost[6] := 1000000.0;
	
	select relname INTO seqname from pg_class where relname='rownum';
	
	IF seqname IS NOT NULL THEN
	EXECUTE 'drop sequence rownum';
	END IF;

	EXECUTE 'create sequence rownum';
	
        IF s_gid = t_gid THEN
	  
	  SELECT * FROM get_middle(geom_table, s_gid, source_x, source_y, target_x, target_y, true) INTO middle;
	  
	  geom.gid := extra_edges.gid[5];
          geom.the_geom := extra_edges.the_geom[5];
	  geom.id := 0;    
          RETURN NEXT geom;

	  geom.gid := extra_edges.gid[1];
          geom.the_geom := middle;
	  geom.id := 1;
          RETURN NEXT geom;

	  geom.gid := extra_edges.gid[6];
          geom.the_geom := extra_edges.the_geom[6];
	  geom.id := 2;
          RETURN NEXT geom;
	  
	  RETURN;

	END IF;

        -- Main search query
	-- Need to search for new geometries in the array instead of the table
	query := 'select distinct a.rownum as id, a.edge_id, b.gid, b.the_geom from (select nextval(''rownum'') as rownum, edge_id from ' || 
          fname || '(''SELECT DISTINCT gid as id, source::integer, ' || 
          'target::integer, '||cost_column||'::double precision as cost, ' || 
          'x1::double precision, y1::double precision, x2::double ' ||
          'precision, y2::double precision ';
          
        IF rc THEN query := query || ' , '||reverse_cost_column||' as reverse_cost ';  
        END IF;
          
        query := query || 'FROM ' || quote_ident(geom_table) || ' where setSRID(''''BOX3D('||
          ll_x-delta||' '||ll_y-delta||','||ur_x+delta||' '||
          ur_y+delta||')''''::BOX3D, ' || srid || ') && the_geom';

--	RAISE NOTICE 'Query: %', query;

	-- Newly created edges should be appended here
        FOR i IN 1..6 LOOP
	  --curr := extra_edges[i];
--	  RAISE NOTICE 'i=%', i;
	  IF extra_edges.rule[i] IS NULL THEN extra_edges.rule[i]:='NULL';
	  ELSE extra_edges.rule[i]:=''''''||extra_edges.rule[i]||'''''';
	  END IF;
	  
	  query := query || ' UNION ALL SELECT ' || extra_edges.gid[i]::integer || ', ' || extra_edges.source[i]::integer ||
	           ', ' || extra_edges.target[i]::integer || ', ' || extra_edges.length[i]::double precision ||
	           ', ' || extra_edges.x1[i]::double precision || ', ' || extra_edges.y1[i]::double precision ||
	           ', ' || extra_edges.x2[i]::double precision || ', ' || extra_edges.y2[i]::double precision;
                   IF rc THEN query := query || ' , ' || extra_edges.reverse_cost[i]::double precision;
                   END IF;
--	RAISE NOTICE 'Query: %', query;
	END LOOP;
	
        -- Need to use new ids as source and target

	  --curr := extra_edges[5];
	  --tmp := extra_edges[6];

--	RAISE NOTICE 'Query: %', query;

	query := query || ''', ' || 	  
          quote_literal(extra_edges.source[5]) || ' , ' || 
          quote_literal(extra_edges.target[6]) || ' , '''||text(dir)||''', '''||text(rc)||''' ) ) a LEFT JOIN ' || 
          quote_ident(geom_table) || ' b ON (a.edge_id=b.gid) ';
          
	--RAISE NOTICE 'max_gid: %, max_vertex_id: %', max_gid, max_vertex_id;
--	RAISE NOTICE 'Query: %', query;
	
        --geom.gid := extra_edges.gid[5];
        --geom.the_geom := extra_edges.the_geom[5];
        --geom.id := 0;
        
        --RETURN NEXT geom;
        
        FOR path_result IN EXECUTE query
        LOOP
                 geom.gid      := path_result.edge_id;
--	         RAISE NOTICE ' -- gid: %', geom.gid;
	         
		 -- Need to search for new geometries in the array instead of the table
		 IF geom.gid > max_gid THEN
		   geom.the_geom := extra_edges.the_geom[geom.gid-max_gid];
		 ELSE
                   geom.the_geom := path_result.the_geom;
		 END IF;
                 
		 id := id+1;
                 geom.id       := path_result.id;
                 
                 RETURN NEXT geom;

        END LOOP;
        
        RETURN;
END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT; 

