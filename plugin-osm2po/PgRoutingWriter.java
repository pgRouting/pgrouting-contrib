/**
  Copyright (c) 2011 Carsten Moeller, Pinneberg, Germany. <info@osm2po.de>

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.cm.osm2po.converter;

import static de.cm.osm2po.Utils.DF;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Date;

import de.cm.osm2po.Config;
import de.cm.osm2po.Log;
import de.cm.osm2po.Version;
import de.cm.osm2po.model.Node;
import de.cm.osm2po.model.SegmentedWay;
import de.cm.osm2po.model.WaySegment;
import de.cm.osm2po.primitives.InStream;
import de.cm.osm2po.primitives.InStreamDisk;
import de.cm.osm2po.primitives.VarTypeDesk;

public class PgRoutingWriter implements PostProcessor {

    private static Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    
    private static byte[] KOMMA = ",".getBytes();
    private static byte[] SEMIKOLON = ";".getBytes();
    private static byte[] NEWLINE = "\n".getBytes();

    @Override
    public void run(Config config) throws Exception {
        
        File dir = config.getWorkDir();
        String prefix = config.getPrefix();
        Log log = config.getLogger();
        
        String tableName = prefix + "_2po_4pgr";
        String fileName = prefix + "_2po_4pgr.sql";
        
        File waysInFile = new File(dir, Segmenter.SEGMENTS_FILENAME);
        InStream inStream = new InStreamDisk(waysInFile);
        
        OutputStream os = null;
        File sqlOutFile = null;
        if (config.isPipeOut()) {
            os = System.out;
            log.info("Writing results to stdout");
        } else {
            sqlOutFile = new File(dir, fileName);
            os = new BufferedOutputStream(
                    new FileOutputStream(sqlOutFile), 0x10000 /*64k*/);
            log.info("Creating sql file " + sqlOutFile.toString());
        }

        byte varType = inStream.readByte();
        if (varType != VarTypeDesk.typeIdOf(SegmentedWay.class)) 
            throw new RuntimeException("Unexpected VarType " + varType);
        

        os.write((""
            + "-- Created by  : " + Version.getName() + "\n"
            + "-- Version     : " + Version.getVersion() + "\n"
            + "-- Author (c)  : Carsten Moeller - info@osm2po.de\n"
            + "-- Date        : " + new Date().toString()
            + "\n\n"
            + "DROP TABLE IF EXISTS " + tableName + ";"
            + "\n"
            + "-- SELECT DropGeometryTable('" + tableName + "');"
            + "\n\n"
            + "CREATE TABLE " + tableName + "("
            + "id integer, "
            + "osm_id bigint, osm_name character varying, "
            + "osm_source_id bigint, osm_target_id bigint, "
            + "clazz integer, flags integer, "
            + "source integer, target integer, "
            + "km double precision, kmh integer, "
            + "cost double precision, reverse_cost double precision, "
            + "x1 double precision, y1 double precision, "
            + "x2 double precision, y2 double precision"
            + ");\n"
            + "SELECT AddGeometryColumn('" + tableName + "', "
            + "'geom_way', 4326, 'LINESTRING', 2);\n")
        .getBytes());

        byte[] INSERT = ("\nINSERT INTO " + tableName + " VALUES ").getBytes();
        
        SegmentedWay way = new SegmentedWay();
        long n = 0, g = 0;
        while (!inStream.isEof()) {
            way.readFromStream(inStream);

            long osm_id = way.getId();
            String osm_name = escapeSQL(way.getName().toString());
            int type = way.getType();
            int flags = way.getFlags();
            int kmh = way.getKmh(); if (kmh <= 0) kmh = 1;
            boolean isOneWay = way.isOneWay();

            for (int i = 0; i < way.getSegments().length; i++) {
                WaySegment waySegment = way.getSegments()[i];
                int id = waySegment.getId();
                int source = waySegment.getSourceId();
                int target = waySegment.getTargetId();
                Node n1 = waySegment.getNodes()[0];
                Node n2 = waySegment.getNodes()[waySegment.getNodes().length -1];
                long osmSourceId = n1.getId();
                long osmTargetId = n2.getId();
                float km = (float) waySegment.calcLengthKm();
                float cost = km / kmh;
                float reverse_cost = isOneWay ? 1000000f : cost;
                
                String geom_way =
                    GeomConverter.latLons2LineWkbHex(waySegment.getNodes(), true);
                
                // Gruppenbildung macht SQL erwiesenermassen schneller und kuerzer.
                if (g == 25) {
                    os.write(SEMIKOLON);
                    os.write(NEWLINE);
                    g = 0;
                }
                if (++g == 1) os.write(INSERT); else os.write(KOMMA);
                os.write(NEWLINE);
                
                
                os.write(("(" + id + ", " + osm_id + ", " + "'").getBytes());
                os.write(osm_name.getBytes(CHARSET_UTF8));
                os.write(("', "
                    + osmSourceId + ", " + osmTargetId + ", "
                    + type + ", " + flags + ", "
                    + source + ", " + target + ", "
                    + km + ", " + kmh + ", " + cost + ", " + reverse_cost + ", "
                    + n1.getLon() + ", " + n1.getLat() + ", "
                    + n2.getLon() + ", " + n2.getLat() + ", "
                    + "'" + geom_way + "'" + ")")
                    .getBytes());
                
                if (++n % 100000 == 0) log.progress(DF(n) + " Segments written.");
            }
        }
        // Rest schreiben.
        os.write(SEMIKOLON);
        os.write(NEWLINE);

        log.info(DF(n) + " Segments written.");

        os.write(("\n"   
            + "ALTER TABLE " + tableName + " ADD CONSTRAINT pkey_" + tableName + " PRIMARY KEY(id);\n"
            + "CREATE INDEX idx_" + tableName + "_source ON " + tableName + "(source);\n"
            + "CREATE INDEX idx_" + tableName + "_target ON " + tableName + "(target);\n"
            + "-- CREATE INDEX idx_" + tableName + "_osm_source_id ON " + tableName + "(osm_source_id);\n"
            + "-- CREATE INDEX idx_" + tableName + "_osm_target_id ON " + tableName + "(osm_target_id);\n"
            + "-- CREATE INDEX idx_" + tableName + "_geom_way  ON " + tableName
            + " USING GIST (geom_way GIST_GEOMETRY_OPS);\n")
            .getBytes());

        os.close();
        inStream.close();

        if (sqlOutFile != null) {
            log.info("commandline template:\n"
                    + "psql -U [username] -d [dbname] -q -f \""
                    + sqlOutFile.getAbsolutePath() + "\"");
        }
        
    }    

    private String escapeSQL(String strToEscape) {
        return strToEscape
        .replaceAll("\'", "\'\'")
        .replaceAll("\\\\", "/");
    }
    
}
