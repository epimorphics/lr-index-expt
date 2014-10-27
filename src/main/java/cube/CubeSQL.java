/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cube;

import java.io.PrintStream ;
import java.util.LinkedHashMap ;
import java.util.Map ;
import java.util.Map.Entry ;

import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.hp.hpl.jena.query.QuerySolution ;
import com.hp.hpl.jena.query.ResultSet ;
import com.hp.hpl.jena.rdf.model.Literal ;
import com.hp.hpl.jena.rdf.model.RDFNode ;
import com.hp.hpl.jena.rdf.model.Resource ;

public class CubeSQL {
    static Logger log = LoggerFactory.getLogger(CubeSQL.class) ;

    // Schema
    public static final String STRING = "VARCHAR(255)" ;
    public static final String DATE = "DATE" ;
    public static final String NUMBER = "INT" ;

    public static Map<String, String> entityMap = new LinkedHashMap<>() ;
    static {
        entityMap.put("item", STRING) ;
        entityMap.put("ppd_pricePaid", NUMBER) ;
        //entityMap.put("ppd_hasTransaction",null) ;
        //entityMap.put("ppd_propertyAddress",null) ;
        //entityMap.put("ppd_publishDate",DATE) ;
        entityMap.put("ppd_transactionDate",DATE) ;
        entityMap.put("ppd_transactionId",STRING) ;
        entityMap.put("ppd_estateType",STRING) ;
        entityMap.put("ppd_newBuild",STRING) ;
        entityMap.put("ppd_propertyType",STRING) ;
        //entityMap.put("ppd_recordStatus",STRING) ;

        entityMap.put("ppd_propertyAddressCounty",STRING) ;
        entityMap.put("ppd_propertyAddressDistrict",STRING) ;
        entityMap.put("ppd_propertyAddressLocality",STRING) ;
        entityMap.put("ppd_propertyAddressPaon",STRING) ;
        entityMap.put("ppd_propertyAddressPostcode",STRING) ;
        entityMap.put("ppd_propertyAddressSaon",STRING) ;
        entityMap.put("ppd_propertyAddressStreet",STRING) ;
        entityMap.put("ppd_propertyAddressTown",STRING) ;
    }
    
    public static String createTableString() {
        StringBuilder sb = new StringBuilder() ;
        sb.append("TRUNCATE TABLE cube ;\n") ;
        sb.append("CREATE TABLE cube (\n") ;
        boolean first = true ;
        for ( Entry<String, String> e : entityMap.entrySet() ) {
            if ( ! first )
                sb.append(" ,\n") ;
            first = false ;
            sb.append("    "+e.getKey()+" "+e.getValue()) ;
        }
        sb.append("\n) ; ") ;
        return sb.toString() ;
        
    }

    public static int LIMIT = 1*1000*1000 ; 
    
    public static void build(PrintStream out, ResultSet rs) throws Exception {
        //log.info("Values");
        int count = 0 ;

        out.println("INSERT INTO LR.cube VALUES") ;
        boolean stmtFirst = true ;
        while(count < LIMIT && rs.hasNext()) {
            if ( stmtFirst ) {
                stmtFirst = false ;
            } else {
                out.print(",\n") ;
            }
            QuerySolution row = rs.next() ;
            count++ ;
            
            // Process row
            /*
                    ?ppd_pricePaid
                    ?ppd_hasTransaction ."
                    ,"    ?item ppd:propertyAddress ?ppd_propertyAddress ."
                    ,"    ?item ppd:publishDate ?ppd_publishDate ."
                    ,"    ?item ppd:transactionDate ?ppd_transactionDate ."
                    ,"    ?item ppd:transactionId ?ppd_transactionId"
                    ,"    OPTIONAL { ?item ppd:estateType ?ppd_estateType }"
                    ,"    OPTIONAL { ?item ppd:newBuild ?ppd_newBuild }"
                    ,"    OPTIONAL { ?ppd_propertyAddress lrcommon:county ?ppd_propertyAddressCounty }"
                    ,"    OPTIONAL { ?ppd_propertyAddress lrcommon:district ?ppd_propertyAddressDistrict }"
                    ,"    OPTIONAL { ?ppd_propertyAddress lrcommon:locality ?ppd_propertyAddressLocality }"
                    ,"    OPTIONAL { ?ppd_propertyAddress lrcommon:paon ?ppd_propertyAddressPaon }"
                    ,"    OPTIONAL { ?ppd_propertyAddress lrcommon:postcode ?ppd_propertyAddressPostcode }"
                    ,"    OPTIONAL { ?ppd_propertyAddress lrcommon:saon ?ppd_propertyAddressSaon }"
                    ,"    OPTIONAL { ?ppd_propertyAddress lrcommon:street ?ppd_propertyAddressStreet }"
                    ,"    OPTIONAL { ?ppd_propertyAddress lrcommon:town ?ppd_propertyAddressTown }"
                    ,"    OPTIONAL { ?item ppd:propertyType ?ppd_propertyType }"
                    ,"    OPTIONAL { ?item ppd:recordStatus ?ppd_recordStatus }"
             */

            boolean first = true ;
            out.print("( ") ;
            for ( Entry<String, String> e : entityMap.entrySet() ) {
                if ( ! first ) {
                    out.print(", ") ; 
                }
                first = false ;
                
                String col = e.getKey() ;
                String type = e.getValue() ;
                
                if ( row.contains(col) ) {
                    RDFNode o = row.get(col) ;
                    String oStr = (o.isLiteral()) ? ((Literal)o).getLexicalForm() : ((Resource)o).getURI() ;
                    oStr = fixup(oStr) ;
                    switch(type) {
                        case STRING: out.print("'"+oStr+"'") ; break ;
                        case NUMBER: out.print(oStr) ; break ;
                        case DATE: out.print("'"+oStr+"'") ; break ; // Convert format?
                    }
                } else {
                    out.print("NULL") ;
                }
            }
            out.print(" )") ;
        }
        out.print("\n;\n") ;
    }

    private static String fixup(String s) {
        s = s.replace("'",  "\\'") ;
        s = s.replace("\"",  "\\\"") ;
        return s ;
    }
}

