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

package lridx;

import com.hp.hpl.jena.query.* ;
import com.hp.hpl.jena.tdb.TDB ;
import com.hp.hpl.jena.tdb.TDBFactory ;

import org.apache.jena.atlas.lib.StrUtils ;

public class DeNorm {
    // Change endpoint and LIMIT
    
    public static String endpoint = "http://lr-data-staging.epimorphics.com/landregistry/query" ;
    public static String DB = "/media/ephemeral0/fuseki/databases/LR-DB" ;

    public static ResultSet extract() throws Exception {
        String x = DeNorm.queryString() ;
        com.hp.hpl.jena.query.Query q = QueryFactory.create(x) ;
        //System.out.println(q) ;
        //System.exit(0) ;

        if ( DeNorm.endpoint != null ) {
            //log.info("Remote extraction");
            QueryExecution qExec = QueryExecutionFactory.sparqlService(DeNorm.endpoint, x) ;
            return qExec.execSelect() ;
        } else {
            //log.info("Local extraction"); 
            Dataset ds = TDBFactory.createDataset(DeNorm.DB) ;
            ds.getContext().set(TDB.symUnionDefaultGraph, true) ;
            QueryExecution qExec = QueryExecutionFactory.create(q, ds) ;

            ResultSet rs = qExec.execSelect() ;
            //            ResultSetRewindable rsw = ResultSetFactory.makeRewindable(rs) ;
            //            int c = ResultSetFormatter.consume(rsw) ;
            //            log.info("Extracted: "+c) ;
            //            rsw.reset() ;
            //            rs = rsw ;
            return rs ;
        }
    }

    public static final String ppiPrefix = "http://landregistry.data.gov.uk/def/ppi/" ;
    public static final String prefixes = StrUtils.strjoinNL
          ("prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
           ,"prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
           ,"prefix owl: <http://www.w3.org/2002/07/owl#>"
           ,"prefix xsd: <http://www.w3.org/2001/XMLSchema#>"
           ,"prefix sr: <http://data.ordnancesurvey.co.uk/ontology/spatialrelations/>"
           ,"prefix lrhpi: <http://landregistry.data.gov.uk/def/hpi/>"
           ,"prefix lrppi: <http://landregistry.data.gov.uk/def/ppi/>"
           ,"prefix skos: <http://www.w3.org/2004/02/skos/core#>"
           ,"prefix lrcommon: <http://landregistry.data.gov.uk/def/common/>"
           ,"PREFIX  hpi:  <http://landregistry.data.gov.uk/def/hpi/>"
           ,"PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>"
           ,"PREFIX  ppd:  <http://landregistry.data.gov.uk/def/ppi/>"
           ,"PREFIX  ppi:  <http://landregistry.data.gov.uk/def/ppi/>"
           ) ;
      /*
        {
          ?item ppd:pricePaid ?ppd_pricePaid .
          ?item ppd:hasTransaction ?ppd_hasTransaction .
          ?item ppd:propertyAddress ?ppd_propertyAddress .
          ?item ppd:publishDate ?ppd_publishDate .
          ?item ppd:transactionDate ?ppd_transactionDate .
          ?item ppd:transactionId ?ppd_transactionId
          OPTIONAL { ?item ppd:estateType ?ppd_estateType }
          OPTIONAL { ?item ppd:newBuild ?ppd_newBuild }
          OPTIONAL { ?ppd_propertyAddress lrcommon:county ?ppd_propertyAddressCounty }
          OPTIONAL { ?ppd_propertyAddress lrcommon:district ?ppd_propertyAddressDistrict }
          OPTIONAL { ?ppd_propertyAddress lrcommon:locality ?ppd_propertyAddressLocality }
          OPTIONAL { ?ppd_propertyAddress lrcommon:paon ?ppd_propertyAddressPaon }
          OPTIONAL { ?ppd_propertyAddress lrcommon:postcode ?ppd_propertyAddressPostcode }
          OPTIONAL { ?ppd_propertyAddress lrcommon:saon ?ppd_propertyAddressSaon }
          OPTIONAL { ?ppd_propertyAddress lrcommon:street ?ppd_propertyAddressStreet }
          OPTIONAL { ?ppd_propertyAddress lrcommon:town ?ppd_propertyAddressTown }
          OPTIONAL { ?item ppd:propertyType ?ppd_propertyType }
          OPTIONAL { ?item ppd:recordStatus ?ppd_recordStatus }
        }
       */
    public static int LIMIT = 2 ;
    public static String queryString() {
        String type = "lrppi:TransactionRecord" ;
        String x = StrUtils.strjoinNL
          (DeNorm.prefixes
           ,"SELECT * { ?item rdf:type "+type+" ."
           ,"    ?item ppd:pricePaid ?ppd_pricePaid ."
           //,"    ?item ppd:hasTransaction ?ppd_hasTransaction ."
           ,"    ?item ppd:propertyAddress ?ppd_propertyAddress ."  // Include linkage
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
           ,"}"
              ) ;
        if ( LIMIT > 0 )
            x = x+"\nLIMIT "+LIMIT ;
        
      return x ;
  }

}

