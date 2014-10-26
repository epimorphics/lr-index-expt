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

import java.io.File ;
import java.util.ArrayList ;
import java.util.Iterator ;
import java.util.List ;
import java.util.Objects ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.lucene.analysis.Analyzer ;
import org.apache.lucene.analysis.core.KeywordAnalyzer ;
import org.apache.lucene.document.Document ;
import org.apache.lucene.document.Field ;
import org.apache.lucene.document.TextField ;
import org.apache.lucene.index.IndexWriter ;
import org.apache.lucene.index.IndexWriterConfig ;
import org.apache.lucene.store.Directory ;
import org.apache.lucene.store.FSDirectory ;
import org.apache.lucene.store.RAMDirectory ;
import org.apache.lucene.util.Version ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.hp.hpl.jena.query.* ;
import com.hp.hpl.jena.rdf.model.Literal ;
import com.hp.hpl.jena.rdf.model.RDFNode ;
import com.hp.hpl.jena.rdf.model.Resource ;
import com.hp.hpl.jena.tdb.TDB ;
import com.hp.hpl.jena.tdb.TDBFactory ;

import cube.CubeSQL ;
import cube.DeNorm ;

public class Builder {

    static Logger log = LoggerFactory.getLogger(Builder.class) ;
    static { LogCtl.setCmdLogging(); } 
    
    public static String INDEX = "TEXT" ;
    public static String DB = "/media/ephemeral0/fuseki/databases/LR-DB" ;

    
    public static void main(String[] args) throws Exception {
        
        CubeSQL.endpoint = null ;
        
        boolean needsBuilding = false ;
        
        if ( INDEX != null ) {
            boolean exists = FileOps.exists(INDEX) ;
            log.debug("Exists "+exists) ;
            if ( ! exists ) {
                FileOps.ensureDir(INDEX);
                needsBuilding = true ;
            }                
        }
        
        try (Directory dir = ( INDEX != null ) ? FSDirectory.open(new File(INDEX)) : new RAMDirectory() ) {
            if ( needsBuilding ) {
                log.info("Building...") ;
                ResultSet rs = Builder.extract() ;
                Builder.build(dir, rs);
            }
        }
        log.info("DONE") ;
    }

    public static ResultSet extract() throws Exception {
        String x = DeNorm.queryString() ;
        com.hp.hpl.jena.query.Query q = QueryFactory.create(x) ;
        //System.out.println(q) ;
        //System.exit(0) ;

        if ( CubeSQL.endpoint != null ) {
            //log.info("Remote extraction");
            QueryExecution qExec = QueryExecutionFactory.sparqlService(CubeSQL.endpoint, x) ;
            return qExec.execSelect() ;
        } else {
            //log.info("Local extraction"); 
            Dataset ds = TDBFactory.createDataset(DB) ;
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

    /* Record */
    public static class EntityDetail {
        
    }

    public static void build(Directory dir, ResultSet rs) throws Exception {
        String current = null ;
        Document doc = null ;
        
        Analyzer anlzKeyword = new KeywordAnalyzer() ;
        
        List<String> stopWords_ = new ArrayList<>() ;
//        CharArraySet stopWords = new CharArraySet(Version.LUCENE_46, stopWords_, true);
//        Analyzer analyzer2 = new StandardAnalyzer(Version.LUCENE_46, stopWords) ;
        
        int count = 0 ;

        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_46, anlzKeyword) ;
        try(IndexWriter wIdx = new IndexWriter(dir, conf)) {

            while(rs.hasNext()) {
                QuerySolution row = rs.next() ;
                String item = row.getResource("item").getURI() ;
                if ( Objects.equals(item, current) ) {
                } else {
                    if ( doc != null ) {
                        count++ ;
                        if ( count % 10000 == 0 )
                            FmtLog.info(log, "Docs %,d", count) ;
                        wIdx.addDocument(doc);
                    }
                    current = item ;
                    doc = new Document() ;
                    Field field = new Field("uri", item, TextField.TYPE_STORED) ;
                    doc.add(field);
                    //                System.out.print("** ") ;
                    //                System.out.println(item) ;
                }

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

                Iterator<String> iter = row.varNames() ;
                while(iter.hasNext()) {
                    String vn = iter.next() ;
                    if ( vn.startsWith("ppd_") ) {
                        String p = vn.substring("ppd_".length()) ; 
                        RDFNode o = row.get(vn) ;
                        String oStr = (o.isLiteral()) ? ((Literal)o).getLexicalForm() : ((Resource)o).getURI() ;
                        Field field = new Field(p, oStr, TextField.TYPE_STORED) ;
                        //log.info(field.toString()) ;
                        doc.add(field); 
                    }
                }
            }   
            if ( doc != null ) {
                count++ ;
                wIdx.addDocument(doc);
            }
        }
        FmtLog.info(log, "Count %,d", count) ;
    }
        

}

