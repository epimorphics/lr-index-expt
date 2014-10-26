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

package lucene;

import java.io.File ;

import lridx.Builder ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.lucene.analysis.Analyzer ;
import org.apache.lucene.analysis.core.KeywordAnalyzer ;
import org.apache.lucene.document.Document ;
import org.apache.lucene.index.DirectoryReader ;
import org.apache.lucene.index.IndexReader ;
import org.apache.lucene.queryparser.classic.QueryParser ;
import org.apache.lucene.search.IndexSearcher ;
import org.apache.lucene.search.ScoreDoc ;
import org.apache.lucene.store.Directory ;
import org.apache.lucene.store.FSDirectory ;
import org.apache.lucene.util.Version ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class Access {
    static Logger log = LoggerFactory.getLogger(Access.class) ;
    static { LogCtl.setCmdLogging(); } 
    
    public static void main(String... argv) throws Exception {
        if ( Builder.INDEX == null ) {
            log.error("Null index") ;
            System.exit(1) ;
        }
        
        if ( ! FileOps.exists(Builder.INDEX) ) {
            log.error("No index") ;
            System.exit(1) ;
        }
        
        if ( argv.length != 1 ) {
            log.error("Usage : \"lucene_query\"") ;
            System.exit(2) ;
        }
            
        String queryString = argv[0] ;
        
        try (Directory dir = FSDirectory.open(new File(Builder.INDEX))) {
            read(dir, queryString);
        }
    }
    
    public static void read(Directory dir, String qs) throws Exception {    
        Analyzer analyzer = new KeywordAnalyzer() ;
        IndexReader indexReader = DirectoryReader.open(dir) ;
        IndexSearcher indexSearcher = new IndexSearcher(indexReader) ;
    
        QueryParser queryParser = new QueryParser(Version.LUCENE_46, "uri", analyzer) ;
        queryParser.setAllowLeadingWildcard(true) ;
        org.apache.lucene.search.Query query = queryParser.parse(qs) ;
        
        
        // There are 19M transaction records.
        int limit = 50*1000*1000 ;
        if ( limit <= 0 )
            limit = 100000 ;
        ScoreDoc[] sDocs = indexSearcher.search(query, limit).scoreDocs ;
//        if ( sDocs.length == 0 )
//            log.info("No hits"); 
        for ( ScoreDoc sd : sDocs ) {
            Document doc2 = indexSearcher.doc(sd.doc) ;
            //String[] values = doc2.getValues("uri") ;
            log.info(doc2.get("uri")+"  "+doc2.get("transactionDate")+"  "+doc2.get("pricePaid")) ;
        }
    }

}

