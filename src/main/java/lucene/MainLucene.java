/** Copyrigh Epimoprhics 2014 */
package lucene;

import java.io.File ;

import lridx.Builder ;
import lridx.DeNorm ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.lucene.index.Term ;
import org.apache.lucene.search.* ;
import org.apache.lucene.search.BooleanClause.Occur ;
import org.apache.lucene.store.Directory ;
import org.apache.lucene.store.FSDirectory ;
import org.apache.lucene.store.RAMDirectory ;

import com.hp.hpl.jena.query.ResultSet ;

public class MainLucene {
    
    public static void main(String... argv) throws Exception {

        if ( false )
        {
            Query qn1 = NumericRangeQuery.newIntRange("propertyPrice", 1000*1000, null, true, true);
            Term t = new Term("propertyTown", "LONDON") ;
            Query qn2 = new TermQuery(t) ;

            BooleanClause bc1 = new BooleanClause(qn1, Occur.MUST) ;
            BooleanClause bc2 = new BooleanClause(qn2, Occur.MUST) ;
            
            BooleanQuery bq = new BooleanQuery() ;
            bq.add(bc1);
            bq.add(bc2);
            
//            Analyzer analyzer = new KeywordAnalyzer() ;
//            QueryParser queryParser = new QueryParser(Version.LUCENE_46, "uri", analyzer) ;
//            queryParser.setAllowLeadingWildcard(true) ;
//            org.apache.lucene.search.Query query = queryParser.parse(qs1) ;
            System.out.println(bq ) ;

            System.exit(0) ;
        }
        
        
        
        
        
        boolean needsBuilding = false ;
        
        if ( Builder.INDEX != null ) {
            boolean exists = FileOps.exists(Builder.INDEX) ;
            if ( ! exists ) {
                FileOps.ensureDir(Builder.INDEX);
                needsBuilding = true ;
            }                
        }
        
        try (Directory dir = ( Builder.INDEX != null ) ? FSDirectory.open(new File(Builder.INDEX)) : new RAMDirectory() ) {
            if ( needsBuilding ) {
                System.out.println("Building...") ;
                ResultSet rs = DeNorm.extract() ;
                Builder.build(dir, rs);
            }
            System.out.println("Querying...") ;
            String qs = "transactionDate:[ 2005 TO 2008 }" ;
            Access.read(dir, qs) ;
        }
        System.out.println("DONE") ;
    }
    

    
//    static Transform<QuerySolution, String> transform = new Transform<QuerySolution, String>() {
//        @Override
//        public String convert(QuerySolution row) {
//            return row.getResource("item").getURI() ;
//        } } ;
//    
//    
//    static Iterator<String> things(String type) {
//        String x = StrUtils.strjoinNL
//            (DeNorm.prefixes
//            ,"SELECT * { ?item rdf:type "+type+"} LIMIT 10"
//            ) ;
//        QueryExecution qExec = QueryExecutionFactory.sparqlService(DeNorm.endpoint, x) ;
//        ResultSet rs = qExec.execSelect() ;
//        return Iter.map(rs, transform) ;
//    }
//    
//    static void propertiesOf(String itemURI) {
//        System.out.println("Properties of: "+itemURI) ;
//        String x = StrUtils.strjoinNL
//            (DeNorm.prefixes
//            ,"SELECT * { ?item ?p ?o . FILTER(?item = <"+itemURI+">) }"
//            ) ;
//        try (QueryExecution qExec = QueryExecutionFactory.sparqlService(DeNorm.endpoint, x)) {
//            ResultSet rs = qExec.execSelect() ;
//            while(rs.hasNext() ) {
//                System.out.println(rs.next()) ;
//            }
//        }
//    }
}

