/** Copyrigh Epimoprhics 2014 */
package lridx;

import java.io.File ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.lucene.store.Directory ;
import org.apache.lucene.store.FSDirectory ;
import org.apache.lucene.store.RAMDirectory ;

import com.hp.hpl.jena.query.ResultSet ;

public class MainCube {
    
    public static void main(String... argv) throws Exception {
        boolean needsBuilding = false ;
        
        if ( DeNorm.INDEX != null ) {
            boolean exists = FileOps.exists(DeNorm.INDEX) ;
            if ( ! exists ) {
                FileOps.ensureDir(DeNorm.INDEX);
                needsBuilding = true ;
            }                
        }
        
        try (Directory dir = ( DeNorm.INDEX != null ) ? FSDirectory.open(new File(DeNorm.INDEX)) : new RAMDirectory() ) {
            if ( needsBuilding ) {
                System.out.println("Building...") ;
                ResultSet rs = Builder.extract() ;
                Builder.build(dir, rs);
            }
            System.out.println("Querying...") ;
            Access.read(dir) ;
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

