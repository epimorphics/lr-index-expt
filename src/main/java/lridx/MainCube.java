/** Copyrigh Epimoprhics 2014 */
package lridx;

import java.io.File ;
import java.util.Iterator ;
import java.util.Objects ;

import org.apache.jena.atlas.iterator.Iter ;
import org.apache.jena.atlas.iterator.Transform ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.StrUtils ;
import org.apache.lucene.analysis.Analyzer ;
import org.apache.lucene.analysis.core.KeywordAnalyzer ;
import org.apache.lucene.document.Document ;
import org.apache.lucene.document.Field ;
import org.apache.lucene.document.TextField ;
import org.apache.lucene.index.DirectoryReader ;
import org.apache.lucene.index.IndexReader ;
import org.apache.lucene.index.IndexWriter ;
import org.apache.lucene.index.IndexWriterConfig ;
import org.apache.lucene.queryparser.classic.QueryParser ;
import org.apache.lucene.search.IndexSearcher ;
import org.apache.lucene.search.ScoreDoc ;
import org.apache.lucene.store.Directory ;
import org.apache.lucene.store.FSDirectory ;
import org.apache.lucene.store.RAMDirectory ;
import org.apache.lucene.util.Version ;

import com.hp.hpl.jena.query.* ;
import com.hp.hpl.jena.rdf.model.Literal ;
import com.hp.hpl.jena.rdf.model.RDFNode ;
import com.hp.hpl.jena.rdf.model.Resource ;

public class MainCube {
    
    public static final String endpoint = "http://lr-data-staging.epimorphics.com/landregistry/query" ;
    
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
    
    public static String INDEX = "TEXT" ;
    public static boolean UseLocal = true ; 

    public static void main(String... argv) throws Exception {
        //INDEX = null ;
        if ( INDEX == null )
            UseLocal = false ;
        if ( ! UseLocal ) {
            FileOps.ensureDir(INDEX);
            FileOps.clearDirectory(INDEX);
        }
        
        @SuppressWarnings("resource")
        Directory dir = ( INDEX != null ) ? FSDirectory.open(new File(INDEX)) : new RAMDirectory() ;
        if ( ! UseLocal ) 
        { 
            ResultSet rs = extract() ;
            build(dir, rs) ;
        }
        read(dir) ;
        
        dir.close() ;
        System.out.println("DONE") ;
    }
    
    public static ResultSet extract(String... argv) throws Exception {
        String type = "lrppi:TransactionRecord" ;
        String x = StrUtils.strjoinNL
            (prefixes
            ,"SELECT * { ?item rdf:type "+type+" ."
            ,"    ?item ppd:pricePaid ?ppd_pricePaid ."
            //,"    ?item ppd:hasTransaction ?ppd_hasTransaction ."
            ,"    ?item ppd:propertyAddress ?ppd_propertyAddress ."
            ,"    ?item ppd:publishDate ?ppd_publishDate ."
            ,"    ?item ppd:transactionDate ?ppd_transactionDate ."
            //,"    ?item ppd:transactionId ?ppd_transactionId"
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
            ,"} LIMIT 1000"
            ) ;
        com.hp.hpl.jena.query.Query q = QueryFactory.create(x) ;
        //System.out.println(q) ;
        //System.exit(0) ;
        
        QueryExecution qExec = QueryExecutionFactory.sparqlService(endpoint, x) ;
        ResultSet rs = qExec.execSelect() ;
        //ResultSetFormatter.outputAsJSON(rs);
        return rs ;
    }
        
    public static void build(Directory dir, ResultSet rs) throws Exception {
        String current = null ;
        Document doc = null ;
        
        Analyzer analyzer = new KeywordAnalyzer() ;
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_46, analyzer) ;
        try(IndexWriter wIdx = new IndexWriter(dir, conf)) {

            int count = 0 ;

            while(rs.hasNext()) {
                QuerySolution row = rs.next() ;
                String item = row.getResource("item").getURI() ;
                if ( Objects.equals(item, current) ) {
                } else {
                    if ( doc != null ) {
                        count++ ;
                        if ( count % 1000 == 0 )
                            System.out.println(count) ;
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
                        //System.out.println(field) ;
                        doc.add(field); 
                    }
                }
            }   
            if ( doc != null )
                wIdx.addDocument(doc);
        }
    }
    
    static void read(Directory dir) throws Exception {    
        Analyzer analyzer = new KeywordAnalyzer() ;
        IndexReader indexReader = DirectoryReader.open(dir) ;
        IndexSearcher indexSearcher = new IndexSearcher(indexReader) ;

        QueryParser queryParser = new QueryParser(Version.LUCENE_46, "uri", analyzer) ;
        queryParser.setAllowLeadingWildcard(true) ;
        org.apache.lucene.search.Query query = queryParser.parse("transactionDate:[ 2005 TO 2008 }") ;
        int limit = 10000 ;
        if ( limit <= 0 )
            limit = 1000 ;
        ScoreDoc[] sDocs = indexSearcher.search(query, limit).scoreDocs ;
        for ( ScoreDoc sd : sDocs ) {
            Document doc2 = indexSearcher.doc(sd.doc) ;
            //String[] values = doc2.getValues("uri") ;
            System.out.println(doc2.get("uri")+"  "+doc2.get("transactionDate")+"  "+doc2.get("pricePaid")) ;
        }
    } 


    static Transform<QuerySolution, String> transform = new Transform<QuerySolution, String>() {
        @Override
        public String convert(QuerySolution row) {
            return row.getResource("item").getURI() ;
        } } ;
    
    
    static Iterator<String> things(String type) {
        String x = StrUtils.strjoinNL
            (prefixes
            ,"SELECT * { ?item rdf:type "+type+"} LIMIT 10"
            ) ;
        QueryExecution qExec = QueryExecutionFactory.sparqlService(endpoint, x) ;
        ResultSet rs = qExec.execSelect() ;
        return Iter.map(rs, transform) ;
    }
    
    static void propertiesOf(String itemURI) {
        System.out.println("Properties of: "+itemURI) ;
        String x = StrUtils.strjoinNL
            (prefixes
            ,"SELECT * { ?item ?p ?o . FILTER(?item = <"+itemURI+">) }"
            ) ;
        try (QueryExecution qExec = QueryExecutionFactory.sparqlService(endpoint, x)) {
            ResultSet rs = qExec.execSelect() ;
            while(rs.hasNext() ) {
                System.out.println(rs.next()) ;
            }
        }
    }
}

