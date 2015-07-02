/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package graphbuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
//import org.apache.lucene.search.ScoreDoc;
//import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Collector;

/**
 *
 * @author deppych
 */
public class LuceneHandler {
    
    private  IndexReader reader = null;
    private IndexSearcher searcher = null;
    private QueryParser parser = null;
    public static final String TIMEFRAME_ID_FIELD_NAME = "tf";
    public static final String SOURCE_USER_FIELD_NAME = "src";
    public static final String DEST_USER_FIELD_NAME = "dest";
    public static final String WEIGHT_FIELD_NAME = "weight";
    public static final String REL_TYPE_FIELD_NAME = "type";
    
    public LuceneHandler(String lucenePath) throws CorruptIndexException, IOException{
         reader = IndexReader.open(FSDirectory.open(new File(lucenePath))); // only searching, so read-only=true
         searcher = new IndexSearcher(reader);
    }
    
    
    public void finalizeIndex() throws IOException {
        reader.close();
    }

                
//    public boolean luceneHandler(String userId, StringBuilder userIdPair, StringBuilder typeOfEdge) {
//        
//        try {
//           
//            Document doc = new Document();
//            int i=0;
//            String source = null;
//            String destination = null;
//            String type = null;
//            boolean found = false;
//            while(i<reader.numDocs() && !found) {
//                doc = reader.document(i);
//                source = doc.get("src"); //Source
//                destination = doc.get("dest"); //Destination
//                type = doc.get("type"); //Type
//                if(userId.equals(source)) { //If user is source
//                    userIdPair.append(destination); //Return destination
//                    typeOfEdge.append(type);
//                    found = true;
//                }
//                else if(userId.equals(destination)) { //If user is destination
//                    userIdPair.append(source); //Return source
//                    typeOfEdge.append(type);
//                    found = true;
//                }
//                doc = null;
//                i++;
//            }
//            reader.close();
//            return true;
//        }
//        catch (IOException e) {
//            return false;
//        }
//    }
    
   
    
    
    public List<LuceneEdgeEntity> getUserRelationships(long tf, long usr, RelTypes type) throws IOException{
	
        BooleanQuery booleanQuery = new BooleanQuery();
        Query q1 = NumericRangeQuery.newLongRange(TIMEFRAME_ID_FIELD_NAME, 8, tf, tf, true, true);
        Query q21 = new TermQuery(new Term(SOURCE_USER_FIELD_NAME, Long.toString(usr)));
        Query q22 = new TermQuery(new Term(DEST_USER_FIELD_NAME, Long.toString(usr)));
        Query q3 = new TermQuery(new Term(REL_TYPE_FIELD_NAME, type.name()));

        BooleanQuery q2 = new BooleanQuery();
        q2.add(new BooleanClause(q21, BooleanClause.Occur.SHOULD));
        q2.add(new BooleanClause(q22, BooleanClause.Occur.SHOULD));

        booleanQuery.add(q1, BooleanClause.Occur.MUST);
        booleanQuery.add(q2, BooleanClause.Occur.MUST);
        booleanQuery.add(q3, BooleanClause.Occur.MUST);
        IntegralCollector coll = new IntegralCollector(); // my custom Collector
        searcher.search( booleanQuery, coll);
        List<LuceneEdgeEntity> array = new ArrayList<LuceneEdgeEntity>();
        Document doc;
        for(int i = 0; i < coll.docs().size(); i++ ){
            doc = searcher.doc(coll.docs().get(i));
            array.add(createEdgeEntity(doc));
        }
        return array;
    }
    
    
    
    	public List<LuceneEdgeEntity> getEdgesInTimeframe(long tf, RelTypes type) throws IOException{
		
		BooleanQuery booleanQuery = new BooleanQuery();
		Query q1 = NumericRangeQuery.newLongRange(TIMEFRAME_ID_FIELD_NAME, 8, tf, tf, true, true);
		Query q2 = new TermQuery(new Term(REL_TYPE_FIELD_NAME, type.name()));
		booleanQuery.add(q1, BooleanClause.Occur.MUST);
		booleanQuery.add(q2, BooleanClause.Occur.MUST);
		IntegralCollector coll = new IntegralCollector(); // my custom Collector
		searcher.search( booleanQuery, coll);
		List<LuceneEdgeEntity> array = new ArrayList<LuceneEdgeEntity>();
		Document doc;
		for(int i = 0; i < coll.docs().size(); i++ ){
		    doc = searcher.doc(coll.docs().get(i));
		    array.add(createEdgeEntity(doc));
		}
		return array;
	}
        
    private LuceneEdgeEntity createEdgeEntity(Document doc){
            long tf = 0;
            Fieldable tff = doc.getFieldable(TIMEFRAME_ID_FIELD_NAME);
            if(tff != null){
                tf = Long.parseLong(tff.stringValue());
            }
            long src = Long.parseLong(doc.get(SOURCE_USER_FIELD_NAME));
            long dest = Long.parseLong(doc.get(DEST_USER_FIELD_NAME));
            Fieldable w = doc.getFieldable(WEIGHT_FIELD_NAME);
            double weight = 0;
            if(w != null){
                weight = Double.parseDouble(w.stringValue());
            }
            RelTypes type = RelTypes.valueOf(doc.get(REL_TYPE_FIELD_NAME));
            return new LuceneEdgeEntity(tf, src, dest, weight, type);
	}
   
    
    class IntegralCollector extends Collector {
	
		private int _docBase;

	    private List<Integer> _docs = new ArrayList<Integer>();
	    
	    public List<Integer> docs() {
	         return _docs;
	    }

	    @Override
	    public void collect(int doc) throws IOException{
	    	if (!reader.isDeleted(doc))
		       _docs.add(_docBase + doc);
	    }
	    
	    @Override
	    public void setNextReader(IndexReader reader, int docBase) throws IOException{
	        _docBase = docBase;
	    }
	    
	    @Override
	    public void setScorer(Scorer scorer) throws IOException {
	    }

		@Override
		public boolean acceptsDocsOutOfOrder() {
			 return true;
		}

	}
}
