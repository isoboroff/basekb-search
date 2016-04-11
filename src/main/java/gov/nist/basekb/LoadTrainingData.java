package gov.nist.basekb;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.BufferedReader;
import java.io.FileReader;


public class LoadTrainingData {

    private final FreebaseTools tools;
    final static String qrels_file = "/Users/soboroff/basekb/edl/mention-kbid-type.tab";

    public LoadTrainingData(FreebaseTools fbt) {
        tools = fbt;
    }

    public void load(String filename) throws Exception {
        BufferedReader in = new BufferedReader(new FileReader(filename));
        String line = null;
        QueryParser qps = new QueryParser(FreebaseTools.FIELD_NAME_TEXT, tools.getIndexAnalyzer());
        IndexSearcher searcher = tools.getIndexSearcher();
        IndexReader reader = tools.getIndexReader();

        while ((line = in.readLine()) != null) {
            String[] fields = line.split("\t");
            System.out.println("[Query: " + fields[0] + "] [KBid: " + fields[1] + "] [type: " + fields[2] + "]");

            try {
                // execute a Lucene query for the entity, get back 10 docs
                Query q = qps.parse(fields[0]);
                TopDocs results = searcher.search(q, 10);
                ScoreDoc[] hits = results.scoreDocs;
                boolean found = false;
                long Ndocs = reader.numDocs();

                for (ScoreDoc sd : hits) {
                    //   - if d is the relevant doc, then found=true, this one's relevant.
                    boolean rel = false;
                    Document d = tools.getDocumentInMode(sd.doc);
                    String kbid = d.get("subject");
                    if (kbid.equals(fields[1])) {
                        found = true;
                        rel = true;
                    }
                    //   - get its termvector
                    Fields docfields = reader.getTermVectors(sd.doc);
                    //   - make it into what jforests wants
                    for (String f : docfields) {
                        TermsEnum t = docfields.terms(f).iterator();
                        BytesRef tstring;
                        while ((tstring = t.next()) != null) {
                            PostingsEnum pe = t.postings(null);
                            int i;
                            int df = t.docFreq();
                            while ((i = pe.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                                int freq = pe.freq();
                                double idf = Math.log(Ndocs / df);
                                double tf = 1 + Math.log(freq);
                                double tfidf = tf * idf;
                                // and that's the weight.
                            }
                        }
                    }

                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]) throws Exception {
        FreebaseTools tools = new FreebaseTools();
        LoadTrainingData loader = new LoadTrainingData(tools);
        loader.load(LoadTrainingData.qrels_file);
    }
}
