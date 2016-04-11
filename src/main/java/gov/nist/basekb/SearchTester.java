package gov.nist.basekb;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.kohsuke.args4j.Argument;

import java.io.BufferedReader;
import java.io.FileReader;


public class SearchTester {
	public static String versionString = "SearchTester 1.0";
	
    @Argument(required = true, usage = "Query file.")
    public String queryString = "";

    public FreebaseTools fbt;

    public SearchTester(String[] initArgs) {
        fbt = new FreebaseTools();
    }

	public void run() throws Exception {
        BufferedReader queries = new BufferedReader(new FileReader(queryString));
        String line = null;
        int num_q = 0;
        double sum_rr = 0.0;
        while ((line = queries.readLine()) != null) {
            // This is my tabbed file from TAC TEDL
            num_q ++;
            String[] fields = line.split("\t");
            String query = fields[0];
            String kbid = "f_" + fields[1];

            TopDocs hits = do_search(query, fbt);
            ScoreDoc[] scoreDocs = hits.scoreDocs;
            int rank = 0;
            for (ScoreDoc sd : scoreDocs) {
                rank++;
                Document d = fbt.getDocumentInMode(sd.doc);
                String doc_kbid = d.get(fbt.FIELD_NAME_SUBJECT);
                System.out.println("--" + rank + " " + doc_kbid + " :: " + kbid);
                if (doc_kbid.equals(kbid))
                    break;
            }
            System.out.println(num_q + " " + rank);
            if (rank > 0 && rank < scoreDocs.length)
                sum_rr += 1.0 / rank;
        }
        double mrr = sum_rr / num_q;
        System.out.println(num_q + " " + mrr);
	}

    protected TopDocs do_search(String query, FreebaseTools fbt) throws Exception {
        IndexSearcher searcher = fbt.getIndexSearcher();
        QueryParser qp = new QueryParser(FreebaseTools.FIELD_NAME_TEXT, fbt.getIndexAnalyzer());
        Query q = qp.parse(query);
        TopDocs hits = searcher.search(q, 100);
        return hits;
    }

    public static void main(String[] args) throws Exception {
        SearchTester jig = new SearchTester(args);
        jig.run();
	}
}
