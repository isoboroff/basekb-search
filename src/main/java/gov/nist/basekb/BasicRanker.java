package gov.nist.basekb;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;

/**
 * Provide a basic default Lucene search on the rs_label field.
 */
public class BasicRanker extends Ranker {

    IndexSearcher is;
    Analyzer a;

    public BasicRanker(IndexSearcher is, Analyzer a, int search_depth) {
        super(search_depth);
        this.is = is;
        this.a = a;
    }

    @Override
    public TopDocs rank(String qstring) throws Exception {
        QueryParser qps = new QueryParser("text", a);
        TopDocs results = is.search(qps.parse(qstring), search_depth);
        return results;
    }
}
