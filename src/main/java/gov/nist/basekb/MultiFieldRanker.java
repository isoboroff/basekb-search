package gov.nist.basekb;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

/**
 * Created by soboroff on 5/3/16.
 */
public class MultiFieldRanker extends BasicRanker {
    String[] query_fields = {"text@en", "text@es", "text@zh", "rs_label@en", "rs_label@es",
            "rs_label@zh", "text", "rs_label"};

    public MultiFieldRanker(IndexSearcher is, Analyzer a, int search_depth) {
        super(is, a, search_depth);
    }

    @Override
    public TopDocs rank(String qstring) throws Exception {
        QueryParser qps = new MultiFieldQueryParser(query_fields, a);

        // tools.getIndexSearcher().setSimilarity(new FreebaseTools.PagerankSimilarity(new ClassicSimilarity()));

        Query q = qps.parse(qstring);

        if (!qstring.matches("[\"+()-]")) {
            // plain string query; expand with a phrase
            BooleanQuery.Builder bb = new BooleanQuery.Builder();
            bb.add(q, BooleanClause.Occur.SHOULD);
            bb.add(qps.parse("\"" + qstring + "\""), BooleanClause.Occur.SHOULD);
            q = bb.build();
        }

        FunctionQuery pagerank_query = new FunctionQuery(new SearchServer.SafeLongFieldSource("pr_bin"));
        q = new CustomScoreQuery(q, pagerank_query);
        TopDocs results = is.search(q, search_depth);
        return results;
    }

    // Attempt 1 at integrating Pagerank bins.  This appears to cause sorting on docid when
    // pr_bin is absent, unclear how it interacts with scoring
    // SortField longSort = new SortedNumericSortField("pr_bin", SortField.Type.LONG, true);
    // Sort sort = new Sort(longSort);
    // TopDocs results = tools.getIndexSearcher().search(q, 100, sort);
}
