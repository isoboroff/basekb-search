package gov.nist.basekb;

import com.google.common.base.Joiner;
import gov.nist.basekb.SearchServer.Comparators;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import spark.Request;

import java.io.StringWriter;
import java.util.*;


public class Search {

    public Ranker ranker;
    public FreebaseSearcher search_tools;
    public EntityRenderer renderer;

    public Search(Ranker _ranker, FreebaseSearcher _tools) {
        ranker = _ranker;
        search_tools = _tools;
        renderer = new EntityTypeRenderer(search_tools);
    }

    public HashMap<String, Object> do_search(Request req) {
        StringWriter bufw = new StringWriter();
        HashMap<String, Object> context = new HashMap<>();
        Joiner joiner = Joiner.on(" ");

        String qstring = req.queryParams("q");
        try {
            TopDocs results = ranker.rank(qstring);
            ScoreDoc[] hits = results.scoreDocs;
            int numTotalHits = results.totalHits;
            LinkedHashMap<String, ArrayList<HashMap<String, String>>> disp_docs = new LinkedHashMap<String, ArrayList<HashMap<String, String>>>();
            String types[] = {"PER", "ORG", "GPE", "LOC", "FAC", "OTHER"};
            for (String t : types) {
                disp_docs.put(t, new ArrayList(hits.length));
            }

            context.put("query", qstring);
            context.put("totalHits", numTotalHits);
            context.put("hits", hits);
            context.put("docs", disp_docs);

            for (int i = 0; i < hits.length; i++) {
                bufw.getBuffer().setLength(0);
                int docid = hits[i].doc;
                float score = hits[i].score;
                Document doc = search_tools.getDocumentInMode(docid);

                String type = doc.get("best_class");
                if (type == null) { type = "OTHER"; }
                ArrayList<HashMap<String, String>> this_dispdocs = disp_docs.get(type);
                if (this_dispdocs == null) {
                    this_dispdocs = new ArrayList<HashMap<String, String>>(hits.length);
                    disp_docs.put(type, this_dispdocs);
                }

                renderer.render(doc, bufw, score);

                HashMap<String, String> dmap = new HashMap<>();
                dmap.put("text", bufw.toString());
                dmap.put("subject", doc.get("subject"));
                dmap.put("types", joiner.join(doc.getValues("r_type")));
                dmap.put("label", SearchServer.getFirstEnglishValue(doc, "rs_label"));

                String pr = doc.get("pr_bin");
                if (pr == null)
                    pr = "0";
                dmap.put("pr_bin", pr);
                dmap.put("score", Double.toString(hits[i].score));
                this_dispdocs.add(dmap);
            }

            int first_nonzero_type_count = 0;
            String first_nonzero_type = "";
            for (Map.Entry<String, ArrayList<HashMap<String, String>>> disp_pair : disp_docs.entrySet()) {
                String this_type = disp_pair.getKey();
                ArrayList this_dispdocs = disp_pair.getValue();
                Collections.sort(this_dispdocs, Comparators.SCORE);
                if (first_nonzero_type_count == 0 && this_dispdocs.size() > 0) {
                    first_nonzero_type_count = this_dispdocs.size();
                    first_nonzero_type = this_type;
                }
            }
            context.put("first_type", first_nonzero_type);

        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }

        return context;
    }
}
