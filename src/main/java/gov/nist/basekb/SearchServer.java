// -*- Mode: Java -*-

package gov.nist.basekb;

// Java:
import com.google.common.base.Joiner;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import static spark.Spark.*;

// Guava goodies
// Lucene:
// Spark
// Pebble

public class SearchServer {

    public int server_port = 8080;

	public static Map<String, String> docToMap(Document doc) {
		Map<String, String> m = new LinkedHashMap<>();
		for (IndexableField field: doc.getFields()) {
			String key = field.name();
			String val = field.stringValue();
			if (m.containsKey(key)) {
				val = m.get(key) + ", " + val;
			}
			m.put(key, val);
		}
		return m;	  
	}

    public static void printSubjectVerbose(Document subject, float score, String indent,
											  FreebaseTools tools, PrintWriter out) throws IOException {
        // Pretty-print everything we know about `subject' to `out'.
        // Annotate its `score' if it is non-negative.
        if (score >= 0.0)
            out.println(tools.getSubjectName(subject) + ": [score=" + score + "]");
        else
            out.println(tools.getSubjectName(subject) + ":");
        for (IndexableField field : subject.getFields()) {
            if (! tools.FIELD_NAME_SUBJECT.equals(field.name())) {
				out.print(indent + field.name() + ": " + tools.normalizeNewlines(field.stringValue()));
				if (field.stringValue().startsWith("f_m.")) {
					int docid = tools.getSubjectDocID(field.stringValue());
					if (docid > 0) {
						Document new_subj = tools.getDocumentInMode(docid);
					INNER: for (IndexableField new_field : new_subj.getFields()) {
							if ((new_field.name().equals("rs_label") ||
								 new_field.name().equals("f_type.object.name")) &&
								(new_field.stringValue().endsWith("@en"))) {
								out.print(" (" + tools.normalizeNewlines(new_field.stringValue()) + ")");
								break INNER;
							}
						}
					}
				}
				out.println("");
			}
        }
    }

	public static String getFirstEnglishValue(Document doc, String key) {
		for (String v: doc.getValues(key)) {
			if (v.endsWith("@en")) {
				return v;
			}
		}
		return null;
	}
	
	public static void render_short(Document doc, float score, PrintWriter out) {
		if (score > 0.0) {
			out.println(doc.get("subject") + ": [" + score + "]");
		} else {
			out.println(doc.get("subject") + ":");
		}
		out.println("    " + getFirstEnglishValue(doc, "rs_label"));
		out.println("    " + getFirstEnglishValue(doc, "f_common.topic.description"));
	}

	public static class Comparators {
		public static final Comparator<HashMap<String, String>> SCORE =
			(HashMap<String, String> d1, HashMap<String, String> d2) -> {
			return Double.compare(Double.parseDouble(d2.getOrDefault("score", "0.0")),
						   Double.parseDouble(d1.getOrDefault("score", "0.0")));
		};
		public static final Comparator<HashMap<String, String>> PRBIN =
			(HashMap<String, String> d1, HashMap<String, String> d2) -> {
			int i1 = 0, i2 = 0;
			String pr1 = d1.get("pr_bin");
			if (pr1 != null)
				i1 = Integer.parseInt(pr1);
			String pr2 = d2.get("pr_bin");
			if (pr2 != null)
				i2 = Integer.parseInt(pr2);
			return Integer.compare(i2, i1);
		};
		public static final Comparator<HashMap<String, String>> PR_BIN_SCORE =
			(HashMap<String, String> d1, HashMap<String, String> d2) -> {
			return PRBIN.thenComparing(SCORE).compare(d1, d2);
		};
	}
	
    public static void main(String[] args) {
        // FreebaseTools main shell command dispatch.
		SearchServer srv = new SearchServer();
        FreebaseTools tools = new FreebaseTools();
		EntityRenderer abbrev = new EntityTypeRenderer(tools);
		LongFormRenderer full = new LongFormRenderer();
		
        try {
            if (tools.showDebug) {
                tools.printLog("DBG: cmdline args:");
                for (String arg : args)
                    tools.printLog(" " + arg);
                tools.printlnLog();
                tools.getConfig("dummy");
                tools.printlnLog("DBG: configuration:");
                for (Map.Entry<Object, Object> entry : tools.config.entrySet())
                    tools.printlnLog("DBG:    " + entry.getKey() + "=" + entry.getValue());
            }

			staticFileLocation("/public");
			port(srv.server_port);
			PebbleEngine engine = new PebbleEngine.Builder().build();

			PebbleTemplate main_template = engine.getTemplate("templates/main.peb");
			get("/", (req, res) -> {
				StringWriter buf = new StringWriter();
				main_template.evaluate(buf);
				return buf.toString();
			});

			PebbleTemplate disp_template = engine.getTemplate("templates/disp.peb");
			get("/lookup/:subject", (req, res) -> {
				tools.getIndexReader();
				Map<String, Object> context = new HashMap<>();
				int docid = tools.getSubjectDocID(req.params(":subject"));
				if (docid < 0) {
					halt(404, "Subject not found");
				} else {
					Document doc = tools.getDocumentInMode(docid);
					StringWriter bufw = new StringWriter();
					full.render(doc, bufw);
					context.put("text", bufw.toString());
					context.put("doc", docToMap(doc));
					context.put("docid", docid);
					context.put("subject", req.params(":subject"));

					bufw.getBuffer().setLength(0);
					disp_template.evaluate(bufw, context);
					return bufw.toString();
				}
				return null;
			});
			
			PebbleTemplate serp_template = engine.getTemplate("templates/serp.peb");
			Joiner joiner = Joiner.on(", ");
            get("/search", (req, res) -> {
				StringWriter bufw = new StringWriter();
				tools.getIndexSearcher();
				tools.getIndexAnalyzer();
				String qstring = req.queryParams("q");
				QueryParser qps = new QueryParser(tools.getDefaultSearchField(), tools.getIndexAnalyzer());
				
				Query q = qps.parse(qstring);
				if (!qstring.matches("[\"+()-]")) {
					// plain string query; expand with a phrase
					BooleanQuery.Builder bb = new BooleanQuery.Builder();
					bb.add(q, BooleanClause.Occur.SHOULD);
					bb.add(qps.parse("\"" + qstring + "\""), BooleanClause.Occur.SHOULD);
					q = bb.build();
				}

				// Attempt 1 at integrating Pagerank bins.  This appears to cause sorting on docid when
				// pr_bin is absent, unclear how it interacts with scoring
				// SortField longSort = new SortedNumericSortField("pr_bin", SortField.Type.LONG, true);
				// Sort sort = new Sort(longSort);
				// TopDocs results = tools.getIndexSearcher().search(q, 100, sort);

				TopDocs results = tools.getIndexSearcher().search(q, 100);
				ScoreDoc[] hits = results.scoreDocs;
				int numTotalHits = results.totalHits;
				ArrayList<HashMap<String, String>> disp_docs = new ArrayList(hits.length);

				Map<String, Object> context = new HashMap<>();
				context.put("query", q);
				context.put("totalHits", numTotalHits);
				context.put("hits", hits);
				context.put("docs", disp_docs);
				
				for (int i = 0; i < hits.length; i++) {
					bufw.getBuffer().setLength(0);
					int docid = hits[i].doc;
					float score = hits[i].score;
					Document doc = tools.getDocumentInMode(docid);
					abbrev.render(doc, bufw, score);

					HashMap<String, String> dmap = new HashMap();
					dmap.put("text", bufw.toString());
					dmap.put("subject", doc.get("subject"));
					dmap.put("types", joiner.join(doc.getValues("r_type")));
					dmap.put("label", getFirstEnglishValue(doc, "rs_label"));
					String pr = doc.get("pr_bin");
					if (pr == null)
						pr = "0";
					dmap.put("pr_bin", pr);
					dmap.put("score", Double.toString(hits[i].score));
					disp_docs.add(dmap);
				}

				Collections.sort(disp_docs, Comparators.PR_BIN_SCORE);
				bufw.getBuffer().setLength(0);
				serp_template.evaluate(bufw, context);
				return bufw.toString();
			});
        }
        catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }
}
