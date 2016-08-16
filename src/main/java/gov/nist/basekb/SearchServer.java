// -*- Mode: Java -*-

package gov.nist.basekb;

// Java:

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static spark.Spark.*;

// Guava goodies
// Lucene:
// Spark
// Pebble

public class SearchServer {

    @Option(name = "-c", aliases = {"--config"}, usage = "FreebaseTools config file")
    public String config_file_path = "/Users/soboroff/basekb/basekb-search/config.dat";

    @Option(name = "-p", aliases = {"--port"}, usage = "Port for service (default 8080)")
    public int server_port = 8080;

    @Option(name = "-i", aliases = {"--index"}, usage = "Index location")
    public String index_path = "/Users/soboroff/basekb/basekb-index";

    @Option(name = "-d", aliases = {"--search-depth"}, usage = "Depth for first-pass search results")
    public int search_depth = 1000;

    @Option(name = "-l", aliases = {"--label-db"}, usage = "Path to object-to-label database")
    public String label_db_path = null;

    public static Map<String, String> docToMap(Document doc) {
        Map<String, String> m = new LinkedHashMap<>();
        for (IndexableField field : doc.getFields()) {
            String key = field.name();
            String val = field.stringValue();
            if (m.containsKey(key)) {
                val = m.get(key) + ", " + val;
            }
            m.put(key, val);
        }
        return m;
    }

    public static String getFirstEnglishValue(Document doc, String key) {
        for (String v : doc.getValues(key)) {
            if (v.endsWith("@en")) {
                return v;
            }
        }
        return null;
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

    public SearchServer(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println("Usage:");
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

    // A LongFieldSource that returns a default value when a Document is missing the field.
    public static class SafeLongFieldSource extends LongFieldSource {
        public SafeLongFieldSource(String field) {
            super(field);
        }

        protected NumericDocValues getNumericDocValues(LeafReaderContext readerContext, String field) throws IOException {
            NumericDocValues ndv = readerContext.reader().getNumericDocValues(field);
            if (ndv == null) {
                ndv = new NumericDocValues() {
                    @Override
                    public long get(int docid) {
                        return 1;
                    }
                };
            }
            return ndv;
        }

        protected Bits getDocsWithField(LeafReaderContext readerContext, String field) throws IOException {
            Bits bits = readerContext.reader().getDocsWithField(field);
            if (bits == null) {
                bits = new Bits.MatchNoBits(1);
            }
            return bits;
        }

        @Override
        public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            final NumericDocValues arr = getNumericDocValues(readerContext, field);
            final Bits valid = getDocsWithField(readerContext, field);

            return new LongDocValues(this) {
                @Override
                public long longVal(int doc) {
                    return arr.get(doc);
                }

                @Override
                public boolean exists(int doc) {
                    return arr.get(doc) != 0 || valid.get(doc);
                }

                @Override
                public Object objectVal(int doc) {
                    return valid.get(doc) ? longToObject(arr.get(doc)) : null;
                }

                @Override
                public String strVal(int doc) {
                    return valid.get(doc) ? longToString(arr.get(doc)) : null;
                }

                @Override
                protected long externalToLong(String extVal) {
                    return SafeLongFieldSource.this.externalToLong(extVal);
                }

                @Override
                public ValueFiller getValueFiller() {
                    return new ValueFiller() {
                        private final MutableValueLong mval = newMutableValueLong();

                        @Override
                        public MutableValue getValue() {
                            return mval;
                        }

                        @Override
                        public void fillValue(int doc) {
                            mval.value = arr.get(doc);
                            mval.exists = mval.value != 0 || valid.get(doc);
                        }
                    };
                }

            };
        }
    }

    public static void main(String[] args) throws Exception {
        // FreebaseTools main shell command dispatch.
        SearchServer srv = new SearchServer(args);
        FreebaseIndexer index_tools = new FreebaseIndexer(srv.config_file_path);
        index_tools.INDEX_DIRECTORY_NAME = srv.index_path;
        FreebaseSearcher search_tools = new FreebaseSearcher(index_tools);
        Ranker ranker = new MultiFieldRanker(search_tools.getIndexSearcher(), index_tools.getIndexAnalyzer(), srv.search_depth);

        if (srv.label_db_path != null) {
            search_tools.setLabelDb(srv.label_db_path);
        }

        try {
            if (index_tools.SHOW_DEBUG) {
                index_tools.printLog("DBG: cmdline args:");
                for (String arg : args)
                    index_tools.printLog(" " + arg);
                index_tools.printlnLog();
                index_tools.printlnLog("DBG: configuration:");
            }

            staticFileLocation("/public");
            port(srv.server_port);
            int min_threads = 2;
            int max_threads = 8;
            int timeoutmillis = 30000;
            threadPool(max_threads, min_threads, timeoutmillis);
            PebbleEngine engine = new PebbleEngine.Builder().build();

            PebbleTemplate main_template = engine.getTemplate("templates/main.peb");
            get("/", (req, res) -> {
                StringWriter buf = new StringWriter();
                main_template.evaluate(buf);
                return buf.toString();
            });

            PebbleTemplate disp_template = engine.getTemplate("templates/disp.peb");
            get("/lookup/:subject", (req, res) -> {
                search_tools.getIndexReader();
                LongFormRenderer doc_renderer = new LongFormRenderer();
                Map<String, Object> context = new HashMap<>();
                int docid = search_tools.getSubjectDocID(req.params(":subject"));
                if (docid < 0) {
                    halt(404, "Subject not found");
                } else {
                    Document doc = search_tools.getDocumentInMode(docid);
                    StringWriter bufw = new StringWriter();
                    doc_renderer.render(doc, bufw);
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
            get("/search", (req, res) -> {
                res.header("Content-Encoding", "gzip");
                Search s = new Search(ranker, search_tools);
                HashMap context = s.do_search(req);
                StringWriter bufw = new StringWriter();

                serp_template.evaluate(bufw, context);
                return bufw.toString();
            });

            get("/search.json", (req, res) -> {
                res.header("Content-Encoding", "gzip");
                Search s = new Search(ranker, search_tools);
                HashMap context = s.do_search(req);
                ObjectMapper mapper = new ObjectMapper();
                return mapper.writeValueAsString(context);
            });
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
