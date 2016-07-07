// -*- Mode: Java -*-

package gov.nist.basekb;

// Java:

import cc.mallet.classify.Classifier;
import cc.mallet.pipe.Pipe;
import cc.mallet.types.Instance;
import cc.mallet.types.Labeling;
import com.google.common.base.Joiner;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.util.*;

import static spark.Spark.*;

// Guava goodies
// Lucene:
// Spark
// Pebble

public class SearchServer {

    @Option(name="-c", aliases={"--config"}, usage="FreebaseTools config file")
    public String config_file_path = "/Users/soboroff/basekb/basekb-search/config.dat";

    @Option(name="-p", aliases={"--port"}, usage="Port for service (default 8080)")
    public int server_port = 8080;

    @Option(name="-i", aliases={"--index"}, usage="Index location")
    public String index_path = "/Users/soboroff/basekb/basekb-index";

    @Option(name="-m", aliases={"--classifier"}, usage="MALLET classifier for entity types")
    public String classifier_path = "/Users/soboroff/basekb/basekb-search/enttype.classifier";

    @Option(name="-d", aliases={"--search-depth"}, usage="Depth for first-pass search results")
    public int search_depth = 1000;

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
                                           FreebaseSearcher tools, PrintWriter out) throws IOException {
        // Pretty-print everything we know about `subject' to `out'.
        // Annotate its `score' if it is non-negative.
        if (score >= 0.0)
            out.println(tools.getSubjectName(subject) + ": [score=" + score + "]");
        else
            out.println(tools.getSubjectName(subject) + ":");
        for (IndexableField field : subject.getFields()) {
            if (! tools.FIELD_NAME_SUBJECT.equals(field.name())) {
                out.print(indent + field.name() + ": " + tools.fbi.normalizeNewlines(field.stringValue()));
                if (field.stringValue().startsWith("f_m.")) {
                    int docid = tools.getSubjectDocID(field.stringValue());
                    if (docid > 0) {
                        Document new_subj = tools.getDocumentInMode(docid);
                        INNER: for (IndexableField new_field : new_subj.getFields()) {
                            if ((new_field.name().equals("rs_label") ||
                                    new_field.name().equals("f_type.object.name")) &&
                                    (new_field.stringValue().endsWith("@en"))) {
                                out.print(" (" + tools.fbi.normalizeNewlines(new_field.stringValue()) + ")");
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

    public SearchServer(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        }
        catch (CmdLineException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println("Usage:");
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

    public Labeling classify(Document doc, Classifier classifier) {
        Pipe p = classifier.getInstancePipe();
        Joiner join = Joiner.on(" ");
        String data = join.join(doc.getValues("r_type"));
        String name = doc.get("rs_label");
        Instance i = new Instance(data, null, name, null);
        i = p.instanceFrom(i);
        Labeling lab = classifier.classify(i).getLabeling();
        return lab;
    }

    public static class SafeLongFieldSource extends LongFieldSource {
        public SafeLongFieldSource(String field) {
            super(field);
        }

        protected NumericDocValues getNumericDocValues(LeafReaderContext readerContext, String field) throws IOException {
            NumericDocValues ndv = readerContext.reader().getNumericDocValues(field);
            if (ndv == null) {
                ndv = new NumericDocValues() {
                    @Override
                    public long get(int docid) { return 1; }
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
        SearchSetup ss = new SearchSetup(srv);

        try {
            if (ss.getFbi().SHOW_DEBUG) {
                ss.getFbi().printLog("DBG: cmdline args:");
                for (String arg : args)
                    ss.getFbi().printLog(" " + arg);
                ss.getFbi().printlnLog();
                ss.getFbi().printlnLog("DBG: configuration:");
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
                ss.getTools().getIndexReader();
                Map<String, Object> context = new HashMap<>();
                int docid = ss.getTools().getSubjectDocID(req.params(":subject"));
                if (docid < 0) {
                    halt(404, "Subject not found");
                } else {
                    Document doc = ss.getTools().getDocumentInMode(docid);
                    StringWriter bufw = new StringWriter();
                    ss.getFull().render(doc, bufw);
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
		ss.setup(srv, req);
                ss.getBufw().getBuffer().setLength(0);
                serp_template.evaluate(ss.getBufw(), ss.getContext());
                return ss.getBufw().toString();
            });
	    get("/search.json", (req, res) -> {
                res.header("Content-Encoding", "gzip");
		ss.setup(srv, req);
                ss.getBufw().getBuffer().setLength(0);
                serp_template.evaluate(ss.getBufw(), ss.getContext());
                return ss.getContextJSON();
            });
        }
        catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }
}
