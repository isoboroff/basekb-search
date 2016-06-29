// -*- Mode: Java -*-

package gov.nist.basekb;

// Java:

import cc.mallet.classify.Classifier;
import cc.mallet.pipe.Pipe;
import cc.mallet.types.Instance;
import cc.mallet.types.Labeling;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        FreebaseIndexer fbi = new FreebaseIndexer(srv.index_path);
        fbi.INDEX_DIRECTORY_NAME = srv.index_path;
        FreebaseSearcher tools = new FreebaseSearcher(fbi);

        EntityRenderer abbrev = new EntityTypeRenderer(tools);
        LongFormRenderer full = new LongFormRenderer();

        Classifier tmpclass = null;
        ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(srv.classifier_path)));
        tmpclass = (Classifier) ois.readObject();
        final Classifier classifier = tmpclass;
        ois.close();

        Pipe pipe = classifier.getInstancePipe();

        try {
            if (fbi.SHOW_DEBUG) {
                fbi.printLog("DBG: cmdline args:");
                for (String arg : args)
                    fbi.printLog(" " + arg);
                fbi.printlnLog();
                fbi.printlnLog("DBG: configuration:");
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
            Ranker r = new MultiFieldRanker(tools.getIndexSearcher(), fbi.getIndexAnalyzer(), srv.search_depth);
            get("/search", (req, res) -> {
                StringWriter bufw = new StringWriter();
                String qstring = req.queryParams("q");

                TopDocs results = r.rank(qstring);
                ScoreDoc[] hits = results.scoreDocs;
                int numTotalHits = results.totalHits;
                LinkedHashMap<String, ArrayList<HashMap<String, String>>> disp_docs = new LinkedHashMap<String, ArrayList<HashMap<String, String>>>();
                String types[] = {"PER", "ORG", "GPE", "LOC", "FAC", "OTHER"};
                for (String t : types) {
                    disp_docs.put(t, new ArrayList(hits.length));
                }

                Map<String, Object> context = new HashMap<>();
                context.put("query", qstring);
                context.put("totalHits", numTotalHits);
                context.put("hits", hits);
                context.put("docs", disp_docs);

                for (int i = 0; i < hits.length; i++) {
                    bufw.getBuffer().setLength(0);
                    int docid = hits[i].doc;
                    float score = hits[i].score;
                    Document doc = tools.getDocumentInMode(docid);

                    Labeling labs = srv.classify(doc, classifier);
                    String type = labs.getBestLabel().toString();
                    ArrayList<HashMap<String, String>> this_dispdocs = disp_docs.get(type);
                    if (this_dispdocs == null) {
                        this_dispdocs = new ArrayList<HashMap<String, String>>(hits.length);
                        disp_docs.put(type, this_dispdocs);
                    }

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
                bufw.getBuffer().setLength(0);
                serp_template.evaluate(bufw, context);
                return bufw.toString();
            });
            get("/search.json", (req, res) -> {
                StringWriter bufw = new StringWriter();
                String qstring = req.queryParams("q");

                TopDocs results = r.rank(qstring);
                ScoreDoc[] hits = results.scoreDocs;
                int numTotalHits = results.totalHits;
                LinkedHashMap<String, ArrayList<HashMap<String, String>>> disp_docs = new LinkedHashMap<String, ArrayList<HashMap<String, String>>>();
                String types[] = {"PER", "ORG", "GPE", "LOC", "FAC", "OTHER"};
                for (String t : types) {
                    disp_docs.put(t, new ArrayList(hits.length));
                }
                
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> context = new HashMap<>();
                context.put("query", qstring);
                context.put("totalHits", numTotalHits);
                context.put("hits", hits);
                context.put("docs", disp_docs);
                
                
                for (int i = 0; i < hits.length; i++) {
                    bufw.getBuffer().setLength(0);
                    int docid = hits[i].doc;
                    float score = hits[i].score;
                    Document doc = tools.getDocumentInMode(docid);

                    Labeling labs = srv.classify(doc, classifier);
                    String type = labs.getBestLabel().toString();
                    ArrayList<HashMap<String, String>> this_dispdocs = disp_docs.get(type);
                    if (this_dispdocs == null) {
                        this_dispdocs = new ArrayList<HashMap<String, String>>(hits.length);
                        disp_docs.put(type, this_dispdocs);
                    }

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
                
                String contextJSON = mapper.writeValueAsString(context);                
                System.out.println(contextJSON+"\n");
                try{
                	mapper.writeValue(new File("/home/ram7/Desktop/test.json"), context);
                } catch (Exception e){
                	e.printStackTrace();
                }
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
