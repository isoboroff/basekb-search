package gov.nist.basekb;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedReader;
import java.io.FileReader;


public class PredictType {

    public static String versionString = "1.0";

    private final FreebaseTools tools;

    @Option(name="-q", aliases={"--qrels"}, usage="Queries and kbid file")
    String qrels_file = "/Users/soboroff/basekb/edl/mention-kbid-type.tab";

    @Option(name="-n", aliases={"--counts"}, usage="Type counts file")
    String counts_file = "/Users/soboroff/basekb/basekb-search/types.uniq.pruned";

    @Option(name="-i", aliases={"--index"}, usage="Index location")
    String index_location = "/Users/soboroff/basekb/basekb-shrink-un-so-index.3";

    @Option(name="-c", aliases={"--config"}, usage="FreebaseTools configuration file")
    String config_file = "/Users/soboroff/basekb/basekb-search/config.dat";

    @Option(name="-h", aliases={"--help"}, usage="Show usage")
    boolean showUsage = false;

    public PredictType(FreebaseTools fbt) {
        tools = fbt;
    }

    public void setup(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            if (showUsage) {
                System.err.println(versionString);
                System.err.println("Usage:");
                parser.printUsage(System.err);
                System.exit(0);
            }
        }
        catch (CmdLineException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println("Usage:");
            parser.printUsage(System.err);
            System.exit(1);
        }

        tools.INDEX_DIRECTORY_NAME = index_location;
    }


    public void run() throws Exception {
        HashMultimap<String, String> typemap = HashMultimap.create();
        BufferedReader in = new BufferedReader(new FileReader(counts_file));
        String line = null;
        while ((line = in.readLine()) != null) {
            String[] typedata = line.split(" ");
            typemap.put(typedata[1], typedata[0]);
        }
        in.close();

        in = new BufferedReader(new FileReader(qrels_file));
        QueryParser qps = new QueryParser(FreebaseTools.FIELD_NAME_SUBJECT, tools.getIndexAnalyzer());
        IndexSearcher searcher = tools.getIndexSearcher();
        IndexReader reader = tools.getIndexReader();
        Joiner.MapJoiner joiner = Joiner.on(", ").withKeyValueSeparator(" = ");

        int count = 0;
        int correct = 0;
        while ((line = in.readLine()) != null) {
            count++;
            String[] fields = line.split("\t");
            System.out.println("# [Query: " + fields[0] + "] [KBid: " + fields[1] + "] [type: " + fields[2] + "]");
            String lookup = "f_" + fields[1];
            String actual_type = fields[2];

            // execute a Lucene query for the entity, get back 10 docs
            int docid = tools.getSubjectDocID(lookup);
            if (docid == -1) {
                System.out.println("# kbid not found: " + lookup);
                continue;
            }
            Document d = tools.getDocumentInMode(docid);
            String[] types = d.getValues("r_type");
            HashMultiset<String> typecount = HashMultiset.create(4);
            for (String t : types) {
                if (typemap.containsKey(t))
                    for (String tt : typemap.get(t))
                        typecount.add(tt);
            }
            if (typecount.size() > 0) {
                String guess_type = Multisets.copyHighestCountFirst(typecount).entrySet().asList().get(0).getElement();
                System.out.print(actual_type + ", guessing " + guess_type + " [");
                for (Multiset.Entry<String> me : typecount.entrySet()) {
                    System.out.print(me.getElement() + " = " + me.getCount() + " ");
                }
                System.out.println("]");

                if (actual_type.equals(guess_type))
                    correct++;
            }
        }

        System.out.println(correct + " correct out of " + count + " = " + (float)correct/count);
    }

    public static void main(String args[]) throws Exception {

        FreebaseTools tools = new FreebaseTools();
        tools.readConfig();

        PredictType loader = new PredictType(tools);
        loader.setup(args);
        loader.run();
    }
}
