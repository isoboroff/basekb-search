package gov.nist.basekb;

import com.google.common.base.Joiner;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedReader;
import java.io.FileReader;


public class DumpTypesForMallet {

    public static String versionString = "1.0";

    private final FreebaseTools tools;

    @Option(name="-q", aliases={"--qrels"}, usage="Queries and kbid file")
    String qrels_file = "/Users/soboroff/basekb/edl/mention-kbid-type.tab";

    @Option(name="-i", aliases={"--index"}, usage="Index location")
    String index_location = "/Users/soboroff/basekb/basekb-shrink-un-so-index.3";

    @Option(name="-c", aliases={"--config"}, usage="FreebaseTools configuration file")
    String config_file = "/Users/soboroff/basekb/basekb-search/config.dat";

    @Option(name="-h", aliases={"--help"}, usage="Show usage")
    boolean showUsage = false;

    public DumpTypesForMallet(FreebaseTools fbt) {
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


    public void load() throws Exception {
        BufferedReader in = new BufferedReader(new FileReader(qrels_file));
        String line = null;

        while ((line = in.readLine()) != null) {
            String[] fields = line.split("\t");
            // System.out.println("# [Query: " + fields[0] + "] [KBid: " + fields[1] + "] [type: " + fields[2] + "]");
            String lookup = "f_" + fields[1];

            // execute a Lucene query for the entity, get back 10 docs
            int docid = tools.getSubjectDocID(lookup);
            if (docid == -1) {
                System.out.println("# kbid not found: " + lookup);
                continue;
            }
            Document d = tools.getDocumentInMode(docid);
            String[] types = d.getValues("r_type");

            System.out.print(fields[1] + " " + fields[2]);
            for (String t : types) {
                System.out.print(" " + t);
            }
            System.out.println();
        }

        // OTHER crap
        QueryParser qps = new QueryParser("r_type", tools.getIndexAnalyzer());
        IndexSearcher searcher = tools.getIndexSearcher();
        IndexReader reader = tools.getIndexReader();
        Joiner join = Joiner.on(" ");
        final String[] queries = {"f_type.content", "f_music.recording", "f_tv.tv_series_episode",
                "f_music.album", "f_film.film", "f_time.event", "f_book.book", "f_award.award_nominated_work",
                "f_fictional_universe.fictional_character", "f_media_common.cataloged_instance", "f_media_common.creative_work"};
        for (String querystring : queries) {
            Query q = qps.parse(querystring);
            TopDocs td = searcher.search(q, 20);
            ScoreDoc[] sd = td.scoreDocs;
            for (ScoreDoc scoredoc : sd) {
                Document d = tools.getDocumentInMode(scoredoc.doc);
                String types = join.join(d.getValues("r_type"));
                String name = tools.getSubjectName(d);
                System.out.println(name + " OTHER " + types);
            }
        }
    }

    public static void main(String args[]) throws Exception {

        FreebaseTools tools = new FreebaseTools();
        tools.readConfig();

        DumpTypesForMallet loader = new DumpTypesForMallet(tools);
        loader.setup(args);
        loader.load();
    }
}
