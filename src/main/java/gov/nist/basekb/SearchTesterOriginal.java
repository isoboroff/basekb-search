package gov.nist.basekb;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.Messages;

import cc.mallet.classify.Classifier;
import cc.mallet.types.Labeling;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;


public class SearchTesterOriginal {
	@Option(name="-d", aliases={"--search-depth"}, usage="Ranking depth (default 100)")
	int search_depth = 1000;

	@Argument
	List<String> posargs = new ArrayList<String>();


	public FreebaseSearcher fbs;
	public Ranker ranker;
	public String query_file;
	public String indexDirectory;

	public String classifierPath;
	public ObjectInputStream oisTest;
	public Classifier tmpclassTest;
	public final Classifier classifierTest;
	public SearchTesterOriginal(String[] args) throws Exception {
		CmdLineParser argparser = new CmdLineParser(this);
		String home = null;
		String ranker_name = null;
		tmpclassTest = null;
		try {
			argparser.parseArgument(args);

			if (posargs.size() != 5) {
				throw new CmdLineException(argparser, Messages.ILLEGAL_LIST, "Command line fail");
			}
			home = posargs.get(0); //pathname to home
			ranker_name = posargs.get(1);//to ranker (gov.nist.basekb)
			query_file = posargs.get(2);//txt file for queries
			indexDirectory = posargs.get(3);//pathname to index
			classifierPath = posargs.get(4);//pathname to enttype classifier
			if (home == null) {
				throw new CmdLineException(argparser, Messages.ILLEGAL_PATH, "No home given");
			}
			if (ranker_name == null) {
				throw new CmdLineException(argparser, Messages.ILLEGAL_OPERAND, "No ranker given");
			}
			if (query_file == null) {
				throw new CmdLineException(argparser, Messages.ILLEGAL_PATH, "No query file given");
			}
			if (indexDirectory == null) {
				throw new CmdLineException(argparser, Messages.ILLEGAL_PATH, "No index directory given");
			}
			if(classifierPath == null){
				throw new CmdLineException(argparser, Messages.ILLEGAL_PATH, "No classifier path given");
			}

		} catch (CmdLineException cle) {
			System.err.println(cle.getMessage());
			argparser.printUsage(System.err);
			System.err.println();
		}
		oisTest = new ObjectInputStream(new BufferedInputStream(new FileInputStream(classifierPath)));
		tmpclassTest = (Classifier) oisTest.readObject();
		classifierTest = tmpclassTest;
		oisTest.close();
		fbs = new FreebaseSearcher(new FreebaseIndexer(home, indexDirectory));
		Class myClass = Class.forName(ranker_name);
		Class[] types = {IndexSearcher.class, Analyzer.class, Integer.TYPE};
		Constructor constructor = myClass.getConstructor(types);
		ranker = (Ranker)constructor.newInstance(fbs.getIndexSearcher(), fbs.fbi.getIndexAnalyzer(), search_depth);
	}

	public void run() throws Exception {
		BufferedReader queries = new BufferedReader(new FileReader(query_file));
		String line = null;
		int num_q = 0;
		double sum_rr = 0.0;
		int zero_returns = 0;
		int fails = 0;
		while ((line = queries.readLine()) != null) {
		    // This is my tabbed file from TAC TEDL
		    num_q ++;
		    String[] fields = line.split("\t");
		    String query = fields[0];
		    String kbid = "f_" + fields[1];
		    int rank = 0;
		    int found_at_rank = -1;
		    System.out.println("++ " + kbid + " " + query);
		             try {
		                 TopDocs hits = ranker.rank(query);
		                 ScoreDoc[] scoreDocs = hits.scoreDocs;
		                 if (scoreDocs.length == 0) {
		                     zero_returns++;
		                 } else {
		                     for (ScoreDoc sd : scoreDocs) {
		                    	 if(rank<100){
		                         rank++;
		                         Document d = fbs.getDocumentInMode(sd.doc);
		                         String doc_kbid = d.get(fbs.FIELD_NAME_SUBJECT);
		                         System.out.print("--" + rank + " " + doc_kbid);
		                         if (doc_kbid.equals(kbid)) {
		                             System.out.print("!!");
		                             sum_rr += 1.0 / rank;
		                             found_at_rank = rank;
		                             System.out.println(num_q + " " + rank);
		                             break;
		                         } else {
		                             System.out.print("..");
		                         }
		                         System.out.println();
		                    	 }
		                     }
		                     if (found_at_rank == -1){
		                    	 fails++;
		                    	 System.out.println("FAIL!");
		                     }
		                 }
		             } catch (Exception e) {
		                 // move to next query
		             }
		 
		         }
		         double mrr = sum_rr / num_q;
		         System.out.println("num_q " + num_q);
		         System.out.println("mrr " + mrr);
		         System.out.println("fails " + fails);
		         System.out.println("zero_returns " + zero_returns);
	}

	public static void main(String[] args) throws Exception {
		// String home = args[0];
		// String ranker_name = args[1];
		// String query_file = args[2];


		SearchTesterOriginal tester = new SearchTesterOriginal(args);
		tester.run();
	}

}
