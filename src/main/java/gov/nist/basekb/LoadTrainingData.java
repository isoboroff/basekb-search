package gov.nist.basekb;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class LoadTrainingData {

    private final IndexReader reader;
    private final IndexSearcher searcher;

    public LoadTrainingData(IndexReader r, IndexSearcher s) {
        reader = r;
        searcher = s;
    }

    public void load(String filename) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(filename));
        String line = null;
        while ((line = in.readLine()) != null) {
            String[] fields = line.split("\t");
            System.out.println("[Query: " + fields[0] + "] [KBid: " + fields[1] + "] [type: " + fields[2] + "]");
        }
    }

    public static void main(String args[]) throws IOException {
        FreebaseTools tools = new FreebaseTools(args);
        LoadTrainingData loader = new LoadTrainingData(tools.getIndexReader(), tools.getIndexSearcher());
    }
}
