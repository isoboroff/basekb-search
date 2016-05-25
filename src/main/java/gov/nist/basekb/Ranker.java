package gov.nist.basekb;

import org.apache.lucene.search.TopDocs;

/**
 * Created by soboroff on 5/3/16.
 */
public abstract class Ranker {

    public int search_depth = 100;

    public Ranker(int search_depth) {
        this.search_depth = search_depth;
    }

    public abstract TopDocs rank(String qstring) throws Exception;

}
