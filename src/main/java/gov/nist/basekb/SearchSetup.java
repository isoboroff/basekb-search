package gov.nist.basekb;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;

import cc.mallet.classify.Classifier;
import cc.mallet.pipe.Pipe;
import cc.mallet.types.Labeling;
import gov.nist.basekb.SearchServer.Comparators;
import spark.Request;


public class SearchSetup {
	
	public FreebaseIndexer fbi;
	public FreebaseSearcher tools;
	public EntityRenderer abbrev;
	public LongFormRenderer full;
	public Classifier tmpclass;
	public ObjectInputStream ois;
	public Joiner joiner;
	public Ranker r;
	public Pipe pipe;
	public final Classifier classifier;
	public StringWriter bufw;
    public Map<String, Object> context;
    public String qstring;
    public ObjectMapper mapper;
    public String contextJSON;
    public String primaryType;

	public SearchSetup(SearchServer srv) throws Exception
	{
		fbi = new FreebaseIndexer(srv.index_path);
		fbi.INDEX_DIRECTORY_NAME = srv.index_path;
		tools = new FreebaseSearcher(fbi);
		abbrev = new EntityTypeRenderer(tools);
		full = new LongFormRenderer();
		tmpclass = null;
		ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(srv.classifier_path)));
		tmpclass = (Classifier) ois.readObject();
		classifier = tmpclass;
        ois.close();
        pipe = classifier.getInstancePipe();
        
        joiner = Joiner.on(", ");
        r = new MultiFieldRanker(tools.getIndexSearcher(), fbi.getIndexAnalyzer(), srv.search_depth);
        primaryType = "PER";
	}
	public void setup(SearchServer srv, Request req)
	{
		bufw = new StringWriter();
        context = new HashMap<>();
        qstring = req.queryParams("q");
        try{
            TopDocs results = r.rank(qstring);
            ScoreDoc[] hits = results.scoreDocs;
            int numTotalHits = results.totalHits;
            LinkedHashMap<String, ArrayList<HashMap<String, String>>> disp_docs = new LinkedHashMap<String, ArrayList<HashMap<String, String>>>();
            String types[] = {"PER", "ORG", "GPE", "LOC", "FAC", "OTHER"};
            
            int docidOne = hits[0].doc;
            float scoreOne = hits[0].score;
            Document docOne = tools.getDocumentInMode(docidOne);
            Labeling labsOne = srv.classify(docOne, classifier);
            primaryType = labsOne.getBestLabel().toString();
            disp_docs.put(primaryType, new ArrayList(hits.length));
            for (String t : types) {
            	if(t.equalsIgnoreCase(primaryType)==false)
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
            mapper = new ObjectMapper();
            contextJSON = mapper.writeValueAsString(context);
            
        	}catch (Exception e) {
        		System.err.println("ERROR: " + e.getMessage());
        		System.exit(1);
        	}
        
	}
	public FreebaseIndexer getFbi() {
		return fbi;
	}
	public void setFbi(FreebaseIndexer fbi) {
		this.fbi = fbi;
	}
	public FreebaseSearcher getTools() {
		return tools;
	}
	public void setTools(FreebaseSearcher tools) {
		this.tools = tools;
	}
	public EntityRenderer getAbbrev() {
		return abbrev;
	}
	public void setAbbrev(EntityRenderer abbrev) {
		this.abbrev = abbrev;
	}
	public LongFormRenderer getFull() {
		return full;
	}
	public void setFull(LongFormRenderer full) {
		this.full = full;
	}
	public Classifier getTmpclass() {
		return tmpclass;
	}
	public void setTmpclass(Classifier tmpclass) {
		this.tmpclass = tmpclass;
	}
	public ObjectInputStream getOis() {
		return ois;
	}
	public void setOis(ObjectInputStream ois) {
		this.ois = ois;
	}
	public Joiner getJoiner() {
		return joiner;
	}
	public void setJoiner(Joiner joiner) {
		this.joiner = joiner;
	}
	public Ranker getR() {
		return r;
	}
	public void setR(Ranker r) {
		this.r = r;
	}
	public Pipe getPipe() {
		return pipe;
	}
	public void setPipe(Pipe pipe) {
		this.pipe = pipe;
	}
	public StringWriter getBufw() {
		return bufw;
	}
	public void setBufw(StringWriter bufw) {
		this.bufw = bufw;
	}
	public Map<String, Object> getContext() {
		return context;
	}
	public void setContext(Map<String, Object> context) {
		this.context = context;
	}
	public String getQstring() {
		return qstring;
	}
	public void setQstring(String qstring) {
		this.qstring = qstring;
	}
	public ObjectMapper getMapper() {
		return mapper;
	}
	public void setMapper(ObjectMapper mapper) {
		this.mapper = mapper;
	}
	public String getContextJSON() {
		return contextJSON;
	}
	public void setContextJSON(String contextJSON) {
		this.contextJSON = contextJSON;
	}
	public Classifier getClassifier() {
		return classifier;
	}

	
}
