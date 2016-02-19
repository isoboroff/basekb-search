// -*- Mode: Java -*-

/*--------------------------- BEGIN LICENSE BLOCK ---------------------------+
|                                                                            |
| Version: MPL 1.1/GPL 2.0/LGPL 2.1                                          |
|                                                                            |
| The contents of this file are subject to the Mozilla Public License        |
| Version 1.1 (the "License"); you may not use this file except in           |
| compliance with the License. You may obtain a copy of the License at       |
| http://www.mozilla.org/MPL/                                                |
|                                                                            |
| Software distributed under the License is distributed on an "AS IS" basis, |
| WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License   |
| for the specific language governing rights and limitations under the       |
| License.                                                                   |
|                                                                            |
| The Original Code is the Knowledge Resolver System.                        |
|                                                                            |
| The Initial Developer of the Original Code is                              |
| UNIVERSITY OF SOUTHERN CALIFORNIA, INFORMATION SCIENCES INSTITUTE          |
| 4676 Admiralty Way, Marina Del Rey, California 90292, U.S.A.               |
|                                                                            |
| Portions created by the Initial Developer are Copyright (C) 2010-2015      |
| the Initial Developer. All Rights Reserved.                                |
|                                                                            |
| Contributor(s):                                                            |
|                                                                            |
| Alternatively, the contents of this file may be used under the terms of    |
| either the GNU General Public License Version 2 or later (the "GPL"), or   |
| the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),   |
| in which case the provisions of the GPL or the LGPL are applicable instead |
| of those above. If you wish to allow use of your version of this file only |
| under the terms of either the GPL or the LGPL, and not to allow others to  |
| use your version of this file under the terms of the MPL, indicate your    |
| decision by deleting the provisions above and replace them with the notice |
| and other provisions required by the GPL or the LGPL. If you do not delete |
| the provisions above, a recipient may use your version of this file under  |
| the terms of any one of the MPL, the GPL or the LGPL.                      |
|                                                                            |
+---------------------------- END LICENSE BLOCK ----------------------------*/

// Version: $Id$
//
// Author: Hans Chalupsky


// Toolkit to index, host, query and search Freebase and its variants (e.g., BaseKB) as a Lucene index.
// See the README file for more information.

// Libraries we need:
// - lucene-5.2.1/core/lucene-core-5.2.1.jar
// - lucene-5.2.1/analysis/common/lucene-analyzers-common-5.2.1.jar
// - lucene-5.2.1/queryparser/lucene-queryparser-5.2.1.jar
// - args4j/2.0.23/args4j-2.0.23.jar
// - openrdf-sesame-2.8.4-onejar.jar (for triples escape/unescape only)
//
// compile like this (assumes the above libraries in `lib'):
// % javac -cp '.:bin:lib/*' -d bin FreebaseTools.java
//
// run like this (more examples in the README):
// % java -cp '.:bin:lib/*' edu.isi.kres.FreebaseTools -T data/freebase-rdf-latest.shrink.sort.gz  -I data/freebase-rdf-latest.shrink.sort.index  -c index -f -v      // 56min, 13GB index
// % java -cp '.:bin:lib/*' edu.isi.kres.FreebaseTools -T data/freebase-rdf-latest.shrink.sort.gz  -I data/freebase-rdf-latest.shrink.sort.index  -c index -f -o -v   // 71min, 13GB index
// % java -cp '.:bin:lib/*' edu.isi.kres.FreebaseTools -T data/basekb-gold-jan-2015.shrink.sort.gz -I data/basekb-gold-jan-2015.shrink.sort.index -c index -f -o -v   // 51min, 11GB index
// % java -cp '.:bin:lib/*' edu.isi.kres.FreebaseTools -I data/basekb-gold-jan-2015.shrink.sort.index -c lookup -q f_m.0h54qv8 -v
//
// these search commands assume an existing index properly configured in the config.dat file:
// % ./fbt-lookup.sh -q f_m.0h54qv8 -v
// % ./fbt-search.sh -q 'Claude AND Parsons AND r_type:f_people.person'
// % ./fbt-search.sh -q 'Parsons AND r_type:f_people.person' -p subject -v
//
// Useful info about Lucene query syntax:
// - https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
//
// Multi-lingual info:
// - http://lucene.apache.org/core/5_2_1/analyzers-common/
// - http://lucenenet.apache.org/docs/3.0.3/dc/df9/class_lucene_1_1_net_1_1_analysis_1_1_per_field_analyzer_wrapper.html
// - http://stackoverflow.com/questions/5372543/lucene-multilingual-text-field
// - https://docs.lucidworks.com/display/lweug/Multilingual+Indexing+and+Search (tradeoffs of different approaches)
// - http://stackoverflow.com/questions/24757035/lucene-indexing-strategy-with-multilingual-support
// - other Chinese analyzer options:
//   - org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer (needs a version argument)
//   - org.apache.lucene.analysis.cn.ChineseAnalyzer (subsumed by StandardAnalyzer)
//
// TO DO:
// - more elegant API for easy programmatic initialization and search
// - escaping print mode that escapes UTF-8 characters back to N-Triples syntax


package gov.nist.basekb;

// Java:
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

// Lucene:
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

// Spark
import static spark.Spark.*;

// Pebble
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;

public class SearchServer {

    public int server_port = 8080;


	protected static Map<String, String> docToMap(Document doc) {
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

    public static void main(String[] args) {
        // FreebaseTools main shell command dispatch.
		SearchServer srv = new SearchServer();
        FreebaseTools tools = new FreebaseTools(args); 
        try {
            if (tools.showVersion) {
                System.err.println(tools.versionString);
                System.exit(0);
            }
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
			Pattern mid_pattern = Pattern.compile("(f_m\\.[0-9a-z_]+)");
            get("/lookup/:subject", (req, res) -> {
				tools.getIndexReader();
				Map<String, Object> context = new HashMap<>();
				int docid = tools.getSubjectDocID(req.params(":subject"));
				if (docid < 0) {
					halt(404, "Subject not found");
				} else {
					Document doc = tools.getDocumentInMode(docid);
					StringWriter bufw = new StringWriter();
					PrintWriter out = new PrintWriter(bufw);
					tools.printSubjectAllPredicates(doc, -1, out);
					String textdoc = mid_pattern.matcher(bufw.getBuffer()).replaceAll("<a href=\"/lookup/$1\">$1</a>");
					// s{(f_m.[0-9a-z_]*)}{<a http="/lookup/$1">}g
					context.put("text", textdoc);
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
				StringWriter bufw = new StringWriter();
				PrintWriter out = new PrintWriter(bufw);
				tools.getIndexSearcher();
				tools.getIndexAnalyzer();
				Query q = new QueryParser(tools.getDefaultSearchField(),
										  tools.getIndexAnalyzer()).parse(req.queryParams("q"));

				TopDocs results = tools.getIndexSearcher().search(q, 100);
				ScoreDoc[] hits = results.scoreDocs;
				int numTotalHits = results.totalHits;
				List<Map<String, String>> docmaps = new ArrayList<Map<String, String>>(hits.length);
				String[] fulldoc = new String[hits.length];

				Map<String, Object> context = new HashMap<>();
				context.put("query", q);
				context.put("totalHits", numTotalHits);
				context.put("hits", hits);
				
				for (int i = 0; i < hits.length; i++) {
					bufw.getBuffer().setLength(0);
					int docid = hits[i].doc;
					float score = hits[i].score;
					Document doc = tools.getDocumentInMode(docid);
					tools.printSubjectAllPredicates(doc, score, out);
					fulldoc[i] =  mid_pattern.matcher(bufw.getBuffer()).replaceAll("<a href=\"/lookup/$1\">$1</a>");
					docmaps.add(docToMap(doc));
				}
				context.put("fulldoc", fulldoc);
				context.put("docmap", docmaps);

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
