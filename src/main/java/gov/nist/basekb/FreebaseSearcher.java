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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;

// Args4J:
// Lucene:
// Sesame:


public class FreebaseSearcher {

       //
    ////// Configuration options
     //

    public int MAX_HITS = 10;

    public boolean NORMALIZE_NEWLINES = false;
    public String PRINT_MODE = "all";
    public String DEFAULT_SEARCH_FIELD = "text@en";

    // If true, predicate chains will not loop back to their seeds:
    public boolean SUPPRESS_PREDICATE_LOOPS = true;

    public String FREEBASE_TOOLS_HOME = "";

    String getDefaultSearchField() {
        // If no field is given, get it from the configuration or use a default value.
        if (DEFAULT_SEARCH_FIELD == null)
            DEFAULT_SEARCH_FIELD = "rs_label";
        return fbi.normalizeUri(DEFAULT_SEARCH_FIELD);
    }

    public static final String PRINT_MODE_ALL     = "all";
    public static final String PRINT_MODE_SUBJECT = "subject";

    public static final String FIELD_NAME_SUBJECT = "subject";
    public static final String FIELD_NAME_TEXT    = "text";

    public FreebaseIndexer fbi;

       //
    ////// Top-level Constructors and Command Dispatch
     //

    public FreebaseSearcher(FreebaseIndexer fbi) {
        this.fbi = fbi;
    }

    public String getFreebaseToolsHome() {
        // Intuit the home directory of the toolkit based on the location of the class file.
        //return new File(FreebaseTools.class.getResource("FreebaseTools.class").getPath()).getParentFile().getParentFile().getParentFile().getParentFile().getParent();
        return FREEBASE_TOOLS_HOME;
    }

    Directory indexDirectory = null;
    IndexReader indexReader = null;
    IndexSearcher indexSearcher = null;

    public void initializeIndexSearch() throws IOException {
        // Initialize index structures for search.
        if (indexDirectory == null)
            indexDirectory = FSDirectory.open(Paths.get(fbi.getIndexDirectoryName()));
    }

    public IndexReader getIndexReader() throws IOException {
        // Get the current index reader or create it.
        if (indexReader == null) {
            initializeIndexSearch();
            indexReader = DirectoryReader.open(indexDirectory);
        }
        return indexReader;
    }

    public IndexSearcher getIndexSearcher() throws IOException {
        // Get the current index searcher or create it.
        if (indexSearcher == null)
            indexSearcher = new IndexSearcher(getIndexReader());
        return indexSearcher;
    }


    public static class PagerankSimilarity extends Similarity {
        private final Similarity sim;

        public PagerankSimilarity(Similarity sim) {
            this.sim = sim;
        }

        @Override
        public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
            final SimScorer sub = sim.simScorer(weight, context);
            final SortedNumericDocValues pr = context.reader().getSortedNumericDocValues("pr_bin");

            return new SimScorer() {
                @Override
                public float score(int doc, float freq) {
                    if (pr == null)
                        return sub.score(doc, freq);
                    else
                        return (float) pr.valueAt(doc) * sub.score(doc, freq);
                }
                @Override
                public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
                    return sub.computePayloadFactor(doc, start, end, payload);
                }
                @Override
                public float computeSlopFactor(int distance) {
                    return sub.computeSlopFactor(distance);
                }
            };
        }

        @Override
        public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
            return sim.computeWeight(collectionStats, termStats);
        }

        @Override
        public long computeNorm(FieldInvertState state) {
            return sim.computeNorm(state);
        }
    }

       //
    ////// Subject/Predicate API
     //

    public int getSubjectDocID(String subjectURI) throws IOException {
        // Retrieve the document ID identified by `subjectURI', return -1 if nothing was found.
        // Not sure if a TermQuery is the fastest way to do a primary key lookup in Lucene.
        TermQuery query = new TermQuery(new Term(FIELD_NAME_SUBJECT, subjectURI));
        TopDocs result = getIndexSearcher().search(query, 1);
        if (result.totalHits == 0)
            return -1;
        else {
            return result.scoreDocs[0].doc;
        }
    }

    public Document getSubjectDoc(String subjectURI) throws IOException {
        // Retrieve the document identified by `subjectURI', return null if nothing was found.
        int subjectId = getSubjectDocID(subjectURI);
        return (subjectId < 0) ? null : getIndexReader().document(subjectId);
    }

    public String getSubjectName(Document subjectDoc) throws IOException {
        // Return the string name of `subjectDoc'.
        return subjectDoc.get(FIELD_NAME_SUBJECT);
    }

    public String getSubjectPredicateValue(Document subjectDoc, String predName) throws IOException {
        // Return the value of predicate `predName' on `subjectDoc'.  If there are muliple values,
        // return the first one indexed, if there are none, return null.
        return subjectDoc.get(predName);
    }

    public String[] getSubjectPredicateValues(Document subjectDoc, String predName) throws IOException {
        // Return the values of predicate `predName' on `subjectDoc' in the order they were indexed.
        // If there are none, return an empty array.
        return subjectDoc.getValues(predName);
    }

    public String getSubjectPredicateValue(String subjectURI, String predName) throws IOException {
        // Return the value of predicate `predName' on `subjectURI'.  If there are muliple values,
        // return the first one indexed, if there are none, return null.
        // This is specialized to only retrieve the `predName' field of the subject document.
        // If the full document has already been retrieved, use the Document accessor instead.
        int subjectId = getSubjectDocID(subjectURI);
        if (subjectId < 0)
            return null;
        else {
            DocumentStoredFieldVisitor fieldVisitor = new DocumentStoredFieldVisitor(predName);
            getIndexReader().document(subjectId, fieldVisitor);
            Document subject = fieldVisitor.getDocument();
            return getSubjectPredicateValue(subject, predName);
        }
    }

    String[] emptyValues = new String[0];

    public String[] getSubjectPredicateValues(String subjectURI, String predName) throws IOException {
        // Return the values of predicate `predName' on `subjectURI'.
        // If there are none, return an empty array.
        // This is specialized to only retrieve the `predName' field of the subject document.
        // If the full document has already been retrieved, use the Document accessor instead.
        int subjectId = getSubjectDocID(subjectURI);
        if (subjectId < 0)
            return emptyValues;
        else {
            DocumentStoredFieldVisitor fieldVisitor = new DocumentStoredFieldVisitor(predName);
            getIndexReader().document(subjectId, fieldVisitor);
            Document subject = fieldVisitor.getDocument();
            return getSubjectPredicateValues(subject, predName);
        }
    }


       //
    ////// Printing Results
     //

    public void printSubject(Document subject, float score, PrintWriter out) throws IOException {
        // Print `subject''s name to `out', also print its `score' if it is non-negative.
        if (score >= 0.0) {
            out.print(getSubjectName(subject));
            out.print("\t");
            out.println(score);
        }
        else
            out.println(getSubjectName(subject));
    }

    public void printSubjectAllPredicates(Document subject, float score, PrintWriter out) throws IOException {
        // Pretty-print everything we know about `subject' to `out'.
        // Annotate its `score' if it is non-negative.
        if (score >= 0.0)
            out.println(getSubjectName(subject) + ": [score=" + score + "]");
        else
            out.println(getSubjectName(subject) + ":");
        for (IndexableField field : subject.getFields()) {
            if (! FIELD_NAME_SUBJECT.equals(field.name()))
                out.println("    " + field.name() + ": " + fbi.normalizeNewlines(field.stringValue()));
        }
    }

    // Printing predicate chains:

    List<String> valueRow = new ArrayList<String>();

    public List<String> getValueRow() {
        // Use a shared value row buffer for now.
        return valueRow;
    }

    public void pushValueRow(List<String> row, String value) {
        row.add(value);
    }

    public void popValueRow(List<String> row) {
        row.remove(row.size()-1);
    }

    public void printValueRow(List<String> row, float score, String[] predicateChain, PrintWriter out) {
        out.print(row.get(0));
        if (score >= 0.0) {
            out.print("\t");
            out.print(score);
        }
        int nValues = row.size();
        for (int i = 1; i < nValues; i++) {
            out.print("\t");
            out.print(predicateChain[i-1]);
            out.print("\t");
            out.print(fbi.normalizeNewlines(row.get(i)));
        }
        out.println();
    }

    public void printPredicateChain(String subjectUri, float score, String[] predicateChain, int start, List<String> row, PrintWriter out) throws IOException {
        // Helper for `printSubjectPredicates' to print the tail of a predicate chain.
        if (SUPPRESS_PREDICATE_LOOPS && row.contains(subjectUri))
            // don't print value chains that lead back to an earlier seed value:
            return;
        pushValueRow(row, subjectUri);
        if (start >= predicateChain.length)
            printValueRow(row, score, predicateChain, out);
        else {
            String pred = predicateChain[start];
            String[] values = getSubjectPredicateValues(subjectUri, pred);
            if (values.length > 0) {
                for (String value : values) {
                    printPredicateChain(value, score, predicateChain, start+1, row, out);
                }
            }
            else {
                pushValueRow(row, "null");
                printValueRow(row, score, predicateChain, out);
                popValueRow(row);
            }
        }
        popValueRow(row);
    }

    public void printSubjectPredicates(Document subject, float score, String[][] predicates, PrintWriter out) throws IOException {
        // Print all of `subject's `predicates' that have values to `out'.
        // Handles predicate value chains if necessary.
        // Also print `score' after `subject' if it is non-negative.

        String subjectName = getSubjectName(subject);
        List<String> row = getValueRow();
        pushValueRow(row, subjectName);
        for (String[] chain : predicates) {
            String pred = chain[0];
            String[] values = getSubjectPredicateValues(subject, pred);

            if (values.length > 0) {
                for (String value : values)
                    printPredicateChain(value, score, chain, 1, row, out);
            }
            else {
                pushValueRow(row, "null");
                printValueRow(row, score, chain, out);
                popValueRow(row);
            }
        }
        popValueRow(row);
    }


    public String[] parsePredicateList(String predicateList) {
        // Parse a white-space or comma-separated `predicateList' into its components.
        return predicateList.trim().split("(\\s|,)+");
    }

    public String[] parsePredicateChain(String predicateChain) {
        // Parse a >-separated `predicateChain' into its components.
        // Since `>' is a URI-delimiter, it won't show up in predicate names.
        return predicateChain.split(">");
    }

    String[][] printModePredicates = null;    // list of predicate chains

    public String[][] getPrintModePredicates() {
        if (printModePredicates == null) {
            String[] preds = parsePredicateList(PRINT_MODE);
            printModePredicates = new String[preds.length][];
            for (int i = 0; i < preds.length; i++)
                printModePredicates[i] = parsePredicateChain(preds[i]);
        }
        return printModePredicates;
    }

    HashSet<String> printModeFields = null;  // list of fields to restrict Document access to, only used for top-level queries

    public Set<String> getPrintModeFields() {
        if ((printModeFields == null) && PRINT_MODE_ALL.equals(PRINT_MODE))
            return null;
        else if (printModeFields == null) {
            printModeFields = new HashSet<String>(Arrays.asList(FIELD_NAME_SUBJECT));
            for (String[] pred : getPrintModePredicates())
                printModeFields.add(pred[0]);
        }
        return printModeFields;
    }

    public void printSubjectInMode(Document subject, float score, String mode, PrintWriter out) throws IOException {
        // Print `subject' and `score' onto `out' according to `mode'.
        if (PRINT_MODE_SUBJECT.equals(mode))
            printSubject(subject, score, out);
        else if (PRINT_MODE_ALL.equals(mode))
            printSubjectAllPredicates(subject, score, out);
        else
            printSubjectPredicates(subject, score, getPrintModePredicates(), out);
    }

    public Document getDocumentInMode(int docid) throws IOException {
        // Access a Document from its `docid' according to the current print mode.
        // This ensures we are materializing the minimal number of document fields necessary.
        Set<String> displayFields = getPrintModeFields();
        if (displayFields == null)
            return getIndexReader().document(docid);
        else
            return getIndexReader().document(docid, displayFields);
    }


       //
    ////// Command Implementations
     //

    public void doLookup(String queryString) {
        try {
            long timeSetup = 0;
            long timeQuery = 0;
            long timeDisplay = 0;

            // force index initialization, so we get a better sense of the actual query execution time:
            fbi.printlnProg("Loading index...");
            long timeStart = System.currentTimeMillis();
            getIndexReader();
            timeSetup += (System.currentTimeMillis() - timeStart);

            int blockSize = 1000;
            BufferedReader in = queryString.equals("-") ? new BufferedReader(new InputStreamReader(System.in)) : null;
            List<String> subjects = (in != null) ? fbi.readList(in, blockSize) : Arrays.asList(parsePredicateList(queryString));
            PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))); // Java I/O API is insane
            boolean fullDisplay = PRINT_MODE_ALL.equals(PRINT_MODE);

            while (true) {
                for (String subject : subjects) {
                    // now run the query:
                    timeStart = System.currentTimeMillis();
                    int docid = getSubjectDocID(subject);
                    if (docid < 0) {
                        timeQuery += (System.currentTimeMillis() - timeStart);
                        fbi.printlnLog("Not found: " + subject);
                    }
                    else {
                        Document doc = getDocumentInMode(docid);
                        timeQuery += (System.currentTimeMillis() - timeStart);
                        timeStart = System.currentTimeMillis();
                        printSubjectInMode(doc, -1, PRINT_MODE, out);
                        if (fullDisplay)
                            out.println();
                        timeDisplay += (System.currentTimeMillis() - timeStart);
                    }
                }
                if (in == null)
                    break;
                subjects = fbi.readList(in, blockSize);
                if (subjects.isEmpty())
                    break;
            }
            out.flush();
            fbi.printlnProg("Run time: setup=" + timeSetup + "ms, query=" + timeQuery + "ms, display=" + timeDisplay + "ms");
            getIndexReader().close();
        }
        catch (Exception e) {
            fbi.log.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }

    public void doSearch(String queryString) {
        try {
            long timeSetup = 0;
            long timeQuery = 0;
            long timeDisplay = 0;
            int maxHits = MAX_HITS;

            // force index initialization, so we get a better sense of the actual query execution time:
            fbi.printlnProg("Loading index...");
            long timeStart = System.currentTimeMillis();
            getIndexSearcher();
            fbi.getIndexAnalyzer();
            timeSetup += (System.currentTimeMillis() - timeStart);
            // fbi.printlnProg("Index contains " + fbi.getIndexNumDocs() + " documents");

            timeStart = System.currentTimeMillis();
            fbi.printlnDbg("DBG: query string: " + queryString);
            Query query = new QueryParser(getDefaultSearchField(), fbi.getIndexAnalyzer()).parse(queryString);
            fbi.printlnProg("Searching for: " + query.toString(getDefaultSearchField()));
            boolean allResults = (maxHits == -1);
            int nDocs = allResults ? 1 : maxHits;
            TopDocs results = getIndexSearcher().search(query, nDocs);
            ScoreDoc[] hits = results.scoreDocs;

            int numTotalHits = results.totalHits;
            fbi.printlnProg("Found " + numTotalHits + " matching subject(s)");
            if (allResults) {
                nDocs = numTotalHits;
                fbi.printlnProg("Collecting results...");
                results = getIndexSearcher().search(query, numTotalHits);
                hits = results.scoreDocs;
            }
            timeQuery = System.currentTimeMillis() - timeStart;

            timeStart = System.currentTimeMillis();
            fbi.printlnProg("Printing results...");
            PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))); // Java I/O API is insane
            int end = Math.min(hits.length, nDocs);
            boolean fullDisplay = PRINT_MODE_ALL.equals(PRINT_MODE);

            for (int i = 0; i < end; i++) {
                // this takes about 90s for 3M subjects names, so not super fast but ok (0.03ms per subject)
                int docid = hits[i].doc;
                float score = allResults ? -1.0f : hits[i].score;
                Document doc = getDocumentInMode(docid);
                printSubjectInMode(doc, score, PRINT_MODE, out);
                if (fbi.SHOW_DEBUG) out.flush();
                fbi.printlnDbg("DBG: score explanation: " + getIndexSearcher().explain(query, docid).toString());
                if (fullDisplay)
                    out.println();
            }
            out.flush();
            timeDisplay = System.currentTimeMillis() - timeStart;
            fbi.printlnProg("Run time: setup=" + timeSetup + "ms, query=" + timeQuery + "ms, display=" + timeDisplay + "ms");
            getIndexReader().close();
        }
        catch (Exception e) {
            fbi.log.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }



}
