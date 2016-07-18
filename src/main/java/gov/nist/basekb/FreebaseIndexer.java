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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.openrdf.rio.ntriples.NTriplesUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class FreebaseIndexer {

       //
    ////// Configuration options
     //

    public boolean SHOW_DEBUG = false;
    public boolean SHOW_PROGRESS = true;
    public boolean INDEX_PREDICATES = true;
    public boolean INDEX_TEXT = true;
    public boolean INDEX_LANGUAGE = true;
    public String TRIPLES_FILE = "/Users/soboroff/basekb/basekb-triples.gz";
    public String INDEX_DIRECTORY_NAME = "/Users/soboroff/basekb/basekb-index";

    public boolean OPTIMIZE_INDEX = false;
    public boolean INDEX_FULL_DATA = true;
    public boolean INDEX_TERMVECTORS = false;

    public boolean NORMALIZE_NEWLINES = false;
    public String DEFAULT_ANALYZER = "org.apache.lucene.analysis.standard.StandardAnalyzer";

    public String FREEBASE_TOOLS_HOME = "";

    public static final String FIELD_NAME_SUBJECT = "subject";
    public static final String FIELD_NAME_TEXT    = "text";

    public static final String VALUE_TYPE_URI    = "URI";
    public static final String VALUE_TYPE_STRING = "STRING";
    public static final String VALUE_TYPE_TEXT   = "TEXT";
    public static final String VALUE_TYPE_INT    = "INT";
    public static final String VALUE_TYPE_OTHER  = "OTHER";

    public static String[] INDEXED_PREDICATES = {
            "<f_common.notable_for.display_name>",
            "<f_common.topic.alias>",
            "<f_common.topic.description>",
            "<f_type.object.name>",
            "<f_type.object.type>",
            "<r_type>",
            "<rs_label>",
    };

    public static String[] INDEXED_LANGUAGES = {"EN", "ES", "ZH"};

    public static HashMap<String, String> ANALYZER_MAP = new HashMap();
    static {
        ANALYZER_MAP.put("EN", "org.apache.lucene.analysis.en.EnglishAnalyzer");
        ANALYZER_MAP.put("ES", "org.apache.lucene.analysis.es.SpanishAnalyzer");
        ANALYZER_MAP.put("ZH", "org.apache.lucene.analysis.cjk.CJKAnalyzer");
    };

       //
    ////// Top-level Constructors and Command Dispatch
     //

    public FreebaseIndexer(String home) {
        FREEBASE_TOOLS_HOME = home;
    }
    public FreebaseIndexer(String home, String indexDirectory) {
        FREEBASE_TOOLS_HOME = home;
        INDEX_DIRECTORY_NAME = indexDirectory;
    }
       //
    ////// Utilities:
     //

    public PrintWriter log = new PrintWriter(new OutputStreamWriter(System.err, StandardCharsets.UTF_8), true);

    int maxMsg = 1000;

    public void printLog(String x) {
        log.print(x);
    }

    public void printlnLog(String x) throws IOException {
        maxMsg--;
        if (maxMsg < 0)
            throw new IOException("Too many log messages");
        log.println(x);
    }

    public void printlnLog() throws IOException {
        printlnLog("");
    }

    public void printlnDbg(String x) {
        if (SHOW_DEBUG)
            log.println(x);
    }

    public void printlnProg(String x) {
        if (SHOW_PROGRESS)
            log.println(x);
    }

    public void showProgress(long count, int defaultInterval) {
        if (SHOW_PROGRESS && ((count % defaultInterval) == 0)) {
            log.print(".");
            log.flush();
        }
    }


    public BufferedReader openTextFile(String filename) throws IOException {
        // Open a UTF8-encoded text input stream to the (potentially compressed) file `filename'.
        InputStream in = new FileInputStream(filename);
        if (filename.endsWith(".gz"))
            in = new GZIPInputStream(in);
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    public List<String> readList(BufferedReader in, long maxLines) throws IOException {
        // Read non-empty, non-commented lines from `in' and return the result in a list.
        // If `maxLines' is >= 0, remove at most that many items (supply -1 for unlimited lines).
        String line = null;
        List<String> list = new ArrayList<String>();

        while (maxLines != 0) {
            line = in.readLine();
            if (line == null)
                break;
            if ((line.startsWith("#")) || (line.matches("^\\s*$")))
                // skip comments and blank lines:
                continue;
            else {
                list.add(line);
                maxLines--;
            }
        }
        return list;
    }

    public List<String> readListFile(String filename) throws IOException {
        BufferedReader in = openTextFile(filename);
        List<String> list = readList(in, -1);
        in.close();
        return list;
    }

       //
    ////// Index Creation and Access
     //

    // Lucene compression info:
    // - http://blog.florian-hopf.de/2012/12/looking-at-plaintext-lucene-index.html
    // - https://lucene.apache.org/core/4_4_0/core/org/apache/lucene/codecs/compressing/CompressingStoredFieldsFormat.html
    // - https://lucene.apache.org/core/4_4_0/core/org/apache/lucene/codecs/compressing/package-summary.html
    // It looks like the default storage format compresses now, so we don't have to do anything special.

    public String getIndexDirectoryName() throws IOException {
        // Either return the provided directory or derive one based on `triplesFile'.
        if (! INDEX_DIRECTORY_NAME.equals(""))
            return INDEX_DIRECTORY_NAME;
        else if (! TRIPLES_FILE.equals(""))
            return TRIPLES_FILE + ".lucene.index";
        else
            throw new IOException("Cannot determine Lucene index directory, supply via -i");
    }

    // We assume we either are building a new index or querying it, but never both.
    // These variables cache various index-related objects for either reading or writing:
    Directory indexDirectory = null;
    IndexWriter indexWriter = null;
    Analyzer indexAnalyzer = null;
    Analyzer defaultIndexAnalyzer = null;

    public void initializeIndexBuilder() throws Exception {
        // Create a new index directory and writer to index a triples file.
        // Raise an error if an index already exists, so we don't accidentally overwrite it.
        String indexDir = getIndexDirectoryName();
        if ((new File(indexDir)).isDirectory())
            throw new IOException("Index directory already exists, remove it before indexing");

        indexDirectory = FSDirectory.open(Paths.get(indexDir));
        IndexWriterConfig iwc = new IndexWriterConfig(getIndexAnalyzer());

        // we always create a new index from scratch:
        iwc.setOpenMode(OpenMode.CREATE);
        iwc.setCodec(new Lucene54Codec(Mode.BEST_SPEED));          // the default
        //iwc.setCodec(new Lucene54Codec(Mode.BEST_COMPRESSION));  // slower, but better compression

        indexWriter = new IndexWriter(indexDirectory, iwc);
        indexAnalyzer = getIndexAnalyzer();

        if (INDEX_PREDICATES) printlnProg("Indexing individual predicates");
        if (INDEX_TEXT) printlnProg("Indexing combined predicate text values");
        if (INDEX_LANGUAGE) printlnProg("Indexing predicates for language(s): " + supportedLanguages);
    }

    public void finalizeIndexBuilder() throws IOException {
        if (OPTIMIZE_INDEX) {
            // To maximize search performance, we call forceMerge here (slow), but since we
            // are dealing with a totally static index, the cost should generally be worth it:
            printlnProg("Optimizing index...");
            indexWriter.forceMerge(1);
            indexWriter.commit();
        }
        indexWriter.close();
        indexDirectory = null;
        indexAnalyzer = null;
        indexWriter = null;
    }

    public Analyzer getDefaultIndexAnalyzer() throws Exception {
        // Get the current default index analyzer or create it.
        // Use the type specified by LUCENE_INDEX_ANALYZER_DEFAULT
        // or use StandardAnalyzer as the default.
        if (defaultIndexAnalyzer == null) {
            String defaultAnalyzerName = DEFAULT_ANALYZER;
            if (defaultAnalyzerName == null)
                defaultAnalyzerName = "org.apache.lucene.analysis.standard.StandardAnalyzer";
            defaultIndexAnalyzer = new SafetyAnalyzer((Analyzer)Class.forName(defaultAnalyzerName).newInstance());
        }
        return defaultIndexAnalyzer;
    }

    public Analyzer getIndexAnalyzer() throws Exception {
        // Get the current index analyzer or create it.
        if (indexAnalyzer == null) {
            Map<String, Analyzer> analyzersMap = createFieldAnalyzersMap();
            // if we use default indexing options, determine language indexing based on the configuration:
            if (supportedLanguages.size() > 0)
                INDEX_LANGUAGE = true;
            if (INDEX_LANGUAGE)
                indexAnalyzer = new PerFieldAnalyzerWrapper(getDefaultIndexAnalyzer(), analyzersMap);
            else
                indexAnalyzer = getDefaultIndexAnalyzer();
        }
        return indexAnalyzer;
    }

       //
    ////// Multi-Lingual Support
     //

    public String getValueLanguage(String value) {
        // If `value' has a `@<lang>' suffix identifying its language, return <lang>.
        // Otherwise return null.
        if (getValueType(value) == VALUE_TYPE_TEXT) {
            int len = value.length();
            int pos = value.lastIndexOf("\"@", len - 4);
            if (pos >= 0)
                return value.substring(pos + 2, len);
        }
        return null;
    }

    public String normalizeLanguage(String language) {
        // Normalizes a `language' designation appended to a Freebase text value string.
        // We do not perform any character substitutions (for now) and simply convert to lower case.
        return language.toLowerCase();
    }

    public String getLanguageRoot(String language) {
        // If `language' is a variant such as `es-419' or `zh-TW', return its root before the hyphen.
        // Otherwise, return `language'.
        int hyphenPos = language.indexOf('-');
        if (hyphenPos > 0)
            return language.substring(0, hyphenPos);
        else
            return language;
    }

    public String languageQualifiedPredicate(String predicate, String language) {
        // Convert `rs_label', etc. to `rs_label@zh' for example.
        // If `language' has a dedicated analyzer, use it as is, otherwise reduce it to its root.
        String normLang = normalizeLanguage(language);
        if (isSupportedLanguage(normLang))
            return predicate + "@" + normLang;
        else
            return predicate + "@" + getLanguageRoot(normLang);
    }

    Set<String> supportedLanguages = null;

    public boolean isSupportedLanguage(String language) {
        // Return true if the normalized `language' has a dedicated analyzer configured for it.
        return supportedLanguages.contains(language);
    }

    public Map<String, Analyzer> createFieldAnalyzersMap() throws Exception {
        // Create a language-specific field analyzers map for all indexed predicates according to the configuration.
        // Initialize `supportedLanguages' as a side-effect.
        Map<String, Analyzer> fieldAnalyzers = new HashMap<String, Analyzer>();
        supportedLanguages = new HashSet<String>();
        if (indexedPreds == null)
            initializeIndexedPreds();
        for (String lang : INDEXED_LANGUAGES) {
            supportedLanguages.add(normalizeLanguage(lang));
            String analyzer_name = ANALYZER_MAP.get(lang);
            if (analyzer_name == null) {
                printlnLog("No analyzer specified for language " + lang);
                continue;
            }
            Analyzer analyzer = new SafetyAnalyzer((Analyzer)Class.forName(analyzer_name).newInstance());
            for (String pred : indexedPreds) {
                String langPred = languageQualifiedPredicate(pred, lang);
                fieldAnalyzers.put(langPred, analyzer);
            }
        }

        printlnDbg("DBG: supported languages: " + supportedLanguages);
        return fieldAnalyzers;
    }


       //
    ////// Indexing Triples
     //

    Set<String> indexedPreds = null;

    void initializeIndexedPreds() throws IOException {
        indexedPreds = new HashSet<String>();
        for (String pred : INDEXED_PREDICATES) {
            indexedPreds.add(normalizeUri(pred));
        }
    }

    public boolean isIndexedPredicate(String predicate) throws IOException {
        // Return true if `predicate' should be indexed in addition to just being stored.
        // `predicate' is expected to be a normalized URI.
        if (indexedPreds == null)
            initializeIndexedPreds();
        return indexedPreds.contains(predicate);
    }

    public String getValueType(String value) {
        // Determine the type of this N-Triples `value'.
        char first = value.charAt(0);
		if (Character.isDigit(first)) {
			return VALUE_TYPE_INT;
		}
        switch (first) {
        case '<': return VALUE_TYPE_URI;
        case '"':
            if (value.charAt(value.length() - 1) == '"')
                return VALUE_TYPE_STRING;
            else
                return VALUE_TYPE_TEXT;
        default:
            return VALUE_TYPE_OTHER;
        }
    }

    public boolean isUri(String x) {
        // Return true if `x' is a URI.
        return x.startsWith("<");
    }

    public String normalizeUri(String uri) {
        // Do some URI normalization to make things go more smoothly with the query parser.
        // We want lower-case namespace IDs and no `:'s, since those make the query field parser unhappy.
        // That way we can freely mix text queries with exact field restrictions such as type names.
        // This is now done mostly by the preprocessing steps, and we only have to strip off angle brackets.
        if (uri.charAt(0) == '<')
            return uri.substring(1, uri.length() - 1);
        else
            return uri;
    }

    public String normalizeNewlines(String value) {
        if (NORMALIZE_NEWLINES)
            // this does't copy and returns `value' if it didn't contain a newline:
            return value.replace('\n', ' ');
        else
            return value;
    }

    public String normalizeStringValue(String value) {
        // Normalize a `value' of type VALUE_TYPE_STRING.
        return value;
    }

    public String normalizeTextValue(String value) {
        // Normalize a `value' of type VALUE_TYPE_TEXT.
        return normalizeNewlines(NTriplesUtil.unescapeString(value));
    }

    public String normalizeValue(String value) {
        // Normalize a `value' depending on its type.
        String type = getValueType(value);
        if (type == VALUE_TYPE_URI)
            return normalizeUri(value);
        else if (type == VALUE_TYPE_STRING)
            return normalizeStringValue(value);
        else if (type == VALUE_TYPE_TEXT)
            return normalizeTextValue(value);
        else
            return value;
    }


    public void indexTriples() throws IOException {
        // Index the triples in `triplesFile' to a Lucene index.
        // This assumes that `triplesFile' is sorted by subject.
        // For each subject we collect all its predicate/value pairs and index them
        // as a single Lucene document.  We create a subject field which will serve
        // as a primary key for direct lookup.  Each predicate becomes the name of a field,
        // if there are multiple values they become multiple fields with the same name.
        // All predicate/value pairs are also stored, so we can retrieve the info
        // directly from the index without having to go to a separate database.
        // Lucene stored field compression will provide good compression and access speed.

        BufferedReader in = openTextFile(TRIPLES_FILE);

        String line = null;
        String[] triple;
        String subject = "";
        long count = 0;
        long maxTriples = INDEX_FULL_DATA ? Long.MAX_VALUE : 20000000;

        // this map collects values for each of a subject's predicates -
        // we use a TreeMap for per-subject predicate sorting:
        Map<String,List<String>> predValues = new TreeMap<String,List<String>>();

        printlnProg("Indexing to directory '" + getIndexDirectoryName() + "'...");
        while (true) {
            line = in.readLine();
            ++count;
            if ((line == null) || (count > maxTriples)) {
                indexRecord(indexWriter, subject, predValues);
                break;
            }

            if (line.startsWith("#"))
                continue; // skip comments
            if (line.equals("")) {
                printlnLog("WARN: input line " + count + " was blank");
                continue;
            }

            // we expect TAB-separation which is more restrictive than N-Triples in general:
            triple = line.split("\t");
            if (triple.length != 4)
                throw new IOException("Unsupported N-triples format at line " + count + ": " + line);
            if (! triple[0].equals(subject)) {
                if (! "".equals(subject))
                    // new subject, index the current subject and start a new one:
                    indexRecord(indexWriter, subject, predValues);
                subject = triple[0];
                predValues.clear();
            }

            // record this predicate and value for the current subject:
            String predicate = triple[1];
            String value = triple[2];
            List<String> values = predValues.get(predicate);
            if (values == null) {
                values = new ArrayList<String>(5);
                predValues.put(predicate, values);
            }
            values.add(value);
            showProgress(count, 1000000);
        }
        printlnProg("");
    }

    public void indexRecord(IndexWriter writer, String subject, Map<String,List<String>> predValues) throws IOException {
        Document doc = new Document();
        Field subjField = new StringField(FIELD_NAME_SUBJECT, normalizeUri(subject), Field.Store.YES);
        //printlnDbg("DBG: indexRecord: " + subject);
        doc.add(subjField);

        FieldType IndexedField = new FieldType(TextField.TYPE_NOT_STORED);
        IndexedField.setStoreTermVectors(INDEX_TERMVECTORS);

        FieldType IndexedStoredField = new FieldType(TextField.TYPE_STORED);
        IndexedStoredField.setStoreTermVectors(INDEX_TERMVECTORS);

        long pagerank = 0;

        for (Map.Entry<String, List<String>> entry : predValues.entrySet()) {
            String predicate = normalizeUri(entry.getKey());
            List<String> values = entry.getValue();

            //printlnDbg("DBG:  " + predicate + ": " + values);
            for (String value : values) {
                String valueType = getValueType(value);
                value = normalizeValue(value);
                if (isIndexedPredicate(predicate)) {
                    if (valueType == VALUE_TYPE_URI)
                        // treat URI elements as atomic strings (e.g., types):
                        doc.add(new Field(predicate, value, StoredField.TYPE));
					else if (valueType == VALUE_TYPE_INT)
                        if (predicate.equals("pr_bin"))
                            pagerank = Long.parseLong(value);
                        else
    						doc.add(new SortedNumericDocValuesField(predicate, Long.parseLong(value)));
                    else {
                        // all others, run through the analyzer:
                        String lang = INDEX_LANGUAGE ? getValueLanguage(value) : null;
                        if (INDEX_LANGUAGE && lang != null && (isSupportedLanguage(lang) || isSupportedLanguage(getLanguageRoot(lang)))) {
                            // multi-lingual indexing: if we have a supported language, we store the field
                            // and then add an index entry with the language-qualified predicate:
                            doc.add(new Field(predicate, value, StoredField.TYPE));
                            if (INDEX_PREDICATES)
                                doc.add(new Field(languageQualifiedPredicate(predicate, lang), value, IndexedStoredField));
                            if (INDEX_TEXT)
                                doc.add(new Field(languageQualifiedPredicate(FIELD_NAME_TEXT, lang), value, IndexedField));
                        }
                        else {
                            // mono-lingual indexing or no language designation:
                            if (INDEX_PREDICATES)
                                doc.add(new Field(predicate, value, IndexedStoredField));
                            if (INDEX_TEXT) {
                                doc.add(new Field(FIELD_NAME_TEXT, value, IndexedField));
                                // make sure we store the triple if it wasn't already:
                                if (!INDEX_PREDICATES)
                                    doc.add(new Field(predicate, value, StoredField.TYPE));
                            }
                        }
                    }
                }
                else
                    doc.add(new Field(predicate, value, StoredField.TYPE));
            }
        }
        doc.add(new SortedNumericDocValuesField("pr_bin", pagerank));

        // we are creating the index from scratch, so we just add the document:
        writer.addDocument(doc);
    }


    public void doIndexTriples() {
        // Implements `index' command.
        try {
            if (! INDEX_FULL_DATA)
                printlnLog("INFO: Running in test mode with 20M triples, use -f to index all the data");
            initializeIndexBuilder();
            indexTriples();
            finalizeIndexBuilder();
        }
        catch (Exception e) {
            log.println("ERROR: " + e.getMessage());
            e.printStackTrace(log);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        FreebaseIndexer fbi = new FreebaseIndexer(args[0]);
        fbi.doIndexTriples();
    }
}
