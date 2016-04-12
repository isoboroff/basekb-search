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
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.openrdf.rio.ntriples.NTriplesUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

// Args4J:
// Lucene:
// Sesame:


public class FreebaseTools {

       //
    ////// Configuration options
     //

    public static String versionString = "FreebaseTools 1.1.0";

    public String configFile = "config.dat";

    public String command = "lookup";

    // Config variables that are cached from the Properties file at instantiation time
    public boolean showDebug = false;
    public boolean showProgress = false;
    public boolean indexPredicates = true;
    public boolean indexText = true;
    public boolean indexLanguage = false;


    public Properties config = null;

    void setDefaultConfig() {
        config = new Properties();
        config.put("triplesFile", "");
        config.put("indexDirectoryName", "");

        config.put("indexedPredsFile", "");
        config.put("optimizeIndex", false) ;
        config.put("indexFullData", false);

        config.put("indexPredicates", true);
        config.put("indexText", true);
        config.put("indexLanguage", false);

        config.put("normalizeNewlines", false);
        config.put("queryString", "supercalifragilisticexpialidocious"); // 161 matching subjects
        config.put("defaultSearchField", "");
        config.put("maxHits", 10);
        config.put("printMode", "all");
        config.put("showProgress", false);
        config.put("showDebug", false);
        config.put("showVersion", false);
        config.put("showUsage", false);

        // If true, predicate chains will not loop back to their seeds:
        config.put("suppressPredicateLoops", true);
    }

    void setCachedConfigVars() {
        showDebug = (Boolean)config.get("showDebug");
        showProgress = (Boolean)config.get("showProgress");
        indexPredicates = (Boolean)config.get("indexPredicates");
        indexText = (Boolean)config.get("indexText");
        indexLanguage = (Boolean)config.get("indexLanguage");
    }

    String getIndexedPredsFile() {
        String indexedPredsFile = getConfig("LUCENE_INDEXED_PREDS");
        if (indexedPredsFile == null)
            indexedPredsFile = getFreebaseToolsHome() + "indexed-preds.lst";
        return indexedPredsFile;
    }

    String getDefaultSearchField() {
        // If no field is given, get it from the configuration or use a default value.
        String defaultSearchField = getConfig("LUCENE_DEFAULT_FIELD");
        if (defaultSearchField == null)
            defaultSearchField = "rs_label";
        return normalizeUri(defaultSearchField);
    }

    public static final String PRINT_MODE_ALL     = "all";
    public static final String PRINT_MODE_SUBJECT = "subject";

    public static final String FIELD_NAME_SUBJECT = "subject";
    public static final String FIELD_NAME_TEXT    = "text";

    public static final String VALUE_TYPE_URI    = "URI";
    public static final String VALUE_TYPE_STRING = "STRING";
    public static final String VALUE_TYPE_TEXT   = "TEXT";
    public static final String VALUE_TYPE_INT    = "INT";
    public static final String VALUE_TYPE_OTHER  = "OTHER";


       //
    ////// Top-level Constructors and Command Dispatch
     //

    public FreebaseTools() {
        // Use this to construct a tools instance and explicitly set configuration variables.
        super();
        setDefaultConfig();
        setCachedConfigVars();
    }

    public FreebaseTools(Properties config) {
        this.config = config;
        setCachedConfigVars();
    }

    public FreebaseTools(String config_filename) {
        configFile = config_filename;
        readConfig();
        setCachedConfigVars();
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
        if (showDebug)
            log.println(x);
    }

    public void printlnProg(String x) {
        if (showProgress)
            log.println(x);
    }

    public void showProgress(long count, int defaultInterval) {
        if (showProgress && ((count % defaultInterval) == 0)) {
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

    public String getFreebaseToolsHome() {
        // Intuit the home directory of the toolkit based on the location of the class file.
        return new File(FreebaseTools.class.getResource("FreebaseTools.class").getPath()).getParentFile().getParentFile().getParentFile().getParentFile().getParent();
    }


    public void readConfig() {
        String fbtHome = getFreebaseToolsHome();
        readConfig(fbtHome);
    }

    public void readConfig(String fbtHome) {
        if (config == null)
            config = new Properties();

        // Read the FBT config.dat which is assumed to use shell variable syntax.
        try {
            List<String> configLines = readListFile(configFile);

            Pattern vardef = Pattern.compile("^setenv\\s+(\\S+)\\s+(.*)");
            Matcher matcher = vardef.matcher("");
            for (String line : configLines) {
                line = line.trim();
                matcher.reset(line);
                if (matcher.matches()) {
                    String property = matcher.group(1);
                    String value = matcher.group(2);
                    value = value.replace("$FBT_HOME", fbtHome);
                    config.setProperty(property, value);
                }
            }
        }
        catch (Exception e) {
            log.println("WARN: problem reading config file `" + configFile + "': " + e.getMessage());
            e.printStackTrace(log);
        }

    }

    public String getConfig(String property) {
        // Get the configuration value for `property', or null if none is defined.
        if (config == null) {
            config = new Properties();
            readConfig();
        }
        return config.getProperty(property);
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
        String indexDirectoryName = config.getProperty("indexDirectoryName");
        String triplesFile = config.getProperty("triplesFile");
        if (! indexDirectoryName.equals(""))
            return indexDirectoryName;
        else if (! triplesFile.equals(""))
            return triplesFile + ".lucene.index";
        else
            throw new IOException("Cannot determine Lucene index directory, supply via -i");
    }

    // We assume we either are building a new index or querying it, but never both.
    // These variables cache various index-related objects for either reading or writing:
    Directory indexDirectory = null;
    IndexWriter indexWriter = null;
    IndexReader indexReader = null;
    IndexSearcher indexSearcher = null;
    Analyzer indexAnalyzer = null;
    Analyzer defaultIndexAnalyzer = null;

    public void initializeIndexBuilder() throws Exception {
        // Create a new index directory and writer to index a triples file.
        // Raise an error if an index already exists, so we don't accidentally overwrite it.
        String indexDir = getIndexDirectoryName();
        if ((new java.io.File(indexDir)).isDirectory())
            throw new IOException("Index directory already exists, remove it before indexing");

        indexDirectory = FSDirectory.open(Paths.get(indexDir));
        IndexWriterConfig iwc = new IndexWriterConfig(getIndexAnalyzer());

        // we always create a new index from scratch:
        iwc.setOpenMode(OpenMode.CREATE);
        iwc.setCodec(new Lucene54Codec(Mode.BEST_SPEED));          // the default
        //iwc.setCodec(new Lucene54Codec(Mode.BEST_COMPRESSION));  // slower, but better compression

        indexWriter = new IndexWriter(indexDirectory, iwc);
        indexAnalyzer = getIndexAnalyzer();

        if (indexPredicates) printlnProg("Indexing individual predicates");
        if (indexText) printlnProg("Indexing combined predicate text values");
        if (indexLanguage) printlnProg("Indexing predicates for language(s): " + supportedLanguages);
    }

    public void finalizeIndexBuilder() throws IOException {
        if ((Boolean)config.get("optimizeIndex")) {
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

    public void initializeIndexSearch() throws IOException {
        // Initialize index structures for search.
        if (indexDirectory == null)
            indexDirectory = FSDirectory.open(Paths.get(getIndexDirectoryName()));
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
    
    public Analyzer getDefaultIndexAnalyzer() throws Exception {
        // Get the current default index analyzer or create it.
        // Use the type specified by LUCENE_INDEX_ANALYZER_DEFAULT
        // or use StandardAnalyzer as the default.
        if (defaultIndexAnalyzer == null) {
            String defaultAnalyzerName = getConfig("LUCENE_INDEX_ANALYZER_DEFAULT");
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
                config.put("indexLanguage", true);
            if ((Boolean)config.get("indexLanguage"))
                indexAnalyzer = new PerFieldAnalyzerWrapper(getDefaultIndexAnalyzer(), analyzersMap);
            else
                indexAnalyzer = getDefaultIndexAnalyzer();
        }
        return indexAnalyzer;
    }

    public long getIndexNumDocs() throws IOException {
        // Returns the number of documents/subjects indexed in the index.
        return getIndexReader().numDocs();
    }

    public long getIndexTermNumDocs(Term term) throws IOException {
        // Return the number of documents/subjects containing `term'.
        return getIndexReader().docFreq(term);
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
        String analyzerPrefix = "LUCENE_INDEX_ANALYZER_";
        if (indexedPreds == null)
            initializeIndexedPreds();
        printlnDbg("DBG: field analyzers:");
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith(analyzerPrefix)) {
                String value = (String) config.getProperty(key);
                String lang = key.substring(analyzerPrefix.length()).toLowerCase().replace('_', '-');
                if (! lang.equals("default")) {
                    // record language as a supported language as a side effect:
                    supportedLanguages.add(normalizeLanguage(lang));
                    Analyzer langAnalyzer = new SafetyAnalyzer((Analyzer)Class.forName(value).newInstance());
                    for (String pred : indexedPreds) {
                        String langPred = languageQualifiedPredicate(pred, lang);
                        fieldAnalyzers.put(langPred, langAnalyzer);
                        printlnDbg("DBG:    " + langPred + " -> " + langAnalyzer);
                    }
                }
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
        for (String pred : readListFile(getIndexedPredsFile())) {
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
        if ((Boolean)config.get("normalizeNewlines"))
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

        String triplesFile = config.getProperty("triplesFile");
        BufferedReader in = openTextFile(triplesFile);

        String line = null;
        String[] triple;
        String subject = "";
        long count = 0;
        long maxTriples = (Boolean)config.get("indexFullData") ? Long.MAX_VALUE : 20000000;

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

		FieldType VectorField = new FieldType(StringField.TYPE_STORED);
		VectorField.setStoreTermVectors(true);

		FieldType TokVectorField = new FieldType(TextField.TYPE_STORED);
		TokVectorField.setStoreTermVectors(true);
		
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
                        doc.add(new Field(predicate, value, VectorField));
					else if (valueType == VALUE_TYPE_INT)
						doc.add(new SortedNumericDocValuesField(predicate, Long.parseLong(value)));
                    else {
                        // all others, run through the analyzer:
                        String lang = indexLanguage ? getValueLanguage(value) : null;
                        if (indexLanguage && lang != null && (isSupportedLanguage(lang) || isSupportedLanguage(getLanguageRoot(lang)))) {
                            // multi-lingual indexing: if we have a supported language, we store the field
                            // and then add an index entry with the language-qualified predicate:
                            doc.add(new Field(predicate, value, VectorField));
                            if (indexPredicates)
                                doc.add(new Field(languageQualifiedPredicate(predicate, lang), value, TokVectorField));
                            if (indexText)
                                doc.add(new Field(languageQualifiedPredicate(FIELD_NAME_TEXT, lang), value, TokVectorField));
                        }
                        else {
                            // mono-lingual indexing or no language designation:
                            if (indexPredicates)
                                doc.add(new Field(predicate, value, TokVectorField));
                            if (indexText) {
                                doc.add(new Field(FIELD_NAME_TEXT, value, TokVectorField));
                                // make sure we store the triple if it wasn't already:
                                if (! indexPredicates)
                                    doc.add(new Field(predicate, value, VectorField));
                            }
                        }
                    }
                }
                else
                    doc.add(new Field(predicate, value, VectorField));
            }
        }

        // we are creating the index from scratch, so we just add the document:
        writer.addDocument(doc);
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
                out.println("    " + field.name() + ": " + normalizeNewlines(field.stringValue()));
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
            out.print(normalizeNewlines(row.get(i)));
        }
        out.println();
    }

    public void printPredicateChain(String subjectUri, float score, String[] predicateChain, int start, List<String> row, PrintWriter out) throws IOException {
        // Helper for `printSubjectPredicates' to print the tail of a predicate chain.
        if ((Boolean)config.get("suppressPredicateLoops") && row.contains(subjectUri))
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
            String[] preds = parsePredicateList(config.getProperty("printMode"));
            printModePredicates = new String[preds.length][];
            for (int i = 0; i < preds.length; i++)
                printModePredicates[i] = parsePredicateChain(preds[i]);
        }
        return printModePredicates;
    }

    HashSet<String> printModeFields = null;  // list of fields to restrict Document access to, only used for top-level queries

    public Set<String> getPrintModeFields() {
        if ((printModeFields == null) && PRINT_MODE_ALL.equals(config.getProperty("printMode")))
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

    public void doIndexTriples() {
        // Implements `index' command.
        try {
            if (! (Boolean)config.get("indexFullData"))
                printlnLog("INFO: Running in test mode with 20M triples, use -f to index all the data");
            initializeIndexBuilder();
            indexTriples();
            finalizeIndexBuilder();
        }
        catch (Exception e) {
            log.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }

    public void doLookup() {
        try {
            long timeSetup = 0;
            long timeQuery = 0;
            long timeDisplay = 0;

            // force index initialization, so we get a better sense of the actual query execution time:
            printlnProg("Loading index...");
            long timeStart = java.lang.System.currentTimeMillis();
            getIndexReader();
            timeSetup += (java.lang.System.currentTimeMillis() - timeStart);

            int blockSize = 1000;
            String queryString = config.getProperty("queryString");
            BufferedReader in = queryString.equals("-") ? new BufferedReader(new InputStreamReader(System.in)) : null;
            List<String> subjects = (in != null) ? readList(in, blockSize) : Arrays.asList(parsePredicateList(queryString));
            PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))); // Java I/O API is insane
            boolean fullDisplay = PRINT_MODE_ALL.equals(config.getProperty("printMode"));

            while (true) {
                for (String subject : subjects) {
                    // now run the query:
                    timeStart = java.lang.System.currentTimeMillis();
                    int docid = getSubjectDocID(subject);
                    if (docid < 0) {
                        timeQuery += (java.lang.System.currentTimeMillis() - timeStart);
                        printlnLog("Not found: " + subject);
                    }
                    else {
                        Document doc = getDocumentInMode(docid);
                        timeQuery += (java.lang.System.currentTimeMillis() - timeStart);
                        timeStart = java.lang.System.currentTimeMillis();
                        printSubjectInMode(doc, -1, config.getProperty("printMode"), out);
                        if (fullDisplay)
                            out.println();
                        timeDisplay += (java.lang.System.currentTimeMillis() - timeStart);
                    }
                }
                if (in == null)
                    break;
                subjects = readList(in, blockSize);
                if (subjects.isEmpty())
                    break;
            }
            out.flush();
            printlnProg("Run time: setup=" + timeSetup + "ms, query=" + timeQuery + "ms, display=" + timeDisplay + "ms");
            getIndexReader().close();
        }
        catch (Exception e) {
            log.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }

    public void doSearch() {
        try {
            long timeSetup = 0;
            long timeQuery = 0;
            long timeDisplay = 0;
            String queryString = config.getProperty("queryString");
            int maxHits = Integer.parseInt(config.getProperty("maxHits"));

            // force index initialization, so we get a better sense of the actual query execution time:
            printlnProg("Loading index...");
            long timeStart = java.lang.System.currentTimeMillis();
            getIndexSearcher();
            getIndexAnalyzer();
            timeSetup += (java.lang.System.currentTimeMillis() - timeStart);
            printlnProg("Index contains " + getIndexNumDocs() + " documents");

            timeStart = java.lang.System.currentTimeMillis();
            printlnDbg("DBG: query string: " + queryString);
            Query query = new QueryParser(getDefaultSearchField(), getIndexAnalyzer()).parse(queryString);
            printlnProg("Searching for: " + query.toString(getDefaultSearchField()));
            boolean allResults = (maxHits == -1);
            int nDocs = allResults ? 1 : maxHits;
            TopDocs results = getIndexSearcher().search(query, nDocs);
            ScoreDoc[] hits = results.scoreDocs;
    
            int numTotalHits = results.totalHits;
            printlnProg("Found " + numTotalHits + " matching subject(s)");
            if (allResults) {
                nDocs = numTotalHits;
                printlnProg("Collecting results...");
                results = getIndexSearcher().search(query, numTotalHits);
                hits = results.scoreDocs;
            }
            timeQuery = java.lang.System.currentTimeMillis() - timeStart;
      
            timeStart = java.lang.System.currentTimeMillis();
            printlnProg("Printing results...");
            PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))); // Java I/O API is insane
            int end = Math.min(hits.length, nDocs);
            boolean fullDisplay = PRINT_MODE_ALL.equals(config.getProperty("printMode"));

            for (int i = 0; i < end; i++) {
                // this takes about 90s for 3M subjects names, so not super fast but ok (0.03ms per subject)
                int docid = hits[i].doc;
                float score = allResults ? -1.0f : hits[i].score;
                Document doc = getDocumentInMode(docid);
                printSubjectInMode(doc, score, config.getProperty("printMode"), out);
                if (showDebug) out.flush();
                printlnDbg("DBG: score explanation: " + getIndexSearcher().explain(query, docid).toString());
                if (fullDisplay)
                    out.println();
            }
            out.flush();
            timeDisplay = java.lang.System.currentTimeMillis() - timeStart;
            printlnProg("Run time: setup=" + timeSetup + "ms, query=" + timeQuery + "ms, display=" + timeDisplay + "ms");
            getIndexReader().close();
        }
        catch (Exception e) {
            log.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }


    public void readCompressedTriples () throws IOException {
        // Test driver to check reading performance: this reads and counts 878M lines in 7m:40s which is
        // fairly comparable to the 5m:40s it takes with `zcat <file> | wc'.
        String triplesFile = config.getProperty("triplesFile");
        BufferedReader in = openTextFile(triplesFile);
        String line = null;
        long count = 0;
        
        while (true) {
            line = in.readLine();
            if (line == null)
                break;
            ++count;
            showProgress(count, 1000000);
        }
        printlnLog();
        printlnLog("total count=" + count);
        in.close();
    }

    public void doReadCompressedTriples() {
        try {
            readCompressedTriples();
        }
        catch (Exception e) {
            log.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }
}
