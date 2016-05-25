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
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.util.Map;

// Args4J:
// Lucene:
// Sesame:


public class FreebaseToolsRunner {

    //
    ////// Command line and configuration options
    //

    public static String versionString = "FreebaseTools 2.0";

    @Option(name = "-H", aliases = {"--home"}, required = false,
            usage = "Home directory where configuration files are found")
    public String homeDirectory = ".";

    @Option(name = "-C", aliases = {"--config-file"}, required = false,
            usage = "Configuration file to load, default is `config.dat'")
    public String configFile = "config.dat";

    @Option(name = "-c", aliases = {"--command"}, required = false,
            usage = "Command to execute, one of `lookup' (the default), `search' or `index'")
    public String command = "lookup";

    @Option(name = "-T", aliases = {"--triples"}, required = false,
            usage = "Compressed, abbreviated N-triples KB file")
    public String triplesFile = "";

    @Option(name = "-I", aliases = {"--index"}, required = false,
            usage = "Lucene index directory")
    public String indexDirectoryName = "";

    @Option(name = "-P", aliases = {"--indexed-preds"}, required = false,
            usage = "Predicates file listing predicates that should be indexed so they can be restricted in queries")
    public String indexedPredsFile = null;

    @Option(name = "-o", aliases = {"--optimize-index"}, required = false,
            usage = "Optimize index by merging it onto a single segment for faster startup and search (can take a while)")
    public boolean optimizeIndex = false;

    @Option(name = "-f", aliases = {"--full-data"}, required = false,
            usage = "Index the full data, otherwise only a small test portion will be indexed")
    public boolean indexFullData = false;

    // if none of the -ip, -it or -il options are given, default values of -ip, -it (and -il
    // if at least one language-specific analyzer is configured in config.dat) are used:
    @Option(name = "-ip", aliases = {"--index-predicates"}, required = false,
            usage = "Text-valued predicates will be indexed on a per-predicate basis")
    boolean indexPredicatesOption = false;
    public boolean indexPredicates = true;

    @Option(name = "-it", aliases = {"--index-text"}, required = false,
            usage = "Text values of different predicates will be combined into a single `text' field for indexing")
    boolean indexTextOption = false;
    public boolean indexText = true;

    @Option(name = "-il", aliases = {"--index-language"}, required = false,
            usage = "Language-identified text values will be indexed in language-specific fields")
    boolean indexLanguageOption = false;
    public boolean indexLanguage = false;

    @Option(name = "-nn", aliases = {"--normalize-newlines"}, required = false,
            usage = "If true newlines will be mapped onto spaces during indexing and/or printing")
    public boolean normalizeNewlines = false;

    @Option(name = "-q", aliases = {"--query"}, required = false,
            usage = "Query string for lookup and search queries.  Accepts full Lucene query syntax for `search' queries.  `lookup' queries take a comma/white-space separated list of subject keys, if `-' is given, subjects will be read from stdin.")
    public String queryString = "supercalifragilisticexpialidocious"; // 161 matching subjects

    @Option(name = "-s", aliases = {"--search-field"}, required = false,
            usage = "Default search field used by queries if a field is not explicitly specified, defaults to `rs_label'")
    public String defaultSearchField = "rs_label";

    @Option(name = "-m", aliases = {"--max-hits"}, required = false,
            usage = "Maximum number of `search' query results to generate, default is 10, use -1 to get all results")
    public int maxHits = 10;

    @Option(name = "-p", aliases = {"--print-mode"}, required = false,
            usage = "Result print mode, one of `all' (default), `subject', or a comma/white-space separated list of predicates/chains (e.g., 'p1, p2>p3, p4')")
    public String printMode = "all";

    @Option(name = "-v", aliases = {"--show-progress"}, required = false,
            usage = "Show progress and timing information")
    public boolean showProgress = false;

    @Option(name = "-d", aliases = {"--show-debug"}, required = false,
            usage = "Show debugging information")
    public boolean showDebug = false;

    @Option(name = "--version", required = false,
            usage = "Show version information")
    public boolean showVersion = false;

    @Option(name = "-h", aliases = {"--help"}, required = false,
            usage = "Show usage information")
    public boolean showUsage = false;

    // If true, predicate chains will not loop back to their seeds:
    public boolean suppressPredicateLoops = true;

    public static final String COMMAND_INDEX = "index";
    public static final String COMMAND_LOOKUP = "lookup";
    public static final String COMMAND_SEARCH = "search";


    //
    ////// Top-level Constructors and Command Dispatch
    //

    public FreebaseToolsRunner() {
        // Use this to construct a tools instance and explicitly set configuration variables.
        super();
    }

    public FreebaseToolsRunner(String[] initArgs) {
        // Use this to construct a tools instance with a set of command-line `initArgs',
        // for example, `new FreebaseTools(new String[]{"--version"})'.
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(initArgs);
            if (showUsage) {
                System.err.println(versionString);
                System.err.println("Usage:");
                parser.printUsage(System.err);
                System.exit(0);
            }
        } catch (CmdLineException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println("Usage:");
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

    public void configureTools(FreebaseTools tools) {
        tools.FREEBASE_TOOLS_HOME = homeDirectory;
        tools.TRIPLES_FILE = triplesFile;
        tools.INDEX_DIRECTORY_NAME = indexDirectoryName;
        tools.INDEXED_PREDS_FILE = indexedPredsFile;
        tools.OPTIMIZE_INDEX = optimizeIndex;
        tools.INDEX_FULL_DATA = indexFullData;
        tools.INDEX_PREDICATES = indexPredicates;
        tools.INDEX_TEXT = indexText;
        tools.INDEX_LANGUAGE = indexLanguage;
        tools.NORMALIZE_NEWLINES = normalizeNewlines;
        tools.DEFAULT_SEARCH_FIELD = defaultSearchField;
        tools.MAX_HITS = maxHits;
        tools.PRINT_MODE = printMode;
        tools.SHOW_PROGRESS = showProgress;
        tools.SHOW_DEBUG = showDebug;
    }

    public static void main(String[] args) {
        // FreebaseTools main shell command dispatch.
        FreebaseToolsRunner runner = new FreebaseToolsRunner(args);
        FreebaseTools tools = new FreebaseTools(runner.configFile);

        runner.configureTools(tools);

        try {
            if (runner.showVersion) {
                System.err.println(versionString);
                System.exit(0);
            }
            if (tools.SHOW_DEBUG) {
                tools.printLog("DBG: cmdline args:");
                for (String arg : args)
                    tools.printLog(" " + arg);
                tools.printlnLog();
                tools.printlnLog("DBG: configuration:");
                for (Map.Entry<Object, Object> entry : tools.CONFIG.entrySet())
                    tools.printlnLog("DBG:    " + entry.getKey() + "=" + entry.getValue());
            }
            if (runner.command.equals(COMMAND_INDEX)) {
                tools.doIndexTriples();
            } else if (runner.command.equals(COMMAND_LOOKUP)) {
                tools.doLookup(runner.queryString);
            } else if (runner.command.equals(COMMAND_SEARCH)) {
                tools.doSearch(runner.queryString);
            } else
                throw new IOException("Illegal command: " + runner.command);
            System.exit(0);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }
}