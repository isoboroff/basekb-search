package gov.nist.basekb;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;

/*
 * Lucene's IndexWriter.addDocument() will pitch an exception if the document contains a token that is too long.
 * I would rather just drop long tokens.
 */

public class SafetyAnalyzer extends AnalyzerWrapper {

	private Analyzer baseAnalyzer;

	public SafetyAnalyzer(Analyzer baseAnalyzer) {
		super(Analyzer.PER_FIELD_REUSE_STRATEGY);
		this.baseAnalyzer = baseAnalyzer;
	}

	@Override
	public void close() {
		baseAnalyzer.close();
		super.close();
	}

	@Override
	protected Analyzer getWrappedAnalyzer(String fieldName) {
		return baseAnalyzer;
	}

	@Override
	protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
		TokenStream ts = components.getTokenStream();
		LengthFilter drop_long_tokens = new LengthFilter(ts, 0, 1024);
		return new TokenStreamComponents(components.getTokenizer(), drop_long_tokens);
	}
}
