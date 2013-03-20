package util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public final class LuceneUtils {
	protected static abstract class SaveMethod<T> {
		public T t;
		public SaveMethod(T t) {
			this.t = t;
		}
		public T getT() {
			return t;
		}
		public abstract void save(String key);
	}

	/**
	 * Extract tokens from a given text using the given analyzer.
	 * @param <T>
	 * 
	 * @param analyzer
	 *            The analyzer to be used to extract terms/tokens.
	 * @param text
	 *            The text to extract the tokens from.
	 * @return The extracted tokens.
	 * @throws IOException
	 *             If something fails while handling the StringReader stream
	 *             (shouldn't happen normally).
	 */
	public static List<String> parseKeywords(Analyzer analyzer, String text) throws IOException {
		List<String> keywords = new ArrayList<String>();
		SaveMethod<List<String>> saveMethod = new SaveMethod<List<String>>(keywords) {

			@Override
			public void save(String key) {
				this.t.add(key);
			}
		};
		return parseKeywordsT(analyzer, text, saveMethod);
	}
	
	public static void parseKeywordsAndFrequency(Analyzer analyzer, String text, Map<String, Integer> map) throws IOException {
		SaveMethod<Map<String, Integer>> saveMethod = new SaveMethod<Map<String,Integer>>(map) {

			@Override
			public void save(String key) {
				Integer count = 1;
				if(t.containsKey(t)) {
					count = t.get(key) + 1;
				}
				t.put(key, count);
			}
		};
		parseKeywordsT(analyzer, text, saveMethod);
	}
	
	
	protected static <T> T parseKeywordsT(Analyzer analyzer, String text, SaveMethod<T> saveMethod)
			throws IOException {
		// field is null, because we don't use field names (only necessary if
		// you store data in lucene)
		TokenStream stream = analyzer.tokenStream(null, new StringReader(text));
		CharTermAttribute cattr = stream.addAttribute(CharTermAttribute.class);
		try {
			stream.reset();
			while (stream.incrementToken()) {
				saveMethod.save(cattr.toString());
			}
			stream.end();
		} finally {
			stream.close();
		}
		return saveMethod.getT();
	}
	
	
}