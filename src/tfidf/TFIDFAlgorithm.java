package tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.util.Version;

import util.LuceneUtils;
import util.MapUtil;

public class TFIDFAlgorithm {
	public static List<SortedMap<String, Double>> tfidf(List<String> docs)
			throws IOException {
		System.out.println("Processing " + docs.size() + " docs");
		Analyzer analyzer = new EnglishAnalyzer(Version.LUCENE_41);
		Map<String, AtomicInteger> globalFrequency = new HashMap<String, AtomicInteger>();
		List<Map<String, Integer>> docKeywordsFrequencies = new ArrayList<Map<String, Integer>>(
				docs.size());
		Map<String, Integer> maxFrequency = new HashMap<String, Integer>();
		// gather information
		Map<String, Integer> keywords = new HashMap<String, Integer>();
		int procCount = 0;
		for (String document : docs) {
			++procCount;
			if(procCount % 100 == 0) {
				System.out.println("Processed " + procCount);
			}
			keywords.clear();
			LuceneUtils.parseKeywordsAndFrequency(analyzer, document, keywords);
			docKeywordsFrequencies.add(keywords);
			MapUtil.addToMapFromMapWithDefaultVal(globalFrequency, keywords, new AtomicInteger(0));
			MapUtil.mapIncrement(globalFrequency, keywords.keySet());
			MapUtil.addHighestFromMapToNewMap(keywords, maxFrequency);
		}
		System.out.println("Saved " + docKeywordsFrequencies.size() + " results");
		// calculate tf-idf
		List<SortedMap<String, Double>> tfidfScores = new ArrayList<SortedMap<String, Double>>(
				docs.size());
		procCount = 0;
		for (Map<String, Integer> localKeywordFrequencies : docKeywordsFrequencies) {
			++procCount;
			if(procCount % 10 == 0) {
				System.out.println("Calculated " + procCount);
			}
			SortedMap<String, Double> tfidfScoresLocal = new TreeMap<String, Double>();
			for (Map.Entry<String, Integer> keywordFrequencies : localKeywordFrequencies
					.entrySet()) {
				// tf(t, d) = f(t,d) / max(f(w,d) : w Element of d)
				double tf = keywordFrequencies.getValue()
						/ maxFrequency.get(keywordFrequencies.getKey());
				// idf = log(N/ni) where N = Number of documents and ni = number
				// of documents that contains the term
				// keywordFrequencies.getKey()
				double idf = Math.log(docs.size()
						/ globalFrequency.get(keywordFrequencies.getKey()).intValue());
				// calculate tf-idf weight
				double tfidf = tf * idf;
				// store the weight
				tfidfScoresLocal.put(keywordFrequencies.getKey(), tfidf);
			}
			System.out.println(tfidfScoresLocal);
			tfidfScores.add(tfidfScoresLocal);
		}
		
		return tfidfScores;

	}
}
