package keywordCount;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;
import org.tartarus.snowball.ext.germanStemmer;

import util.MapUtil;

import db.Database;

public class CountKeywords {

	class ValueComparator<T> implements Comparator<T> {

		Map<T, Integer> base;

		public ValueComparator(Map<T, Integer> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with
		// equals.
		public int compare(T a, T b) {
			if (base.get(a) >= base.get(b)) {
				return -1;
			} else {
				return 1;
			} // returning 0 would merge keys
		}
	}

	private Connection selectConnection;

	private SnowballStemmer stemmer;

	private static final String PUBLICATION_YEAR_COLUMN = "publication_year";
	private static final String COUNTRY_COLUMN = "countries";
	private static final String KEYWORD_PLUS_COLUMN = "keyword_plus";

	private static final String EQUALS = "=";
	private static final String LIKE = " LIKE ";


	public CountKeywords(String dbHost, String dbUser, String dbPass) {
		selectConnection = Database.getConnection(dbHost, dbUser, dbPass);
		stemmer = new englishStemmer();
	}

	public Map<String, Integer> getKeywordsForYear(int year)
			throws SQLException, IOException {
		return getKeywordsForT(year, PUBLICATION_YEAR_COLUMN, ";", EQUALS);
	}

	public Map<String, Integer> getKeywordsForCountry(String country)
			throws SQLException, IOException {
		return getKeywordsForT("'%" + country + "%'", COUNTRY_COLUMN, ";", LIKE);
	}

	private <T> Map<String, Integer> getKeywordsForT(T type, String columnName,
			String splitRegex, String whereComparator) throws SQLException,
			IOException {
		if (type == null) {
			throw new IllegalArgumentException(
					"Argument 'type' must not be null");
		}
		if (columnName == null) {
			throw new IllegalArgumentException(
					"Argument 'columnName' must not be null");
		}
		Statement statement = Database.createStatement(selectConnection);
		String where = columnName + whereComparator + type;

		final int KEYWORD_PLUS = 1;
		// only select the count using group by for the rare case, that the same
		// keywords appears in the same order in #count different entries
		final int COUNT = 2;
		statement
				.execute("SELECT " + KEYWORD_PLUS_COLUMN + ", COUNT(*) FROM tbl_articles WHERE "
						+ where + " GROUP BY " + KEYWORD_PLUS_COLUMN);
		ResultSet resultSet = statement.getResultSet();
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		int processed = 0;
		while (resultSet.next()) {
			if (processed % 1000 == 0) {
				System.out.println("Processed " + processed + " entries");
			}
			// get the keyword list
			String keywordList = resultSet.getString(KEYWORD_PLUS);
			// get the count
			int count = resultSet.getInt(COUNT);
			// get the keywords
			String[] keywords = keywordList.split(splitRegex);
			for (String keyword : keywords) {
				keyword = keyword.toLowerCase();
				// stem the keyword
				keyword = stem(keyword);
				// update/add the count to the map
				Integer oldCount = countMap.get(keyword);
				if (oldCount == null) {
					oldCount = 0;
				}
				oldCount += count;
				countMap.put(keyword, count);
			}
			++processed;
		}
		// Sort the map in descending order and return sorted map
		ValueComparator<String> valueComp = new ValueComparator<String>(
				countMap);
		Map<String, Integer> sortedMap = new TreeMap<String, Integer>(valueComp);
		sortedMap.putAll(countMap);
		return sortedMap;
	}

	private String stem(String keyword) {
		// TODO keyword can be a set of words. Stemming will probably not work
		// then, because it expects only one word as an argument.
		stemmer.setCurrent(keyword);
		stemmer.stem();
		return stemmer.getCurrent();
	}

	public static void main(String[] args) throws SQLException, IOException {
		CountKeywords countKeywords = new CountKeywords("localhost", "root", "");
		Map<String, Integer> keywordsForYear = countKeywords
				.getKeywordsForYear(1999);
		Map<String, Integer> keywordsForCountry = countKeywords
				.getKeywordsForCountry("Vatican");
		MapUtil.printFirstXEntriesFromMap(keywordsForYear, 10);
	}

}
