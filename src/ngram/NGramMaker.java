package ngram;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map.Entry;

import cue.lang.Counter;
import cue.lang.NGramIterator;
import cue.lang.stop.StopWords;
import db.Database;

public class NGramMaker {
	private final String BASE_WORD_COL = "word";
	private final String BASE_COL_TYPE = "nvarchar(255)";
	private Connection dbConnection, selectConnection;

	public NGramMaker(String dbHost, String dbUser, String dbPass) {
		dbConnection = Database.getConnection(dbHost, dbUser, dbPass);
		selectConnection = Database.getConnection(dbHost, dbUser, dbPass);
	}

	public void readAll(String table, String column, int n) {
		String tableName = prepareTable(column, n);
		String insertSql = prepareInsertSql(tableName, n);
		String selectSql = "SELECT " + column + " FROM " + table + "";
		Statement s = null;
		ResultSet rs = null;
		try {
			s = Database.createStreamingStatement(selectConnection);
			rs = s.executeQuery(selectSql);
			// Store the n grams in the counter object
			Counter<String> ngrams = new Counter<String>();
			String text = null;
			while (rs.next()) {
				text = rs.getString(1);
				// Go through the text, ignoring english stopwords.
				for (final String ngram : new NGramIterator(n, text, Locale.ENGLISH,
						StopWords.English)) {
					ngrams.note(ngram.toLowerCase(Locale.ENGLISH));
				}
			}
			System.out.println("generated all ngrams " + ngrams.entrySet().size());
			long insertstart = System.currentTimeMillis();
			insertNGrams(ngrams, n, insertSql);
			long inserttime = System.currentTimeMillis() - insertstart;
			System.out.println("inserting ngrams took: " + (inserttime));
		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException ex) {
			}
			try {
				if (s != null)
					s.close();
			} catch (SQLException ex) {
			}
		}
	}

	private String prepareTable(String tablePrefix, int n) {
		String table_name = tablePrefix + "_" + n + "gram";
		String table_sql = "CREATE TABLE IF NOT EXISTS " + table_name
				+ " (#&# freq int, primary key (#*#))";
		String drop_sql = "DROP TABLE IF EXISTS " + table_name; 
		StringBuilder columns = new StringBuilder();
		for (int i = 1; i <= n; i++) {
			columns.append(BASE_WORD_COL);
			columns.append(i);
			columns.append(" ");
			columns.append(BASE_COL_TYPE);
			columns.append(", \n");
		}
		table_sql = table_sql.replace("#&#", columns.toString());
		// Set the correct column names
		columns = new StringBuilder();
		StringBuilder parameters = new StringBuilder();
		for (int i = 1; i <= n; i++) {
			columns.append(BASE_WORD_COL);
			columns.append(i);
			if (i == n)
				table_sql = table_sql.replace("#*#", columns.toString());
			columns.append(", ");
			//
			parameters.append("?, ");
		}
		PreparedStatement tableStatement = null;
		try {
			tableStatement = dbConnection.prepareStatement(table_sql);
			tableStatement.execute();
		} catch (SQLException ex) {
			ex.printStackTrace();
			return "";
		} finally {
			try {
				if (tableStatement != null)
					tableStatement.close();
			} catch (SQLException ex) {
				// oh my..
			}
		}
		return table_name;
	}

	private String prepareInsertSql(String tableName, int n) {
		// This statement makes sure that values are only inserted once
		String sql = "INSERT INTO " + tableName + " (#&#freq) VALUES (#*#?)";
		// + "ON DUPLICATE KEY UPDATE freq = freq + ?";
		// Set the correct column names
		StringBuilder columns = new StringBuilder();
		StringBuilder parameters = new StringBuilder();
		for (int i = 1; i <= n; i++) {
			columns.append(BASE_WORD_COL);
			columns.append(i);
			columns.append(", ");
			//
			parameters.append("?, ");
		}
		sql = sql.replace("#&#", columns.toString());
		sql = sql.replace("#*#", parameters.toString());
		//
		return sql;
	}

	/**
	 * Inserts the given n-grams in a table in the database
	 * 
	 * @param ngrams The n-grams to insert
	 * @param n The n of the n-gram
	 * @param tablePrefix The prefix to give the table
	 */
	private void insertNGrams(final Counter<String> ngrams, int n, String sql) {
		PreparedStatement insertStatement;
		try {
			insertStatement = dbConnection.prepareStatement(sql);
			dbConnection.setAutoCommit(false);
		} catch (SQLException ex) {
			ex.printStackTrace();
			return;
		}
		int i = 0;
		String[] words;
		try {
			for (final Entry<String, Integer> e : ngrams.entrySet()) {
				words = e.getKey().split(" ");
				for (int j = 1; j <= n; j++)
					insertStatement.setString(j, words[j - 1]);
				// For the insert statement
				insertStatement.setInt(n + 1, e.getValue());
				// For the update statement
				// insertStatement.setInt(n + 2, e.getValue());
				// Execute the statements in small batches
				insertStatement.addBatch();
				if ((i + 1) % 10000 == 0) {
					long startTime = System.currentTimeMillis();
					insertStatement.executeBatch();
					dbConnection.commit();
					System.out.println("inserted " + i + " ngrams in "
							+ (System.currentTimeMillis() - startTime) + " ms");
				}
				i++;
			}
			insertStatement.executeBatch();
			dbConnection.commit();
		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (insertStatement != null)
					insertStatement.close();
			} catch (SQLException e) {
				// nope!
			}
		}
	}

	public static void main(String[] args) {
		NGramMaker ngrammer = new NGramMaker("localhost", "root", "");
		ngrammer.readAll("tbl_articles", "abstract", 3);
	}
}
