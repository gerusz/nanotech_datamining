package tfidf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import au.com.bytecode.opencsv.CSVWriter;
import db.Database;

public class TFIDFForDatabase {

	private Connection selectConnection;

	private static final String ARTICLE_TABLE = "tbl_articles";
	private static final String ID_COLUMN = "id";
	public static final String ABSTRACT_COLUMN = "abstract";
	public static final String TITLE_COLUMN = "title";

	public TFIDFForDatabase(String dbHost, String dbUser, String dbPass) {
		selectConnection = Database.getConnection(dbHost, dbUser, dbPass);
	}

	public Map<Integer, SortedMap<String, Double>> getTFIDFFromX(String column)
			throws SQLException, IOException {
		Statement statement = Database
				.createStatement(selectConnection);
		statement.execute("SELECT " + ID_COLUMN + ", " + column
				+ " FROM " + ARTICLE_TABLE + " WHERE " + column + " != ''");
		ResultSet resultSet = statement.getResultSet();
		List<String> texts = new ArrayList<String>();
		List<Integer> ids = new ArrayList<Integer>();
		while(resultSet.next()) {
			int id = resultSet.getInt(1);
			String columnText = resultSet.getString(2);
			texts.add(columnText);
			ids.add(id);
		}
		resultSet.close();
		List<SortedMap<String, Double>> tfidfvalues = TFIDFAlgorithm.tfidf(texts);
		Map<Integer, SortedMap<String, Double>> tfidfvaluesForDocs = new HashMap<Integer, SortedMap<String,Double>>();
		int idsIndex = 0;
		for(SortedMap<String, Double> vals : tfidfvalues) {
			tfidfvaluesForDocs.put(ids.get(idsIndex++), vals);
		}
		return tfidfvaluesForDocs;
	}
	
	public static void main(String[] args) throws SQLException, IOException {
		TFIDFForDatabase db = new TFIDFForDatabase("localhost", "root", "");
		Map<Integer, SortedMap<String, Double>> tfidfFromAbstract = db.getTFIDFFromX(ABSTRACT_COLUMN);
		// analyze data
		CSVWriter writer = new CSVWriter(new FileWriter(new File("output\\tfidf.csv")));
		int proc = 0;
		System.out.println(tfidfFromAbstract.size());
		for(Map.Entry<Integer, SortedMap<String, Double>> entry : tfidfFromAbstract.entrySet()) {
			String[] line = new String[1+entry.getValue().size()];
			line[0] = entry.getKey().toString();
			int i = 1;
			for(Map.Entry<String, Double> tfidf : entry.getValue().entrySet()) {
				line[i++] = tfidf.getKey() + "=" + tfidf.getValue();
			}
			writer.writeNext(line);
			if(++proc % 100 == 0) {
				System.out.println("Written " + proc + " entries");
			}
		}
		writer.close();
	}
}
