package co_occurrence;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

import util.LuceneUtils;
import util.MapUtil;
import util.Util;
import au.com.bytecode.opencsv.CSVWriter;
import db.Database;

public class CoOccurrenceMatrix {
	public static final class CoOccurrenceMatrixType {
		private final String[] keywordPositions;
		private DenseMatrix64F coOccurenceM;

		public CoOccurrenceMatrixType(DenseMatrix64F coOccurenceM,
				String[] keywordPosArr) {
			super();
			this.coOccurenceM = coOccurenceM;
			this.keywordPositions = keywordPosArr;
		}

		public DenseMatrix64F getCoOccurenceM() {
			return coOccurenceM;
		}

		public String[] getKeywords() {
			return keywordPositions;
		}
	}

	public static CoOccurrenceMatrixType getCoOccurenceMatrix()
			throws SQLException, IOException {
		Map<String, Integer> keywordFreq = new HashMap<String, Integer>();
		Map<String, Integer> keywordPos = new HashMap<String, Integer>();

		Connection connection = Database.getConnection("localhost", "root", "");

		// get size of data set
		Statement sizestatement = Database.createStatement(connection);
		ResultSet sizeResultSet = sizestatement
				.executeQuery("SELECT COUNT(*) FROM tbl_articles");
		sizeResultSet.next();
		int documentsize = sizeResultSet.getInt(1);

		// get actual data
		Statement statement = Database.createStatement(connection);
		statement
				.executeQuery("SELECT title_phrases FROM tbl_articles");
		// execute query and work with the data
		ResultSet resultSet = statement.getResultSet();
		int documentindex = 0;

		// count number of different keywords!
		while (resultSet.next()) {
			String[] keywords = resultSet.getString(1).split(";");
			for (String keyword : keywords) {
				keyword = LuceneUtils.change(keyword);
				if (keyword != null) {
					MapUtil.plusAtIndex(keywordFreq, keyword, 1);
				}
			}
		}
		Percentile percentile = new Percentile(0.9999);
		double[] values = Util.copyFromIntArray(keywordFreq.values().toArray(
				new Integer[keywordFreq.size()]));
		double topTenPercent = percentile.evaluate(values);
		Set<String> toRemove = MapUtil.getEntriesSmallerThan(keywordFreq, 300);
		MapUtil.removeEntries(keywordFreq, toRemove);
		for (String k : keywordFreq.keySet()) {
			keywordPos.put(k, keywordPos.size());
		}
		int keywordsize = keywordFreq.size();
		// move the cursor back
		resultSet.beforeFirst();

		DenseMatrix64F a = new DenseMatrix64F(keywordsize, documentsize);

		while (resultSet.next()) {
			if (documentindex % 1000 == 0) {
				System.out.println("Processed " + documentindex + " documents");
			}
			String[] keywords = resultSet.getString(1).split(";");
			for (String keyword : keywords) {
				keyword = LuceneUtils.change(keyword);
				if (keywordFreq.containsKey(keyword)) {
					int keywordindex = keywordPos.get(keyword);
					a.add(keywordindex, documentindex, keywordFreq.get(keyword));
				}
			}
			++documentindex;
		}
		DenseMatrix64F at = a.copy();
		DenseMatrix64F result = new DenseMatrix64F(a.numRows, a.numRows);
		CommonOps.multTransB(a, at, result);
		String[] keywordPosArr = new String[keywordPos.size()];
		for (Map.Entry<String, Integer> pos : keywordPos.entrySet()) {
			keywordPosArr[pos.getValue()] = pos.getKey();
		}
		return new CoOccurrenceMatrixType(result, keywordPosArr);
	}

	public static void main(String[] args) throws SQLException, IOException {
		CoOccurrenceMatrixType cm = getCoOccurenceMatrix();
		DenseMatrix64F cmm = cm.getCoOccurenceM();
		CSVWriter w = new CSVWriter(new FileWriter("output\\cmm.csv"));
		w.writeNext(new String[] { "Source", "Target", "Weight" });
		int id = 0;
		for (int r = 0; r < cmm.numRows; ++r) {
			for (int c = 0; c < cmm.numCols; ++c) {
				if (r != c && cmm.get(r, c) > 0) {
					w.writeNext(new String[] { cm.getKeywords()[r],
							cm.getKeywords()[c],
							Integer.toString((int) cmm.get(r, c)) });
				}
			}
		}
		w.close();
	}
}
