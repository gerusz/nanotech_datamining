package keywordchemicaltagger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.hp.hpl.jena.tdb.store.Hash;
import com.hp.hpl.jena.util.Tokenizer;

import uk.ac.cam.ch.wwmm.chemicaltagger.ChemicalTaggerTokeniser;
import uk.ac.cam.ch.wwmm.chemicaltagger.ChemistryPOSTagger;
import uk.ac.cam.ch.wwmm.chemicaltagger.OscarTagger;
import uk.ac.cam.ch.wwmm.chemicaltagger.WhiteSpaceTokeniser;
import uk.ac.cam.ch.wwmm.oscar.Oscar;
import uk.ac.cam.ch.wwmm.oscar.document.Token;
import uk.ac.cam.ch.wwmm.oscardata.OscarData;
import uk.ac.cam.ch.wwmm.oscartokeniser.Tokeniser;
import db.Database;

public class KeywordChemicalTagger {
	public static Set<String> getChemTaggedKeywords(String abstractText,
			String keywords) {
		// List<Token> tokens = ChemistryPOSTagger.getDefaultInstance()
		// .getCTTokeniser().tokenise(abstractText);
		// List<String> tagged = ChemistryPOSTagger.getDefaultInstance()
		// .getOscarTagger().runTagger(tokens, abstractText);
		// Set<String> ret = new HashSet<>();
		// for (int i = 0; i < tokens.size(); ++i) {
		// if (!tagged.get(i).equals("nil")
		// && keywords.contains(tokens.get(i).getSurface())) {
		// ret.add(tokens.get(i).getSurface());
		// }
		// }
		// return ret;
		List<Token> tokens = new SemicolonTokeniser().tokenise(keywords);
		List<String> tagged = ChemistryPOSTagger.getDefaultInstance()
				.getOscarTagger().runTagger(tokens, keywords);
		Set<String> ret = new HashSet<>();
		for (int i = 0; i < tokens.size(); ++i) {
			if (!tagged.get(i).equals("nil")) {
				System.out.println(tokens.get(i).getSurface());
				ret.add(tokens.get(i).getSurface());
			}
		}
		return ret;
	}

	public static void main(String[] args) throws SQLException, IOException,
			InterruptedException {
		Connection con = Database.getConnection("localhost", "root", "");
		Statement statement = Database.createStatement(con);
		statement.execute("SELECT abstract, keyword_plus FROM tbl_articles");
		ResultSet resultSet = statement.getResultSet();
		List<String> abstractsL = new ArrayList<>();
		List<String> keywordsL = new ArrayList<>();
		while (resultSet.next()) {
			abstractsL.add(resultSet.getString(1));
			keywordsL.add(resultSet.getString(2));
		}
		TagThread t1 = new TagThread(abstractsL.subList(0, 40000),
				keywordsL.subList(0, 40000));
		TagThread t2 = new TagThread(abstractsL.subList(40001, 80000),
				keywordsL.subList(40000, 80000));
		TagThread t3 = new TagThread(abstractsL.subList(80001, 120000),
				keywordsL.subList(80000, 120000));
		TagThread t4 = new TagThread(abstractsL.subList(120001,
				abstractsL.size()),
				keywordsL.subList(120000, abstractsL.size()));
		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t1.join();
		t2.join();
		t3.join();
		t4.join();
		Set<String> chemTaggedKeywords = t1.getChemTaggedKeywords();
		chemTaggedKeywords.addAll(t2.getChemTaggedKeywords());
		chemTaggedKeywords.addAll(t3.getChemTaggedKeywords());
		chemTaggedKeywords.addAll(t4.getChemTaggedKeywords());
		FileWriter w = new FileWriter(new File("output\\chemkeywords.txt"));
		for (String string : chemTaggedKeywords) {
			w.write(string);
			w.write("\n");
		}
		w.close();
	}

	private static class TagThread extends Thread {
		private List<String> keywords;
		private List<String> abstracts;
		private Set<String> chemTaggedKeywords = new HashSet<>();

		public Set<String> getChemTaggedKeywords() {
			return chemTaggedKeywords;
		}

		public TagThread(List<String> abstracts, List<String> keywords) {
			this.abstracts = abstracts;
			this.keywords = keywords;
		}

		@Override
		public void run() {
			for (int i = 0; i < abstracts.size(); ++i) {
				chemTaggedKeywords.addAll(KeywordChemicalTagger
						.getChemTaggedKeywords(abstracts.get(i),
								keywords.get(i)));
			}
		}
	}
}
