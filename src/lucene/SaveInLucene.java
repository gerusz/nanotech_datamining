package lucene;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PatternAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import db.Database;

public class SaveInLucene {
	public static void saveInLucene() throws IOException, SQLException {
		FSDirectory fsDirectory = FSDirectory.open(new File("lucene"));
		Map<String, Analyzer> map = new HashMap<>();
		map.put("keyword_plus", new PatternAnalyzer(Version.LUCENE_41, Pattern.compile(";"), true, null));
		PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_41), map);
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_41, analyzer);
		iwc.setOpenMode(OpenMode.CREATE);
		IndexWriter writer = new IndexWriter(fsDirectory, iwc);
		
		Connection connection = Database.getConnection("localhost", "root", "");
		Statement statement = Database.createStreamingStatement(connection);
		ResultSet rs = statement.executeQuery("SELECT * FROM tbl_articles");
		int c = 0;
		while(rs.next()) {
			if(c%1000==0) System.out.println(c);
			++c;
			Document doc = new Document();
			FieldType idType = new FieldType();
			idType.setStored(true);
			idType.setIndexed(true);
			idType.setTokenized(false);

			FieldType textType = new FieldType();
			textType.setTokenized(true);
			textType.setStored(false);
			textType.setIndexed(true);
			
			doc.add(new Field("id", rs.getString("id"), idType));			
			doc.add(new Field("abstract", rs.getString("abstract"), textType ));
			doc.add(new Field("title", rs.getString("title"), textType ));
			doc.add(new Field("keywords", rs.getString("title_phrases"), textType));
			
			writer.addDocument(doc);
		}
		
		writer.close();
		
	}
	
	public static void main(String[] args) throws IOException, SQLException {
		saveInLucene();
	}
}
