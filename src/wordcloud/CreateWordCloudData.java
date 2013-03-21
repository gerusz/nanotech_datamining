package wordcloud;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import keywordCount.CountKeywords;


/**
 * 
 * This class generates data to generate a word cloud using
 * <a href=http://www.wordle.net/advanced>http://www.wordle.net/advanced</a>. The Data has to be in the format:<br>
 * word:weight<br>The weight is for this case, the frequency of a word. So for
 * example:<br>hello:1231<br>bye:2131<br>DataMiningIsMyMostLovedSubjectButThisIsNoWord:0
 * 
 * 
 * @author felix
 * 
 */
public class CreateWordCloudData {
	
	public static String createWordCloudData(int wordFrequencyTreshold) throws SQLException, IOException {
		// TODO change
		CountKeywords ck = new CountKeywords("localhost", "root", "");
		Map<String, Integer> keywordsForCountry = ck.getAllKeywords();
		StringBuilder wordCloudData = new StringBuilder();
		for(Map.Entry<String, Integer> entry : keywordsForCountry.entrySet()) {
			if(entry.getValue() > wordFrequencyTreshold) {
				wordCloudData.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
			}
		}
		return wordCloudData.toString();
	}
	
	public static void main(String[] args) throws IOException, SQLException {
		FileWriter writer = new FileWriter(new File("output\\wordCloudData.txt"));
		writer.write(createWordCloudData(3));
		writer.close();
	}
	
}
