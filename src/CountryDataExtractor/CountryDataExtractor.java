/**
 * 
 */
package cde;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * @author Gerusz
 *
 */
public class CountryDataExtractor {
	
	/**
	 * @param args
	 * 
	 * Usage: java CountryDataExtractor host hostname:port user username pass password database dbname
	 * 
	 * 
	 */
	public static void main(String[] args) {
		
		String host;
		String user;
		String pass;
		String dbName;
		
		host = "";
		user = "";
		pass = "";
		dbName = "";
		
		for(int i=0; i<args.length; i++) {
			if(i<args.length-1) {
				if(args[i].contains("host")) {
					i++;
					host = args[i];
				}
				else if(args[i].contains("user")) {
					i++;
					user = args[i];
				}
				else if(args[i].contains("pass")) {
					i++;
					pass = args[i];
				}
				else if(args[i].contains("database")) {
					i++;
					dbName = args[i];
				}
			}
		}
		
		if(host.isEmpty()) {
			System.out.println("Host:port?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				host = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(user.isEmpty()) {
			System.out.println("Username?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				user = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(pass.isEmpty()) {
			System.out.println("Password?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				pass = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(dbName.isEmpty()) {
			System.out.println("Database name?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				dbName = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			Connection connect = DriverManager.getConnection("jdbc:mysql://"+host+"/"+dbName+"?"
			              + "user="+user+"&password="+pass);
			Statement tableRecreateStatement = connect.createStatement();
			String tableRecreateQuery = "DROP TABLE IF EXISTS `"+dbName+"`.`article_countries`;\n"+
			"CREATE  TABLE `"+dbName+"`.`article_countries` (\n"+
			"`fk_article_id` INT(11) NULL ,\n"+
  			"`country` VARCHAR(64) NOT NULL ,\n"+
  			"INDEX `fk_article_id_idx` (`fk_article_id` ASC) ,\n"+
  			"INDEX `country_index` (`country` ASC) ,\n"+
  			"CONSTRAINT `fk_article_id`\n"+
    		"FOREIGN KEY (`fk_article_id` )\n" +
    		"REFERENCES `tbl_articles` (`id` )\n" +
    		"ON DELETE CASCADE\n" +
    		"ON UPDATE CASCADE);";
			//System.out.println(tableRecreateQuery);
			//tableRecreateStatement.executeUpdate(tableRecreateQuery);
			
			//At this point, the program has created the article_countries table
			System.out.println("Table created.");
			
			String selectDistinctCountriesString = "SELECT DISTINCT countries FROM tbl_articles";
			Statement countriesStatement = connect.createStatement();
			ResultSet countriesSet = countriesStatement.executeQuery(selectDistinctCountriesString);
			
			HashMap<String, Vector<Integer>> countriesMap = new HashMap<String, Vector<Integer>>();
			
			while(countriesSet.next()) {
				String countriesRow = countriesSet.getString(1);
				String[] countriesInRow = countriesRow.split(";");
				for(int i=0; i<countriesInRow.length; i++) {
					countriesInRow[i] = countriesInRow[i].trim();
					if(countriesInRow[i].length() > 0 && !countriesMap.containsKey(countriesInRow[i])) {
						Vector<Integer> dummyVector = new Vector<Integer>();
						countriesMap.put(countriesInRow[i], dummyVector);
					}
				}
			}
			
			countriesStatement.close();
			
			System.out.println("Countries selected, HashMap prepared.");
			
			//At this point, countriesMap contains every country with an empty vector.
			//That vector will be filled with the country IDs now.
			
			String getArticlesForCountryStatementString = "SELECT id FROM tbl_articles WHERE countries LIKE ?";
			PreparedStatement getArticlesForCountryStatement = connect.prepareStatement(getArticlesForCountryStatementString);
			
			int totalCountries = countriesMap.keySet().size();
			int done = 0;
			int articleCountriesRowCount = 0;
			for(String country : countriesMap.keySet()) {
				getArticlesForCountryStatement.setString(1, "%"+country+"%");
				System.out.printf("Doing %s...\n", country);
				ResultSet articlesForCountry = getArticlesForCountryStatement.executeQuery();
				int articleCount = 0;
				while(articlesForCountry.next()) {
					int id = articlesForCountry.getInt(1);
					countriesMap.get(country).add(id);
					articleCount++;
				}
				done++;
				System.out.printf("Done %s, %d / %d, %d articles.\n", country, done, totalCountries, articleCount);
				articleCountriesRowCount += articleCount;
			}
			
			getArticlesForCountryStatement.close();
			
			System.out.println("HashMap filled. Inserting rows into table...");
			System.out.println("Total: "+ Integer.toString(articleCountriesRowCount) + " rows.");
			
			int fivePercent = articleCountriesRowCount / 20;
			
			PreparedStatement insertStatement = connect.prepareStatement("INSERT INTO article_countries VALUES (?, ?)");
			int batchSize = 10000;
			int batchCount = 0;
			done = 0;
			for(String country: countriesMap.keySet()) {
				insertStatement.setString(2, country);
				for(Integer id : countriesMap.get(country)) {
					insertStatement.setInt(1, id);
					insertStatement.addBatch();
					batchCount++;
					done++;
					if(batchCount % batchSize == 0) {
						batchCount = 0;
						insertStatement.executeBatch();
						System.out.printf("Batch %d of %d done.\n", done/batchSize, articleCountriesRowCount/batchSize);
					}
				}
			}
			insertStatement.executeBatch();
			insertStatement.close();
			connect.close();
			System.out.println("Done.");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

}
