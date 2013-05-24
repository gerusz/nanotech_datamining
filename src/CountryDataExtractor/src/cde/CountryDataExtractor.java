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
import java.util.HashSet;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
		String sourceTable;
		String columnToSeparate;
		String uniqueIdName;
		String destinationTable;
		String separatorString;
		String outputUniqueIdName;
		String outputSeparatedColumnName;
		String filter;
		HashSet<String> filterWords;
		
		host = "";
		user = "";
		pass = "";
		dbName = "";
		sourceTable = "";
		columnToSeparate = "";
		uniqueIdName = "";
		destinationTable = "";
		separatorString = "";
		outputUniqueIdName = "";
		outputSeparatedColumnName = "";
		filter = "";
		filterWords = null;
		
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
		
		if(sourceTable.isEmpty()) {
			System.out.println("Source table name?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				sourceTable = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(destinationTable.isEmpty()) {
			System.out.println("Destination table name?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				destinationTable = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(columnToSeparate.isEmpty()) {
			System.out.println("Column to separate?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				columnToSeparate = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(separatorString.isEmpty()) {
			System.out.println("Separator string?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				separatorString = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(uniqueIdName.isEmpty()) {
			System.out.println("Unique ID column name in the source table?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				uniqueIdName = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(outputUniqueIdName.isEmpty()) {
			System.out.println("Unique ID column name in the destination table?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				outputUniqueIdName = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(outputSeparatedColumnName.isEmpty()) {
			System.out.println("Separated column name in the destination table?");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				outputSeparatedColumnName = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(filter.isEmpty()) {
			System.out.println("Filter? (separate with comma, type no for empty)");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				filter = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
			if(!filter.equalsIgnoreCase("no")) {
				filterWords = new HashSet<String>();
				String[] fwArray = filter.split(",");
				for(int i=0; i<fwArray.length; i++) {
					filterWords.add(fwArray[i].trim());
				}
				System.out.printf("%d filter words\n", filterWords.size());
			}
		}
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			Connection connect = DriverManager.getConnection("jdbc:mysql://"+host+"/"+dbName+"?"
			              + "user="+user+"&password="+pass);
			Statement tableRecreateStatement = connect.createStatement();
			String tableDropQuery = "DROP TABLE IF EXISTS `"+dbName+"`.`"+destinationTable+"`;";
			String tableRecreateQuery = "CREATE  TABLE `"+dbName+"`.`"+destinationTable+"` (\n"+
			"`"+outputUniqueIdName+"` INT(11) NULL ,\n"+
  			"`"+outputSeparatedColumnName+"` VARCHAR(64) NOT NULL ,\n"+
  			"INDEX `"+outputUniqueIdName+"_idx` (`"+outputUniqueIdName+"` ASC) ,\n"+
  			"INDEX `"+outputSeparatedColumnName+"_index` (`"+outputSeparatedColumnName+"` ASC) ,\n"+
  			"CONSTRAINT `"+outputUniqueIdName+"_"+destinationTable+"_constraint`\n"+
    		"FOREIGN KEY (`"+outputUniqueIdName+"` )\n" +
    		"REFERENCES `"+sourceTable+"` (`"+uniqueIdName+"` )\n" +
    		"ON DELETE CASCADE\n" +
    		"ON UPDATE CASCADE);";
			
			try {
				tableRecreateStatement.executeUpdate(tableDropQuery);
				tableRecreateStatement.executeUpdate(tableRecreateQuery);
			}
			catch(SQLException ex) {
				System.out.println("Couldn't create destination table. Run this command in the workbench:");
				System.out.println(tableDropQuery);
				System.out.println(tableRecreateQuery);
				System.out.println("Type something and press enter to continue");
				try {
					BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
					bufferRead.readLine();
				}
				catch(IOException exc) {
					
				}
			}
			
			System.out.println("Selecting "+columnToSeparate+" values...");
			
			//At this point, the program has created the article_countries table
			//System.out.println("Table created.");
			
			String selectDistinctSeparateString = "SELECT DISTINCT "+columnToSeparate+" FROM "+sourceTable;
			PreparedStatement separateStatement = connect.prepareStatement(selectDistinctSeparateString);
			ResultSet separateSet = separateStatement.executeQuery();
			
			HashMap<String, Vector<Integer>> separateMap = new HashMap<String, Vector<Integer>>();
			
			while(separateSet.next()) {
				String countriesRow = separateSet.getString(1);
				String[] countriesInRow = countriesRow.split(separatorString);
				for(int i=0; i<countriesInRow.length; i++) {
					countriesInRow[i] = countriesInRow[i].trim();
					if(countriesInRow[i].length() > 0 && !separateMap.containsKey(countriesInRow[i]) && (filterWords == null || filterWords.contains(countriesInRow[i]))) {
						Vector<Integer> dummyVector = new Vector<Integer>();
						separateMap.put(countriesInRow[i], dummyVector);
					}
				}
			}
			
			separateStatement.close();
			
			System.out.println("Separate selected, HashMap prepared.");
			
			//At this point, countriesMap contains every country with an empty vector.
			//That vector will be filled with the country IDs now.
			/*
			String getArticlesForSeparatedStatementString = "SELECT "+uniqueIdName+" FROM "+sourceTable+" WHERE "+columnToSeparate+" LIKE ?";
			PreparedStatement getArticlesForSeparatedStatement = connect.prepareStatement(getArticlesForSeparatedStatementString);
			
			int totalSeparated = separateMap.keySet().size();
			int done = 0;
			int articleSeparatedRowCount = 0;
			for(String separated : separateMap.keySet()) {
				getArticlesForSeparatedStatement.setString(1, "%"+separated+"%");
				System.out.printf("Doing %s...\n", separated);
				ResultSet articlesForSeparated = getArticlesForSeparatedStatement.executeQuery();
				int articleCount = 0;
				while(articlesForSeparated.next()) {
					int id = articlesForSeparated.getInt(1);
					separateMap.get(separated).add(id);
					articleCount++;
				}
				done++;
				System.out.printf("Done %s, %d / %d, %d articles.\n", separated, done, totalSeparated, articleCount);
				articleSeparatedRowCount += articleCount;
			}
			
			getArticlesForSeparatedStatement.close();
			*/
			
			int sourceRowCount = 0;
			int batchSize = 10000;
			String sourceRowCountString = "SELECT COUNT(*) FROM "+sourceTable;
			Statement sourceRowCountStatement = connect.createStatement();
			ResultSet sourceRowCountRS = sourceRowCountStatement.executeQuery(sourceRowCountString);
			sourceRowCountRS.next();
			sourceRowCount = sourceRowCountRS.getInt(1);
			
			String selectString = "SELECT "+uniqueIdName+", "+columnToSeparate+" FROM "+sourceTable+" LIMIT ? OFFSET ?";
			PreparedStatement selectStatement = connect.prepareStatement(selectString);
			selectStatement.setInt(1, batchSize);
			
			int offset = 0;
			do {
				System.out.printf("Doing %d - %d of %d\n", offset, offset+batchSize, sourceRowCount);
				selectStatement.setInt(2, offset);
				
				ResultSet rows = selectStatement.executeQuery();
				while(rows.next()) {
					String cts = rows.getString(2); //Column To Separate
					String[] separated = cts.split(separatorString);
					int id = rows.getInt(1);
					for(int i=0; i<separated.length; i++) {
						String keyword = separated[i].trim();
						if(keyword != null && !keyword.isEmpty() && (filterWords == null || filterWords.contains(keyword))) {
							separateMap.get(keyword).add(id);
						}
					}
				}
				
				offset += batchSize;
			} while(offset < sourceRowCount);
			
			System.out.println("HashMap filled. Generating temp CSV file...");
			
			FileWriter fw = new FileWriter("C:\\tmp.csv");
			BufferedWriter writer = new BufferedWriter(fw);
			
			
			/*System.out.println("Total: "+ Integer.toString(articleSeparatedRowCount) + " rows.");
			
			PreparedStatement insertStatement = connect.prepareStatement("INSERT INTO article_countries VALUES (?, ?)");
			int batchSize = 10000;
			int batchCount = 0;
			done = 0;
			for(String country: separateMap.keySet()) {
				insertStatement.setString(2, country);
				for(Integer id : separateMap.get(country)) {
					insertStatement.setInt(1, id);
					insertStatement.addBatch();
					batchCount++;
					done++;
					if(batchCount % batchSize == 0) {
						batchCount = 0;
						insertStatement.executeBatch();
						System.out.printf("Batch %d of %d done.\n", done/batchSize, articleSeparatedRowCount/batchSize);
					}
				}
			}
			insertStatement.executeBatch();
			insertStatement.close();
			*/
			
			//writer.write("\""+outputUniqueIdName+"\",\""+outputSeparatedColumnName+"\"\n");
			
			int fileRows = 0;
			int fileBatchSize = 20000;
			int totalRows = 0;
			
			for(String separated: separateMap.keySet()) {
				totalRows += separateMap.get(separated).size();
			}
			
			for(String separated: separateMap.keySet()) {
				for(Integer id: separateMap.get(separated)) {
					writer.write(Integer.toString(id)+"\t");
					writer.write(separated);
					writer.write("\n");
					fileRows++;
					if(fileRows % fileBatchSize == 0) {
						writer.flush();
						writer.close();
						fw.close();
						
						System.out.printf("Temp file generated, importing rows %d - %d / %d into database...\n", fileRows-20000, fileRows, totalRows);
						
						String importStatementString = "LOAD DATA INFILE 'C:\\\\tmp.csv' INTO TABLE "+destinationTable+" FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\\n'";
						PreparedStatement importStatement = connect.prepareStatement(importStatementString);
						try {
						importStatement.executeUpdate();
						}
						catch (SQLException ex) {
							System.out.println("SQL ERROR");
							ex.printStackTrace();
						}
						importStatement.close();
						
						System.out.println("Done.");
						
						File tmpFile = new File("C:\\tmp.csv");
						tmpFile.delete();
						
						fw = new FileWriter("C:\\tmp.csv");
						writer = new BufferedWriter(fw);
					}
				}
			}
			
			writer.close();
			fw.close();
			
			System.out.println("Temp file generated, importing into database...");
			
			String importStatementString = "LOAD DATA CONCURRENT INFILE 'C:\\\\tmp.csv' INTO TABLE "+destinationTable+" FIELDS TERMINATED BY '\t' IGNORE 1 LINES";
			PreparedStatement importStatement = connect.prepareStatement(importStatementString);
			try {
			importStatement.executeUpdate();
			}
			catch (SQLException ex) {
				System.out.println("SQL ERROR");
				ex.printStackTrace();
			}
			importStatement.close();
			
			connect.close();
			System.out.println("Done.");

			File tmpFile = new File("C:\\tmp.csv");
			tmpFile.delete();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

}
