package chemfinder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Vector;

import javax.swing.JOptionPane;

import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.chemspider.www.*;
import com.chemspider.www.InChIStub.InChIToCSIDResponse;
import com.chemspider.www.SearchStub.GetAsyncSearchResultResponse;
import com.chemspider.www.SearchStub.GetAsyncSearchStatusResponse;
import com.chemspider.www.SearchStub.SimpleSearchResponse;
import com.chemspider.www.MassSpecAPIStub.ArrayOfInt;
import com.chemspider.www.MassSpecAPIStub.ArrayOfString;
import com.chemspider.www.MassSpecAPIStub.ExtendedCompoundInfo;
import com.chemspider.www.MassSpecAPIStub.GetDatabasesResponse;
import com.chemspider.www.MassSpecAPIStub.GetExtendedCompoundInfoArrayResponse;
import com.chemspider.www.MassSpecAPIStub.SearchByMassAsyncResponse;

public class ChemicalFinder implements Runnable {

	String host;
	String user;
	String pass;
	String dbName;
	String selectString;
	String targetTableChemicals;
	String targetTableNonchemicals;
	boolean recreateTables;
	String[] storedArgs;
	String threadName;
	
	public ChemicalFinder(String[] args) {
		// TODO Auto-generated constructor stub
		super();
		storedArgs = args.clone();
	}
	
	public ChemicalFinder() {
		super();
		host = "";
		user = "";
		pass = "";
		dbName = "";
		selectString = "";
		targetTableChemicals = "";
		targetTableNonchemicals = "";
		threadName = "";
		recreateTables = true;
	}

	public static void main(String[] args) {
		if(args.length > 0 && !args[0].equalsIgnoreCase("multithread")) {
			ChemicalFinder cf = new ChemicalFinder(args);
			cf.startSearch();
		}
		else {
			//Absolute final, multithreaded fuckery
			//One thread = 1000 keywords
			int threadCount = 15;
			if(args.length > 1) {
				threadCount = Integer.parseInt(args[1]);
			}
			String pass = "";
			if(args.length > 2) {
				pass = args[2];
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
			int threadSize = 150000/threadCount;
			Vector<Thread> finderThreads = new Vector<Thread>();
			String selectTemp = "SELECT * FROM keywordlist LIMIT ";
			String[] argsTemp = {"localhost:3306", "root", pass, "datamining", "", "chemical_keywords", "nonchemical_keywords", "N", "T"};
			for(int i=0; i<threadCount; i++) {
				String select = selectTemp + Integer.toString(i*threadSize) + ","+Integer.toString(threadSize);
				String[] newArgs = argsTemp.clone();
				newArgs[4] = select;
				newArgs[8] = "T"+Integer.toString(i);
				ChemicalFinder cf = new ChemicalFinder(newArgs);
				Thread finderThread = new Thread(cf, newArgs[8]);
				finderThreads.add(finderThread);
				finderThread.start();
			}
		}
	}
	
	private static final Logger LOG = Logger.getLogger(ChemicalFinder.class.getName());
	private static String ChemSpiderToken = "9d66f3de-8a08-4e42-88da-6b714cde91d7";
	
	public void startSearch() {
		startSearch(storedArgs);
	}
	
	/**
	 * @param args
	 */
	public void startSearch(String[] args) {
		
		for(int i=0; i<args.length; i++) {
			if(i==0) {
				host = args[i];
			}
			if(i==1) {
				user = args[i];
			}
			if(i==2) {
				pass = args[i];
			}
			if(i==3) {
				dbName = args[i];
			}
			if(i==4) {
				selectString = args[i];
			}
			if(i==5) {
				targetTableChemicals = args[i];
			}
			if(i==6) {
				targetTableNonchemicals = args[i];
			}
			if(i==7) {
				if(args[i].equalsIgnoreCase("y")) {
					recreateTables = true;
				}
				else {
					recreateTables = false;
				}
			}
			if(i==8) {
				threadName = args[8];
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
		
		if(selectString.isEmpty()) {
			System.out.println("Select statement? (Warning, unsanitized, only first column will be used!)");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				selectString = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(targetTableChemicals.isEmpty()) {
			System.out.println("Target table for chemicals? (Warning, unsanitized!)");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				targetTableChemicals = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(targetTableNonchemicals.isEmpty()) {
			System.out.println("Target table for chemicals? (Warning, unsanitized!)");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				targetTableNonchemicals = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		if(args.length < 8) {
			System.out.println("Recreate target tables? (Y/N)");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				String recreate = bufferRead.readLine();
				if(recreate.equalsIgnoreCase("y")) {
					recreateTables = true;
				}
				else {
					recreateTables = false;
				}
			}
			catch(IOException ex) {
				
			}
		}
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			Connection connect = DriverManager.getConnection("jdbc:mysql://"+host+"/"+dbName+"?"
			              + "user="+user+"&password="+pass);
			
			if(recreateTables) {
				String ttcDrop = "DROP TABLE IF EXISTS `"+dbName+"`.`"+targetTableChemicals+"`;";
				String ttncDrop = "DROP TABLE IF EXISTS `"+dbName+"`.`"+targetTableNonchemicals+"`;";
				
				String ttcCreate = "CREATE  TABLE `"+dbName+"`.`"+targetTableChemicals+"` (\n"+
			  			"`keyword` VARCHAR(128) NOT NULL ,\n"+
			  			"INDEX `keyword_idx` (`keyword` ASC) ,\n"+
			    		"PRIMARY KEY (`keyword`));\n";
				
				String ttncCreate = "CREATE  TABLE `"+dbName+"`.`"+targetTableNonchemicals+"` (\n"+
			  			"`keyword` VARCHAR(128) NOT NULL ,\n"+
			  			"INDEX `keyword_idx` (`keyword` ASC) ,\n"+
			    		"PRIMARY KEY (`keyword`));\n";
				
				Statement tableMgmtStatement = connect.createStatement();
				try {
					tableMgmtStatement.execute(ttcDrop);
					tableMgmtStatement.execute(ttcCreate);
					tableMgmtStatement.execute(ttncDrop);
					tableMgmtStatement.execute(ttncCreate);
					System.out.println(threadName+":\t"+"Tables recreated");
				}
				catch(SQLException ex) {
					System.out.println("Table creation and update failed");
					System.out.println("Execute the following commands then type something:");
					System.out.println(ttcDrop);
					System.out.println(ttcCreate);
					System.out.println(ttncDrop);
					System.out.println(ttncCreate);
					BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
					try {
						String dummy = bufferRead.readLine();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				tableMgmtStatement.close();
			}
			
			HashSet<String> keywords = new HashSet<String>();
			HashSet<String> chemicals = new HashSet<String>();
			HashSet<String> nonchemicals = new HashSet<String>();
			
			Statement selectStatement = connect.createStatement();
			ResultSet selectResults = selectStatement.executeQuery(selectString);
			while(selectResults.next()) {
				keywords.add(selectResults.getString(1));
			}
			
			System.out.printf(threadName+":\t"+"%d keywords got\n", keywords.size());
			
			double tenperc = keywords.size()/10.0;
			double oneperc = keywords.size()/100.0;
			double thousandth = keywords.size()/1000.0;
			
			int total = keywords.size();
			
			//OK, we have the keywords, time for checking them against the ChemSpider
			//BasicConfigurator.configure(new Appender());
			
			int done = 0;
			for(String kw : keywords) {
				int[] searchResults = get_Search_SimpleSearch_Results("IDENTIFIER = "+kw, ChemSpiderToken);
				if(searchResults != null && searchResults.length > 0) {
					chemicals.add(kw);
				}
				else {
					nonchemicals.add(kw);
				}
				done++;
				if(tenperc > 0 && done % (int)tenperc == 0) {
					System.out.printf(threadName+":\t"+"Done: %.2f%% (%d / %d)\n", (done/tenperc)*10.0, done, total);
				}
				else if(oneperc > 0 && done % (int)oneperc == 0) {
					System.out.printf(threadName+":\t"+"Done: %.2f%% (%d / %d)\n", (float)(done/oneperc), done, total);
				}
				else if(thousandth > 0 && done % (int)thousandth == 0) {
					System.out.printf(threadName+":\t"+"Done: %.2f%% (%d / %d)\n", (done/thousandth)/10.0, done, total);
				}
			}
			
			//Test
			System.out.printf(threadName+":\t"+"Chemicals: %d, non-chemicals: %d\n", chemicals.size(), nonchemicals.size());
			String chemInsert = "INSERT INTO `"+dbName+"`.`"+targetTableChemicals+"` VALUES(?)";
			String nonchemInsert = "INSERT INTO `"+dbName+"`.`"+targetTableNonchemicals+"` VALUES(?)";
			
			connect.close();
			connect = DriverManager.getConnection("jdbc:mysql://"+host+"/"+dbName+"?"
		              + "user="+user+"&password="+pass);
			
			PreparedStatement chemInsertStatement = connect.prepareStatement(chemInsert);
			PreparedStatement nonchemInsertStatement = connect.prepareStatement(nonchemInsert);
			
			int batchSize = 100;
			
			System.out.println(threadName+":\t"+"Inserting chemicals");
			done = 0;
			for(String kw : chemicals) {
				chemInsertStatement.setString(1, kw);
				chemInsertStatement.addBatch();
				done++;
				if(done % batchSize == 0) {
					chemInsertStatement.executeBatch();
					chemInsertStatement.clearBatch();
					System.out.printf(threadName+":\t"+"Done: %d of %d\n", done, chemicals.size());
				}
			}
			
			chemInsertStatement.executeBatch();
			System.out.printf(threadName+":\t"+"Done: %d of %d\n", done, chemicals.size());
			System.out.println(threadName+":\t"+"Chemicals inserted");
			
			System.out.println(threadName+":\t"+"Inserting nonchemicals");
			done = 0;
			for(String kw : nonchemicals) {
				nonchemInsertStatement.setString(1, kw);
				nonchemInsertStatement.addBatch();
				done++;
				if(done % batchSize == 0) {
					nonchemInsertStatement.executeBatch();
					nonchemInsertStatement.clearBatch();
					System.out.printf(threadName+":\t"+"Done: %d of %d\n", done, nonchemicals.size());
				}
			}
			nonchemInsertStatement.executeBatch();
			System.out.printf(threadName+":\t"+"Done: %d of %d\n", done, nonchemicals.size());
			System.out.println(threadName+":\t"+"Nonchemicals inserted");
			System.out.println(threadName+":\t"+"Great success!");
			
			connect.close();
		}
		catch(SQLException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static int[] get_Search_SimpleSearch_Results(String query, String token) {
		int[] Output = null;
		try {
		final SearchStub thisSearchStub = new SearchStub();
		com.chemspider.www.SearchStub.SimpleSearch SimpleSearchInput = new com.chemspider.www.SearchStub.SimpleSearch();
		SimpleSearchInput.setQuery(query);
		SimpleSearchInput.setToken(token);
		final SimpleSearchResponse thisSimpleSearchResponse = thisSearchStub.simpleSearch(SimpleSearchInput);
		Output = thisSimpleSearchResponse.getSimpleSearchResult().get_int();
		} catch (Exception e) {
		LOG.log(Level.ERROR, "Problem retrieving ChemSpider webservices", e);
		}
		return Output;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		startSearch();
	}

}
