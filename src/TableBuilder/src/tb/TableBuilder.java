package tb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

public class TableBuilder {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String host;
		String user;
		String pass;
		String dbName;
		String selectString;
		String outputFile;
		int colToRow;
		int colToColumn;
		int colToData;
		boolean dataIsFraction = false;
		boolean dataIsString = false;
		
		host = "";
		user = "";
		pass = "";
		dbName = "";
		selectString = "";
		outputFile = "";
		colToRow = 0;
		colToColumn = 0;
		colToData = 0;
		
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
			System.out.println("Select statement? (Warning, unsanitized!)");
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				selectString = bufferRead.readLine();
			}
			catch(IOException ex) {
				
			}
		}
		
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			Connection connect = DriverManager.getConnection("jdbc:mysql://"+host+"/"+dbName+"?"
			              + "user="+user+"&password="+pass);
			
			Statement selectStatement = connect.createStatement();
			ResultSet selectResults = selectStatement.executeQuery(selectString);
			ResultSetMetaData selectMetadata = selectResults.getMetaData();
			
			//Get the columns of the statement...
			
			Vector<String> columns = new Vector<String>();
			
			int columnCount = selectMetadata.getColumnCount();
			
			for(int i=1; i<=columnCount; i++) {
				columns.add(selectMetadata.getColumnName(i));
			}
			
			//Ask the user
			
			System.out.println("Which column should be turned into rows?");
			
			System.out.println("[0]Single row");
			
			for(int i=0; i<columns.size(); i++) {
				System.out.println("[" + Integer.toString(i+1) + "]" + columns.elementAt(i));
			}
			
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				colToRow = Integer.parseInt(bufferRead.readLine());
			}
			catch(IOException ex) {
				
			}
			
			System.out.println("Which column should be turned into columns?");
			
			System.out.println("[0]Single column");
			
			for(int i=0; i<columns.size(); i++) {
				System.out.println("[" + Integer.toString(i+1) + "]" + columns.elementAt(i));
			}
			
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				colToColumn = Integer.parseInt(bufferRead.readLine());
			}
			catch(IOException ex) {
				
			}
			
			System.out.println("Data column?");
			
			for(int i=0; i<columns.size(); i++) {
				System.out.println("[" + Integer.toString(i+1) + "]" + columns.elementAt(i));
			}
			
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				colToData = Integer.parseInt(bufferRead.readLine());
			}
			catch(IOException ex) {
				
			}
			
			System.out.println("Data type? (F: fraction, I: integer, S: string)");
			
			try {
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				String dTypeString = bufferRead.readLine();
				if(dTypeString.equalsIgnoreCase("f")) {
					dataIsFraction = true;
				}
				else if(dTypeString.equalsIgnoreCase("s")) {
					dataIsString = true;
				}
			}
			catch(IOException ex) {
				
			}
			
			Vector<String> tableRows = new Vector<String>();
			Vector<String> tableCols = new Vector<String>();
			HashMap<String, HashMap<String, Object>> dataTable = new HashMap<String, HashMap<String, Object>>();
			
			while(selectResults.next()) {
				String row = "Data";
				if(colToRow != 0) {
					row = selectResults.getString(colToRow);
				}
				if(!tableRows.contains(row)) {
					HashMap<String, Object> tmp = new HashMap<String, Object>();
					dataTable.put(row, tmp);
					tableRows.add(row);
				}
				
				String col = "Data";
				if(colToColumn != 0) {
					col = selectResults.getString(colToColumn);
				}
				if(!tableCols.contains(col)) {
					tableCols.add(col);
				}
				if(dataIsFraction) {
					dataTable.get(row).put(col, selectResults.getDouble(colToData));
				}
				else if(dataIsString) {
					dataTable.get(row).put(col,  selectResults.getString(colToData));
				}
				else {
					dataTable.get(row).put(col, selectResults.getInt(colToData));
				}
			}
						
			
			System.out.println("Data table completed. Saving file...");
			
			//Sort the rows and the columns
			Collections.sort(tableRows);
			Collections.sort(tableCols);
			
			if(outputFile.isEmpty()) {
				System.out.println("Output file?");
				try {
					BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
					outputFile = bufferRead.readLine();
				}
				catch(IOException ex) {
					
				}
			}
			
			//Open the file
			FileWriter fw = new FileWriter(outputFile);
			BufferedWriter writer = new BufferedWriter(fw);
			
			//Let's just hope that the metadata isn't closed...
			
			//Write header rows
			
			writer.write(",");
			for(int i=0; i<tableCols.size(); i++) {
				writer.write("\"" + selectMetadata.getColumnName(colToData) + "\"");
				if(i != tableCols.size()-1) {
					writer.write(",");
				}
				else {
					writer.write("\n");
				}
			}
			
			writer.write(selectMetadata.getColumnName(colToRow) + ",");
			for(int i=0; i<tableCols.size(); i++) {
				writer.write("\"" + tableCols.elementAt(i) + "\"");
				if(i != tableCols.size()-1) {
					writer.write(",");
				}
				else {
					writer.write("\n");
				}
			}
			
			//Write the actual data
			
			for(int row = 0; row < tableRows.size(); row++) {
				String rowName = tableRows.elementAt(row);
				writer.write("\"" + rowName + "\",");
				HashMap<String, Object> rowData = dataTable.get(rowName);
				for(int col = 0; col < tableCols.size(); col++) {
					String colName = tableCols.elementAt(col);
					if(rowData.containsKey(colName)) {
						if(dataIsFraction) {
							writer.write(Double.toString(((Number)rowData.get(colName)).doubleValue()));
						}
						else if(dataIsString) {
							writer.write((String)rowData.get(colName));
						}
						else {
							writer.write(Integer.toString(((Number)rowData.get(colName)).intValue()));
						}
					}
					else { 
						writer.write("0");
					}
					if(col != tableCols.size()-1) {
						writer.write(",");
					}
					else {
						writer.write("\n");
					}
				}
			}
			
			//Close the file
			selectStatement.close();
			connect.close();
			writer.close();
			fw.close();
			
			System.out.println("Done.");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

}
