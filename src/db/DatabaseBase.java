package db;

import java.sql.Connection;

public class DatabaseBase {
	protected Connection selectConnection;

	public DatabaseBase(String dbHost, String dbUser, String dbPass) {
		selectConnection = Database.getConnection(dbHost, dbUser, dbPass);
	}
}
