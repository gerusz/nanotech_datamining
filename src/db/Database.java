package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Database {

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection(String host, String username, String password) {
		Connection connect = null;
		String url = "jdbc:mysql://" + host + "/nano_tech?useServerPrepStmts=false&rewriteBatchedStatements=true";
		try {
			connect = (Connection) DriverManager.getConnection(url, username, password);
			System.out.println("Connected to the database");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connect;
	}

	public static Statement createStreamingStatement(final Connection connection)
			throws SQLException {
		Statement statement = (Statement) connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		// This makes sure the records are read 1 at a time, in stead of reading the whole result in memory
		// (https://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html)
		statement.setFetchSize(Integer.MIN_VALUE);
		return statement;
	}

	public static Statement createStatement(final Connection connection) throws SQLException {
		Statement statement = (Statement) connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		return statement;
	}
}
