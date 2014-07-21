package com.cloudera.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ClouderaHiveJdbcExample {
	
	// here is an example query based on one of the Hue Beeswax sample tables 
	private static final String SQL_STATEMENT = "select * from tmp_hotel_info limit 50";
	
	// set the impalad host
  private static final String HIVE_HOST = "192.168.9.247";
	
	// port 21050 is the default impalad JDBC port 
  private static final String HIVE_JDBC_PORT = "50000";

  private static final String CONNECTION_URL = "jdbc:hive2://" + HIVE_HOST
      + ':' + HIVE_JDBC_PORT + "/default";

	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  private static final String HIVE_USERNAME = "hadoop";
  private static final String HIVE_PASSWORD = "";

	public static void main(String[] args) {

		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + CONNECTION_URL);
		System.out.println("Running Query: " + SQL_STATEMENT);

		Connection con = null;

		try {

			Class.forName(JDBC_DRIVER_NAME);

      con =
          DriverManager.getConnection(CONNECTION_URL, HIVE_USERNAME,
              HIVE_PASSWORD);

			Statement stmt = con.createStatement();

			ResultSet rs = stmt.executeQuery(SQL_STATEMENT);

			System.out.println("\n== Begin Query Results ======================");

			// print the results to the console
			while (rs.next()) {
				// the example query returns one String column
				System.out.println(rs.getString(1));
			}

			System.out.println("== End Query Results =======================\n\n");

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}
	}
}
