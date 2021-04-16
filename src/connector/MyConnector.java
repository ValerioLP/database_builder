package connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MyConnector {
	
	static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
		
		public static void main(String[] args) throws SQLException, ClassNotFoundException {
			Class.forName("com.mysql.cj.jdbc.Driver");
			System.out.println("Connecting to database...");
			Connection conn = DriverManager
					.getConnection("jdbc:mysql://localhost:3306/", "root", "1febbraio1992");
			
			System.out.println("Creating statement...");
			Statement stmt = conn.createStatement();
			
			String sql = "insert into Automobile(Codice,Modello,Marca) values() ";
		}
	
}
