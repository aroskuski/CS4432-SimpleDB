import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TestQueries {
	public static void main(String[] args) {	
		Connection conn = null;
			try {
				// Step 1: connect to database server
				Driver d = new SimpleDriver();
				conn = d.connect("jdbc:simpledb://localhost", null);

				// Step 2: execute the query
				Statement stmt = conn.createStatement();
				String qry = "select b1,b2,a1,a2 from test5, test2 where b1 = a1 ";
				long StartIOs = SimpleDB.fileMgr().IONumber();
				ResultSet rs = stmt.executeQuery(qry);

				// Step 3: loop through the result set
				System.out.println("Name\tMajor");
				while (rs.next()) {
					String a1 = rs.getString("a1"); //SimpleDB stores field names
					String b1 = rs.getString("b1"); //in lower case
					System.out.println(a1 + "\t" + b1);
				}
				rs.close();
				
				long EndIOs = SimpleDB.fileMgr().IONumber();
				System.out.println((EndIOs - StartIOs));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
