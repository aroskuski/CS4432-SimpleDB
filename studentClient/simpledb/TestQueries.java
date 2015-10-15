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
				// analogous to the driver
				SimpleDB.init("studentdb");

				// analogous to the connection
				Transaction tx = new Transaction();
				String qry = "select b1,b2,a1,a2 from test5, test2 where b1 = a1 ";
				
				Plan p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				/*CS4432 Gets the number of IOs at the start*/
				long StartIOs = SimpleDB.fileMgr().IONumber();
				
				// analogous to the result set
				Scan s = p.open();

				while (s.next()) {
					String a1 = s.getString("a1"); //SimpleDB stores field names
					String b1 = s.getString("b1"); //in lower case
					System.out.println(a1 + "\t" + b1);
				}
				s.close();
				tx.commit();
				
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				long EndIOs = SimpleDB.fileMgr().IONumber();
				System.out.println((EndIOs - StartIOs));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
