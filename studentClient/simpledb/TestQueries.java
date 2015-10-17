import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

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
				SimpleDB.init("cs4432db");

				// analogous to the connection
				Transaction tx = new Transaction();
				/*CS4432 Does a join on test5 and test2*/
				String qry = "select a1,a2 from test5, test2 where a1 = a2 ";
				
				/*CS4432 Gets the number of IOs at the start*/
				long StartIOs = SimpleDB.fileMgr().IONumber();
				Date StartTime = new Date();
				
				Plan p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				Scan s = p.open();

				while (s.next()) {
					String a1 = s.getString("a1"); //SimpleDB stores field names
					String a2 = s.getString("a2"); //in lower case
					System.out.println(a1 + "\t" + a2);
				}
				s.close();
				tx.commit();
					
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				long EndIOs = SimpleDB.fileMgr().IONumber();
				Date EndTime = new Date();
				System.out.println("IOs for test5 & test2 join: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test5 & test2 join: " + (EndTime.getTime() - StartTime.getTime()));
				
				tx = new Transaction();
				/*CS4432 Does a join on test5 and test3*/
				qry = "select a1,a2 from test5, test3 where a1 = a2 ";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();

				while (s.next()) {
					String a1 = s.getString("a1"); //SimpleDB stores field names
					String a2 = s.getString("a2"); //in lower case
					System.out.println(a1 + "\t" + a2);
				}
				s.close();
				tx.commit();
				
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test5 & test3 join: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test5 & test3 join: " + (EndTime.getTime() - StartTime.getTime()));
				
				tx = new Transaction();
				/*CS4432 Does a join on test5 and test4*/
				qry = "select a1,a2 from test5, test4 where a1 = a2 ";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();

				while (s.next()) {
					String a1 = s.getString("a1"); //SimpleDB stores field names
					String a2 = s.getString("a2"); //in lower case
					System.out.println(a1 + "\t" + a2);
				}
				s.close();
				tx.commit();
				
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test5 & test4 join: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test5 & test4 join: " + (EndTime.getTime() - StartTime.getTime()));
				
				tx = new Transaction();
				/*CS4432 Does a join on test5 and test1*/
				qry = "select a1,a2 from test5, test1 where a1 = a2 ";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();

				while (s.next()) {
					String a1 = s.getString("a1"); //SimpleDB stores field names
					String a2 = s.getString("a2"); //in lower case
					System.out.println(a1 + "\t" + a2);
				}
				s.close();
				tx.commit();
				
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test5 & test1 join: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test5 & test1 join: " + (EndTime.getTime() - StartTime.getTime()));
				
				tx = new Transaction();
				/*CS4432 Does a sortmergejoin on test6 and test1*/
				qry = "select a1,a2 from test6, test1 where a1 = a2";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();

				while (s.next()) {
					String a1 = s.getString("a1");
					String a2 = s.getString("a2");
					System.out.println(a1 + "\t" + a2);
				}
				s.close();
				tx.commit();
				
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test6 & test1 join: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test6 & test1 join: " + (EndTime.getTime() - StartTime.getTime()));
				
				tx = new Transaction();
				/*CS4432 Does a sortmergejoin on test6 and test1*/
				qry = "select a1,a2 from test6, test1 where a1 = a2";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();

				while (s.next()) {
					String a1 = s.getString("a1");
					String a2 = s.getString("a2");
					System.out.println(a1 + "\t" + a2);
				}
				s.close();
				tx.commit();
				
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test6 & test1 join: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test6 & test1 join: " + (EndTime.getTime() - StartTime.getTime()));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
