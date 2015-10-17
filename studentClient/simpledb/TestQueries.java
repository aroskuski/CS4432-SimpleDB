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
				SimpleDB.init("studentdb");
				
				// analogous to the connection
				Transaction tx = new Transaction();
				/*CS4432 Does a select on test 1*/
				String qry = "select a1,a1 from test1 where a1 = 4";
				
				/*CS4432 Gets the number of IOs at the start*/
				long StartIOs = SimpleDB.fileMgr().IONumber();
				Date StartTime = new Date();
				
				Plan p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				Scan s = p.open();

				s.close();
				tx.commit();
					
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				long EndIOs = SimpleDB.fileMgr().IONumber();
				Date EndTime = new Date();
				System.out.println("IOs for test1 select: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test1 select: " + (EndTime.getTime() - StartTime.getTime()));

				// analogous to the connection
				tx = new Transaction();
				/*CS4432 Does a select on test 2*/
				qry = "select b1,b1 from test2 where b1 = 4";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();
				
				while(s.next()){
					
				}

				s.close();
				tx.commit();
					
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test2 select: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test2 select: " + (EndTime.getTime() - StartTime.getTime()));
				
				// analogous to the connection
				tx = new Transaction();
				/*CS4432 Does a select on test 3*/
				qry = "select c1,c1 from test3 where c1 = 4";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();
				while(s.next()){
					
				}
				s.close();
				tx.commit();
					
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test3 select: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test3 select: " + (EndTime.getTime() - StartTime.getTime()));
				
				// analogous to the connection
				tx = new Transaction();
				/*CS4432 Does a select on test 4*/
				qry = "select d1,d1 from test4 where d1 = 4";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();
				while(s.next()){
					
				}
				s.close();
				tx.commit();
					
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test4 select: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test4 select: " + (EndTime.getTime() - StartTime.getTime()));
				
				// analogous to the connection
				tx = new Transaction();
				/*CS4432 Does a join on test5 and test2*/
				qry = "select e1,e2,b1,b2 from test5, test2 where e1 = b1 ";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();
				while(s.next()){
					
				}
				s.close();
				tx.commit();
					
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test5 & test2 join: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test5 & test2 join: " + (EndTime.getTime() - StartTime.getTime()));
				
				tx = new Transaction();
				/*CS4432 Does a join on test5 and test3*/
				qry = "select e1,e2,c1,c2 from test5, test3 where e1 = c1 ";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();
				while(s.next()){
					
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
				qry = "select e1,e2,d1,d2 from test5, test4 where e1 = d1 ";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();
				while(s.next()){
					
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
				qry = "select e1,e2,a1,a2 from test5, test1 where e1 = a1 ";
				
				/*CS4432 Gets the number of IOs at the start*/
				StartIOs = SimpleDB.fileMgr().IONumber();
				StartTime = new Date();
				
				p = SimpleDB.planner().createQueryPlan(qry, tx);
				
				// analogous to the result set
				s = p.open();

				while(s.next()){
					
				}
				s.close();
				tx.commit();
				
				/*CS4432 Gets the number of IOs at the end and gets the total IOs
				 * for the transaction*/
				EndIOs = SimpleDB.fileMgr().IONumber();
				EndTime = new Date();
				System.out.println("IOs for test5 & test1 join: " + (EndIOs - StartIOs));
				System.out.println("Time in ms for test5 & test1 join: " + (EndTime.getTime() - StartTime.getTime()));
				
//				tx = new Transaction();
//				/*CS4432 Does a sortmergejoin on test6 and test1*/
//				qry = "select f1,f2,a1,a2 from test6, test1 where f1 = a2";
//				
//				/*CS4432 Gets the number of IOs at the start*/
//				StartIOs = SimpleDB.fileMgr().IONumber();
//				StartTime = new Date();
//				
//				p = SimpleDB.planner().createQueryPlan(qry, tx);
//				
//				// analogous to the result set
//				s = p.open();
//
//				while(s.next()){
//					
//				}
//				s.close();
//				tx.commit();
//				
//				/*CS4432 Gets the number of IOs at the end and gets the total IOs
//				 * for the transaction*/
//				EndIOs = SimpleDB.fileMgr().IONumber();
//				EndTime = new Date();
//				System.out.println("IOs for test6 & test1 join: " + (EndIOs - StartIOs));
//				System.out.println("Time in ms for test6 & test1 join: " + (EndTime.getTime() - StartTime.getTime()));
//				
//				tx = new Transaction();
//				/*CS4432 Does a sortmergejoin on test6 and test1*/
//				qry = "select f1,f2,a1,a2 from test6, test1 where f1 = a2";
//				
//				/*CS4432 Gets the number of IOs at the start*/
//				StartIOs = SimpleDB.fileMgr().IONumber();
//				StartTime = new Date();
//				
//				p = SimpleDB.planner().createQueryPlan(qry, tx);
//				
//				// analogous to the result set
//				s = p.open();
//
//				while(s.next()){
//					
//				}
//				s.close();
//				tx.commit();
//				
//				/*CS4432 Gets the number of IOs at the end and gets the total IOs
//				 * for the transaction*/
//				EndIOs = SimpleDB.fileMgr().IONumber();
//				EndTime = new Date();
//				System.out.println("IOs for test6 & test1 join: " + (EndIOs - StartIOs));
//				System.out.println("Time in ms for test6 & test1 join: " + (EndTime.getTime() - StartTime.getTime()));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
