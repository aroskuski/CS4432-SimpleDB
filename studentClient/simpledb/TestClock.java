import simpledb.tx.Transaction;
import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.query.*;
import simpledb.server.SimpleDB;

/* This is a version of the StudentMajor program that
 * accesses the SimpleDB classes directly (instead of
 * connecting to it as a JDBC client).  You can run it
 * without having the server also run.
 * 
 * These kind of programs are useful for debugging
 * your changes to the SimpleDB source code.
 */

public class TestClock {
	public static void main(String[] args) {
		try {
			
			System.out.println("prepping db");
			
			// analogous to the driver
			SimpleDB.init("studentdb");
			SimpleDB.initFileLogAndBufferMgr("studentdb", true);
			
			Transaction tx = new Transaction();
			
			
			String s = "create table Gear (GName varchar(20), BrandId int, MAbility varchar(20))";
			System.out.println(s);
			SimpleDB.planner().executeUpdate(s, tx);
			
			s = "insert into Gear(GName, BrandId, MAbility) values ";
			String[] vals = {"('Gas Mask', 2, 'Tenacity')",
					         "('Moto Boots', 1, 'Quick Respawn')",
					         "('Cherry Kicks', 1, 'Stealth Jump')",
					         "('Octo Tee', 3, 'Haunt')",
					         "('Tinted Shades', 4, 'Last-Ditch Effort')",
					         "('Basic Tee', 5, 'Quick Respawn')"};
			
			for (String v : vals){
				System.out.println(s + v);
				SimpleDB.planner().executeUpdate(s + v, tx);
			}
			
			
			s = "create table Brand (Bid int, BName varchar(20), FavoredSub varchar(20))";
			System.out.println(s);
			SimpleDB.planner().executeUpdate(s, tx);
			
			s = "insert into Brand(Bid, BName, FavoredSub) values ";
			String[] vals2 = {"(1, 'Rockenberg', 'Run Speed Up')",
					          "(2, 'Forge', 'Special Duration Up')",
					          "(3, 'Cuttlegear', 'NULL')",
					          "(4, 'Zekko', 'Special Saver')",
					          "(5, 'Squidforce', 'Damage Up')",
					          "(6, 'Inkline', 'Defense Up')",
					          "(7, 'Tentatek', 'Ink Recovery Up')"};
			
			for (String v : vals2){
				System.out.println(s + v);
				SimpleDB.planner().executeUpdate(s + v, tx);
			}
			
			tx.commit();
			
			System.out.println("Reinitializing file, log, and buffer managers");
			SimpleDB.initFileLogAndBufferMgr("studentdb", true);
			
			BufferMgr bm = SimpleDB.bufferMgr();
			
			System.out.println("Initial state of buffer");
			System.out.println(bm);
			
			Block b1 = new Block("gear.tbl", 0);
			Block b2 = new Block("gear.tbl", 1);
			Block b3 = new Block("gear.tbl", 2);
			Block b4 = new Block("gear.tbl", 3);
			Block b5 = new Block("gear.tbl", 4);
			Block b6 = new Block("brand.tbl", 0);
			Block b7 = new Block("brand.tbl", 1);
			Block b8 = new Block("brand.tbl", 2);
			Block b9 = new Block("brand.tbl", 3);
			Block b10 = new Block("brand.tbl", 4);
			
			System.out.println("b1 = " + b1);
			System.out.println("b2 = " + b2);
			System.out.println("b3 = " + b3);
			System.out.println("b4 = " + b4);
			System.out.println("b5 = " + b5);
			System.out.println("b6 = " + b6);
			System.out.println("b7 = " + b7);
			System.out.println("b8 = " + b8);
			System.out.println("b9 = " + b9);
			System.out.println("b10 = " + b10);
			
			System.out.println("pinning b1-b8");
			
			Buffer buff1 = bm.pin(b1);
			Buffer buff2 = bm.pin(b2);
			Buffer buff3 = bm.pin(b3);
			Buffer buff4 = bm.pin(b4);
			Buffer buff5 = bm.pin(b5);
			Buffer buff6 = bm.pin(b6);
			Buffer buff7 = bm.pin(b7);
			Buffer buff8 = bm.pin(b8);

			System.out.println(bm);
			
			System.out.println("unpinning b7");
			bm.unpin(buff7);
			System.out.println(bm);
			
			System.out.println("pinning b9");
			bm.pin(b9);
			System.out.println(bm);
			
			System.out.println("attempting to pin b10 (should fail)");
			try{
				bm.pin(b10);
			} catch (BufferAbortException e){
				System.out.println(bm);
			}
			
			System.out.println("unpinning b5");
			bm.unpin(buff5);
			System.out.println(bm);
			
			System.out.println("unpinning b3");
			bm.unpin(buff3);
			System.out.println(bm);
			
			System.out.println("pinning b7");
			bm.pin(b7);
			System.out.println(bm);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
