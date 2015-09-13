import java.sql.*;
import simpledb.remote.SimpleDriver;

public class SampleQueries {
	public static void main(String[] args) {
		Connection conn = null;
		try{
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();
			
			String s = "create table Gear (GName varchar(20) PRIMARY_KEY, BName varchar (20), MAbility varchar(20))";
			stmt.executeQuery(s);
			
			s = "insert into Gear(GName, BName, MAbility) values ";
			String[] vals = {"('Gas Mask', 'Forge', 'Tenacity')",
					         "('Moto Boots', 'Rockenberg', 'Quick Respawn')",
					         "('Cherry Kicks', 'Rockenberg', 'Stealth Jump')",
					         "('Octo Tee', 'Cuttlegear', 'Haunt')",
					         "('Tinted Shades', 'Zekko', 'Last-Ditch Effort')",
					         "('Basic Tee', 'SquidForce', 'Quick Respawn')"};
			
			for (String v : vals){
				stmt.executeQuery(s + v);
			}
			
			
			s = "create table Brand (BName varchar (20) PRIMARY_KEY, FavoredSub varchar(20))";
			stmt.executeQuery(s);
			
			s = "insert into Brand(BName, FavoredSub) values ";
			String[] vals2 = {"('Rockenberg', 'Run Speed Up')",
					          "('Forge', 'Special Duration Up')",
					          "('Cuttlegear', NULL)",
					          "('Zekko', 'Special Saver')",
					          "('Squidforce', 'Damage Up')",
					          "('Inkline', 'Defense Up')",
					          "('Tentatek', 'Ink Recovery Up')"};
			
			for (String v : vals2){
				stmt.executeQuery(s + v);
			}
			
			
			
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
