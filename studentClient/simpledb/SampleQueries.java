import java.sql.*;
import simpledb.remote.SimpleDriver;

public class SampleQueries {
	public static void main(String[] args) {
		Connection conn = null;
		try{
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();
			
			String s = "create table Gear (GName varchar(20), BrandId int, MAbility varchar(20))";
			System.out.println(s);
			stmt.executeUpdate(s);
			
			s = "insert into Gear(GName, BrandId, MAbility) values ";
			String[] vals = {"('Gas Mask', 2, 'Tenacity')",
					         "('Moto Boots', 1, 'Quick Respawn')",
					         "('Cherry Kicks', 1, 'Stealth Jump')",
					         "('Octo Tee', 3, 'Haunt')",
					         "('Tinted Shades', 4, 'Last-Ditch Effort')",
					         "('Basic Tee', 5, 'Quick Respawn')"};
			
			for (String v : vals){
				System.out.println(s + v);
				stmt.executeUpdate(s + v);
			}
			
			
			s = "create table Brand (Bid int, BName varchar(20), FavoredSub varchar(20))";
			System.out.println(s);
			stmt.executeUpdate(s);
			
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
				stmt.executeUpdate(s + v);
			}
			
			String qry = "select GName, BName, FavoredSub from Gear, Brand where BrandId = Bid";
			System.out.println(qry);
			ResultSet rs = stmt.executeQuery(qry);
			
			while(rs.next()){
				System.out.println(rs.getString("GName") + "\t" + rs.getString("BName") +
						"\t" + rs.getString("FavoredSub"));
			}
			
			rs.close();
			
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
