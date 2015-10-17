/******************************************************************/
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import simpledb.remote.SimpleDriver;
public class CreateTestTables {
 final static int maxSize=100;
 /**
  * @param args
  */
 public static void main(String[] args) {
  // TODO Auto-generated method stub
  Connection conn=null;
  Driver d = new SimpleDriver();
  String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
  String url = "jdbc:simpledb://" + host;
  String qry="Create table test1" +
  "( a1 int," +
  "  a2 int"+
  ")";
  Random rand=null;
  Statement s=null;
  try {
   conn = d.connect(url, null);
   s=conn.createStatement();
   s.executeUpdate("Create table test1" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test2" +
     "( b1 int," +
     "  b2 int"+
   ")");
   s.executeUpdate("Create table test3" +
     "( c1 int," +
     "  c2 int"+
   ")");
   s.executeUpdate("Create table test4" +
     "( d1 int," +
     "  d2 int"+
   ")");
   s.executeUpdate("Create table test5" +
     "( e1 int," +
     "  e2 int"+
   ")");
   s.executeUpdate("Create table test6" +
     "( f1 int," +
     "  f2 int"+
   ")");

   s.executeUpdate("create sh index idx1 on test2 (b1)");
   s.executeUpdate("create eh index idx2 on test3 (c1)");
   s.executeUpdate("create bt index idx3 on test4 (d1)");
   for(int i=1;i<7;i++)
   {
    if(i!=5)
    {
     rand=new Random(1);// ensure every table gets the same data
     for(int j=0;j<maxSize;j++)
     {
    	 String prefix;
    	 if(i == 1){
    		 prefix = "a";
    	 } else if(i == 2){
    		 prefix = "b";
    	 } else if(i == 3){
    		 prefix = "c";
    	 } else if(i == 4){
    		 prefix = "d";
    	 } else {
    		 prefix = "f";
    	 }
      s.executeUpdate("insert into test"+i+" ("+ prefix + "1,"+ prefix + "2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
     }
    }
    else//case where i=5
    {
     for(int j=0;j<maxSize/2;j++)// insert 10000 records into test5
     {
      s.executeUpdate("insert into test"+i+" (e1,e2) values("+j+","+j+ ")");
     }
    }
   }
   conn.close();

  } catch (SQLException e) {
   // TODO Auto-generated catch block
   e.printStackTrace();
  }finally
  {
   try {
    conn.close();
   } catch (SQLException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
   }
  }
 }
}
