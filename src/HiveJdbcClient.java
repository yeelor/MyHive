import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;




public class HiveJdbcClient {
	
	private static String driverName ="org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String sql ="";
	private static ResultSet res;
	

	
	public static void main(String[] args) throws Exception {
		try {
			Class.forName(driverName);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		Connection conn = DriverManager.getConnection("jdbc:hive://master:10000/default", "hive", "guotao");
		Statement stmt = conn.createStatement();
		
		//创建表名 
		String tableName = "testHiveDriverTable";
		
		//drop and create 
		sql = "drop table "+tableName;
		stmt.executeQuery(sql);
		
		sql = "create table "+tableName + " (key int,value string) ";
		sql +=" row format delimited fields terminated by '\t'";
		stmt.executeQuery(sql);
		
		// show tables;
		sql = "show tables '"+ tableName+"'";
		System.out.println("running :"+sql);
		res = stmt.executeQuery(sql);
		System.out.println("show tables 的结果：");
		while(res.next()){
			System.out.println(res.getString(1));
		}
		
		//describe table
		sql = "describe " + tableName;
		System.out.println("Running:"+sql);
		res = stmt.executeQuery(sql);
		while(res.next()){
			System.out.println(res.getString(1)+"\t"+res.getString(2));
		}
		
		//load data into table
		String filepath ="/home/hadoop/file/hive/userinfo.txt";
		sql  ="load data local inpath '"+filepath+"' into table "+tableName;
		System.out.println(sql);
		res = stmt.executeQuery(sql);
		
		// select * 
		sql  = "select * from "+tableName;
		System.out.println(sql);
		res = stmt.executeQuery(sql);
		while(res.next()){
			System.out.println(res.getInt(1)+"\t"+res.getString(2));
		}
		
		//
		sql ="select count(1) from "+tableName;
		System.out.println(sql);
		res = stmt.executeQuery(sql);
		while(res.next()){
			System.out.println(res.getString(1));
		}
	}

}
