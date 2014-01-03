import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * 该类的主要功能是负责建立Hive与MySQL的连接 
 * 单例模式
 * @author hadoop
 *
 */
public class DBHelper {

	private static Connection connToHive = null;
	private static Connection connToMySQL = null;
	
	// 获得与Hive连接 ,如果连接已经初始化,则直接返回 
	public static Connection getHiveConn ()throws SQLException, ClassNotFoundException{
		if(connToHive == null){
			Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
			connToHive = DriverManager.getConnection("jdbc:hive://master/default", "hive","guotao");
		}
		return connToHive;
	}
	

	
	
	//获得MySQL连接 
	public static Connection getMySQLConn(){
		if(connToMySQL == null){
			try {
				String db ="jdbc:mysql://master:3306/hivetest";
				Class.forName("com.mysql.jdbc.Driver");
				//			connToMySQL = DriverManager.getConnection("jdbc:msyql://master:3306/hivetest?useUnicode=true&characterEncoding=UTF-8", "hivetest", "guotao");
				connToMySQL=DriverManager.getConnection(db, "hivetest", "guotao");
			} catch (Exception e) {
				System.out.println("数据库连接失败" + e.getMessage());
			} 
		}
		return connToMySQL;
	}
	
	public static void closeHiveConn() throws SQLException{
		if(connToHive !=null){
			connToHive.close();
		}
	}
	
	public static void closeMySQLConn() throws SQLException{
		if(connToMySQL !=null)
			connToMySQL.close();
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
