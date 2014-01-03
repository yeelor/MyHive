import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 针对Hive的工具类
 * 
 * @author hadoop
 * 
 */
public class HiveUtil {

	// 创建表
	public static void createTable(String sql) throws Exception {
		Connection conn = DBHelper.getHiveConn();
		Statement stmt = conn.createStatement();
		stmt.executeQuery(sql);
	}

	// 依据条件查询数据
	public static ResultSet queryData(String sql) throws Exception {
		Connection conn = DBHelper.getHiveConn();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		return rs;
	}

	// 加载数据
	public static void loadData(String sql) throws Exception {
		Connection conn = DBHelper.getHiveConn();
		Statement stmt = conn.createStatement();
		stmt.executeQuery(sql);
	}

	// 通用
	public static ResultSet executeSQL(String sql) throws Exception {
		Connection conn = DBHelper.getHiveConn();
		Statement stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}

	public static void hiveToMySQL(ResultSet rs) throws Exception {
		Connection conn = DBHelper.getMySQLConn();
//		Connection conn = JDBCUtil.getConnection();
		Statement stmt = conn.createStatement();
		while (rs.next()) {
			String rdate = rs.getString(1);
			String time = rs.getString(2);
			String type = rs.getString(3);
			String relateclass = rs.getString(4);
			String information = rs.getString(5) +" "+ rs.getString(6) + " "+rs.getString(7);

			StringBuffer sql = new StringBuffer();
			sql.append("insert into hadooplog values(0,'");
			sql.append(rdate + "','");
			sql.append(time + "','");
			sql.append(type + "','");
			sql.append(relateclass + "','");
			sql.append(information + "')");

			int i = stmt.executeUpdate(sql.toString());

		}
	}

}
