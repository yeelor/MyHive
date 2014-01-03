

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
/**
 * JDBC操作基本工具类
 * @author GT
 *
 */
public class JDBCUtil {
	
	// 数据库连接参数
//	public static String db ="jdbc:mysql://localhost:3306/dhu_stu";
	public static String db ="jdbc:mysql://master/hive_stats?user=hive&password=guotao";
	public static String user ="root";
	public static String password = "guotao";
	
	static Connection conn;

	/**
	 * 获取数据库连接
	 * @return
	 */
	public static Connection getConnection() {
		Connection con = null;	//创建用于连接数据库的Connection对象
		try {
			Class.forName("com.mysql.jdbc.Driver");// 加载Mysql数据驱动
//			con = DriverManager.getConnection(db, user, password);// 创建数据连接	
			con = DriverManager.getConnection(db);// 创建数据连接	
		} catch (Exception e) {
			System.out.println("数据库连接失败" + e.getMessage());
		}
		return con;	//返回所建立的数据库连接
	}
	
	/**
	 * 执行insert,update,delete语句,返回影响的记录条数
	 * @param sql
	 * @return
	 */
	public static int executSQL (String sql){
		try {
			conn = getConnection();	// 首先要获取连接，即连接到数据库
			Statement st = (Statement) conn.createStatement();	// 创建用于执行静态sql语句的Statement对象			
			int count = st.executeUpdate(sql);	// 执行sql语句，并返回插入数据的个数
			conn.close();	//关闭数据库连接
			return count;
		} catch (SQLException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	/**未经过测试
	 * 执行查询语句，返回结果集rs
	 * @param sql
	 */
	public static ResultSet query(String sql) {
		try {
			conn = getConnection();	//同样先要获取连接，即连接到数据库
			Statement st = (Statement) conn.createStatement();	//创建用于执行静态sql语句的Statement对象，st属局部变量
			ResultSet rs = st.executeQuery(sql);	//执行sql查询语句，返回查询数据的结果集
			//打印出结果集的试例
//			while (rs.next()) {	
//				String name = rs.getString("name");
//				int age = rs.getInt("age");
//				System.out.println(name + " " + age);
//			}
			conn.close();	//关闭数据库连接
			return rs;  
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 测试一下
	 * @param args
	 */
	public static void main(String[] args) {		
		String sql =  "show tables";
		System.out.println(query(sql));
	}
	
}


