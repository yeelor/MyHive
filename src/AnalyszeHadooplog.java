import java.sql.ResultSet;

public class AnalyszeHadooplog {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		StringBuffer sql = new StringBuffer();

		// 第一步，在Hive中创建表
		sql.append("create table if not exists loginfo(rdate string,");
		sql.append("time array<string>,type string,relateclass string,");
		sql.append("information1 string,information2 string,information3 ");
		sql.append("string) row format delimited fields terminated by ' '");
		sql.append(" collection items terminated by ','");
		sql.append(" map keys terminated by ':'");
		
		System.out.println(sql);
		HiveUtil.createTable(sql.toString());
		
		//第二步,加载Hadoop日志 
		sql.delete(0, sql.length());
		sql.append("load data local inpath '/home/hadoop/file/hive/hadoop.log' overwrite into table loginfo");
		System.out.println(sql);
		HiveUtil.loadData(sql.toString());
		
		
		//第三步,查询有用信息
		sql.delete(0, sql.length());
		sql.append("select rdate,time[0],type,relateclass,information1,information2,information3 from loginfo where type='ERROR' ");
		System.out.println(sql);
		ResultSet rs  = HiveUtil.queryData(sql.toString());
		
		HiveUtil.hiveToMySQL(rs);
		
		DBHelper.closeHiveConn();
		
		DBHelper.closeMySQLConn();
		

	}

}
