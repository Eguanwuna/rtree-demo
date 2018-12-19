package cn.analysys.tag.utils;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class MysqlJDBCUtil implements Serializable{
	private static final long serialVersionUID = -4003303419174869180L;
	private static BasicDataSource dataSource = null;
	private static Connection connection = null;
	private static Statement stmt = null;
	private static Logger log = Logger.getLogger(MysqlJDBCUtil.class);

	public static Connection getConnection() throws SQLException{
		if(dataSource == null) {
//			dataSource= new BasicDataSource();
//			//为数据源实例指定必须的属性
//			dataSource.setUsername(ConstantSpark.MYSQL_USER_NAME());
//			dataSource.setPassword(ConstantSpark.MYSQL_PASSWORD());
//			dataSource.setUrl(ConstantSpark.MYSQL_URL());
//			dataSource.setDriverClassName(ConstantSpark.MYSQL_DRIVER());
//			//指定数据库连接池中初始化连接数的个数
//			dataSource.setInitialSize(ConstantSpark.MYSQL_POOL_INITIAL_SIZE());
//			//指定最大的连接数:同一时刻同时向数据库申请的连接数
//			//最大空闲数，放洪峰过后，连接池中的连接过多，
//			dataSource.setMaxActive(ConstantSpark.MYSQL_POOL_MAX_SIZE());
//			//指定最小连接数:数据库空闲状态下所需要保留的最小连接数
//			//防止当洪峰到来时，再次申请连接引起的性能开销；
//			dataSource.setMinIdle(ConstantSpark.MYSQL_POOL_MIN_IDLE());
//			//最长等待时间:等待数据库连接的最长时间，单位为毫秒，超出将抛出异常
//			dataSource.setMaxWait(0);
		}
		if(connection == null) {
			connection=dataSource.getConnection();
		}
		return connection;
	}

	public static Statement getStatement(){
		if(stmt == null) {
			try {
				getConnection();
				stmt = connection.createStatement();
			} catch (SQLException e) {
				log.error("create statement error:", e);
				e.printStackTrace();
				throw new RuntimeException("create statement error!");
			}
		}
		return stmt;
	}

	public static ResultSet getResultSet(String sql) {
		ResultSet rs = null;
		try {
			getConnection();
			getStatement();
			rs =  stmt.executeQuery(sql);
		} catch (SQLException e) {
			log.error("get result error:", e);
			e.printStackTrace();
			throw new RuntimeException("get result error Error!");
		}
		return rs;
	}

	public static void closeConnection() {
		try {
			if(stmt != null) {
				stmt.close();
			}

			if(connection != null) {
				connection.close();
			}

		} catch (SQLException e) {
			log.error("close statement & connection error:", e);
			e.printStackTrace();
		}
	}

	public static void closeStatement() {
		try {
			if(stmt != null) {
				stmt.close();
			}

		} catch (SQLException e) {
			log.error("close statement error:", e);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			ResultSet rs = MysqlJDBCUtil.getResultSet( "select app_id,app_name from dim_stand_app where status = 1  and app_id=4565");
			while(rs.next()) {
				System.out.println("" + rs.getInt(1) + "----" + rs.getString(2));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}