package com.ibstech.xdr.quarteranalysis.utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.types.StructType;

import scala.collection.Iterator;

public class BatchInputDb {
	private static final String HOTAREA_CELL_SQL = "INSERT INTO HW4G_HOTAREA_CELL_15MIN(TTIME,CITY_NAME,SCENE,HOTAREA,TAC,CELL_ID,CELL_NAME,PROCEDURE_TYPE,INTERVAL,ATTNBR,SUCCNBR,TOTALRESPTIME,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	// private static final String HOTAREA_APP_SQL = "INSERT INTO
	// HW4G_HOTAREA_APP_15MIN(TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFIC_FIVEHUNDRED,TOTALRESPTIME_FIVEHUNDRED)
	// VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String F_RY_HTTP_CELL_15M_SQL = "INSERT INTO QY_CHENLANG.F_RY_HTTP_CELL_15M(PROCEDURE_STARTTIME,INTERFACE,TRANSACTION_TYPE,APP_TYPE,APP_SUB_TYPE,APP_CONTENT,USER_IPV4,USER_IPV6,USER_PORT,APP_SERVER_IP_IPV4,APP_SERVER_IP_IPV6,APP_SERVER_PORT,BUSS_BROWSER,PORTAL_APP_SET,ADSL_ACCOUNT,BAS_IPV4,BRAS_NAME,HTTP_WAP_AFFAIR_STATUS,HTTP_CONTENT_TYPE,BUSS_BEHAVIOR_FLAG,APP_STATUS,CONTENT_LENGTH,BUSS_FINISH_FLAG,RESPONSE_TIME,BUSS_DELAY) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String F_RY_TCP_CELL_15M_SQL = "INSERT INTO QY_CHENLANG.F_RY_TCP_CELL_15M(PROCEDURE_STARTTIME,INTERFACE,USER_IPV4,USER_IPV6,USER_PORT,APP_SERVER_IP_IPV4,APP_SERVER_IP_IPV6,APP_SERVER_PORT,ADSL_ACCOUNT,BAS_IPV4,BRAS_NAME,TCP_LINK_STATUS,TCP_CREATELINK_TRYTIMES,TCP_CREATELINK_RESPONSE_DELAY,TCP_CREATELINK_CONFIRM_DELAY,SUCCESS_DELAY) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String HOTAREA_USERNBR_SQL = "INSERT INTO HW4G_HOTAREA_USERNBR_15MIN(TTIME,CITY_NAME,SCENE,HOTAREA,S1MMEUSERNBR,HTTPUSERNBR) VALUES(?,?,?,?,?,?)";

	/**
	 * save into F_RY_HTTP_CELL_15M
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void saveResultToFryHTTPcell15m(List<String> rows) {
		Connection conn = ConnectionUtil.getConnection();
		PreparedStatement preparedStatement = null;
		try {
			conn.setAutoCommit(false);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			preparedStatement = conn.prepareStatement(F_RY_HTTP_CELL_15M_SQL);
			for (String row : rows) {
				// System.out.println("row --------"+row);
				String[] ss = row.split(",", -1);
				// TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFIC500,TOTALRESPTIME500
				preparedStatement.setTimestamp(1, new Timestamp(df.parse(ss[0]).getTime()));
				preparedStatement.setString(2, ss[1]);
				preparedStatement.setInt(3, Integer.parseInt(ss[2]));
				preparedStatement.setInt(4, Integer.parseInt(ss[3]));
				preparedStatement.setInt(5, Integer.parseInt(ss[4]));
				preparedStatement.setInt(6, Integer.parseInt(ss[5]));
				preparedStatement.setInt(7, Integer.parseInt(ss[6]));
				preparedStatement.setString(8, ss[7]);

				preparedStatement.setInt(9, Integer.parseInt(ss[8]));
				preparedStatement.setInt(10, Integer.parseInt(ss[9]));
				preparedStatement.setString(11, ss[10]);

				preparedStatement.setInt(12, Integer.parseInt(ss[11]));
				preparedStatement.setInt(13, Integer.parseInt(ss[12]));
				preparedStatement.setInt(14, Integer.parseInt(ss[13]));

				preparedStatement.setString(15, ss[14]);
				preparedStatement.setInt(16, Integer.parseInt(ss[15]));
				preparedStatement.setString(17, ss[16]);
				preparedStatement.setInt(18, Integer.parseInt(ss[17]));
				preparedStatement.setString(19, ss[18]);
				preparedStatement.setInt(20, Integer.parseInt(ss[19]));
				preparedStatement.setInt(21, Integer.parseInt(ss[20]));
				preparedStatement.setInt(22, Integer.parseInt(ss[21]));
				preparedStatement.setInt(23, Integer.parseInt(ss[22]));
				preparedStatement.setInt(24, Integer.parseInt(ss[23]));
				preparedStatement.setInt(25, Integer.parseInt(ss[24]));
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			ConnectionUtil.commit();
			ConnectionUtil.close();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e1) {
			e1.printStackTrace();
		} finally {
			try {
				ConnectionUtil.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * save into F_RY_TCP_CELL_15M
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void saveResultIntoFryTCPcell15m(List<String> rows) {

		Connection conn = null;
		PreparedStatement preparedStatement = null;
		try {
			conn = ConnectionUtil.getConnection();
			conn.setAutoCommit(false);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			preparedStatement = conn.prepareStatement(F_RY_TCP_CELL_15M_SQL);
			int count = 0;
			for (String row : rows) {
				String[] ss = row.split(Constant.COMMA, -1);
				// System.out.println(ss[0]+"\t"+ss[1]+"\t"+ss[2]+"\t"+ss[3]+"\t"+ss[4]+"\t"+ss[5]+"\t"+ss[6]+"\t"+ss[7]+"\t"+ss[8]+"\t"+ss[9]+"\t"+ss[10]+"\t"+ss[11]+"\t"+ss[12]+"\t"+ss[13]+"\t"+ss[14]+"\t"+ss[15]);
				// TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFIC500,TOTALRESPTIME500
				preparedStatement.setTimestamp(1, new Timestamp(df.parse(ss[0]).getTime()));
				preparedStatement.setString(2, ss[1]);
				preparedStatement.setString(3, ss[2]);
				preparedStatement.setString(4, ss[3]);
				preparedStatement.setInt(5, Integer.parseInt(ss[4]));
				preparedStatement.setString(6, ss[5]);
				preparedStatement.setString(7, ss[6]);
				preparedStatement.setInt(8, Integer.parseInt(ss[7]));
				preparedStatement.setString(9, ss[8]);
				preparedStatement.setString(10, ss[9]);
				preparedStatement.setString(11, ss[10]);
				preparedStatement.setInt(12, Integer.parseInt(ss[11]));
				preparedStatement.setLong(13, Long.parseLong(ss[12]));
				preparedStatement.setLong(14, Long.parseLong(ss[13]));
				preparedStatement.setLong(15, Long.parseLong(ss[14]));
				preparedStatement.setLong(16, Long.parseLong(ss[15]));
				preparedStatement.addBatch();
				count++;
				if (count % 1000 == 0) {
					preparedStatement.executeBatch();
					ConnectionUtil.commit();
				}
			}
			preparedStatement.executeBatch();
			ConnectionUtil.commit();
			ConnectionUtil.close();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e1) {
			e1.printStackTrace();
		} finally {
			try {
				ConnectionUtil.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * addOrCompressPartition
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void addOrCompressPartition(String FulltableName,String tableName,String currentHourPartitionName,String currentHourValue,String lastHourPartitionName,String thirtyDaysBeforeParitionName) {

		Connection conn = null;
		Statement statement = null;
		try {
			conn = ConnectionUtil.getConnection();
			//compress partition sql
			String compressSql = "alter table "+FulltableName+" move partition "+lastHourPartitionName+" tablespace "+Constant.DB_OWNER+" compress for query high";
			//add partition sql
			String addPartitionSql = "alter table "+FulltableName+" add partition "+currentHourPartitionName+" values ("+Integer.parseInt(currentHourValue)+") TABLESPACE "+Constant.DB_OWNER+"";
			//drop partition sql
			String dropPartitionSql = "alter table "+FulltableName+" drop partition "+thirtyDaysBeforeParitionName;
			         
			System.out.println("addPartitionSql\t"+addPartitionSql);
			System.out.println("compressSql\t"+compressSql);
			System.out.println("dropPartitionSql\t"+dropPartitionSql);
			statement = conn.createStatement();
			//if last hour's partition is not compressed
			ResultSet rs = statement.executeQuery("select 1 from user_tab_partitions where table_name='"+tableName+"' and partition_name='"+lastHourPartitionName+"' and compression='DISABLED'");
			if(rs.next()){
				statement.execute(compressSql);
			}
			//if current day partition don't exist
			rs = statement.executeQuery("select 1 from user_tab_partitions where table_name='"+tableName+"' and partition_name='"+currentHourPartitionName+"' ");
			if(!rs.next()){
				statement.execute(addPartitionSql);
			} 
			//remove partition of thirty days ago
			rs = statement.executeQuery("select 1 from user_tab_partitions where table_name='"+tableName+"' and partition_name='"+thirtyDaysBeforeParitionName+"' ");
			if(rs.next()){
				statement.execute(dropPartitionSql);
			}                    
			//statement.execute("alter table "+FulltableName+" drop partition "+" F_RY_MAIL_PERF_P_2017092009");
			
			ConnectionUtil.commit();
			ConnectionUtil.close();
		}catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				ConnectionUtil.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * addOrCompressPartition
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void addOrCompressPartition2(String FulltableName,String tableName,String currentHourPartitionName,String currentHourValue,String lastHourPartitionName,String thirtyDaysBeforeParitionName) {

		Connection conn = null;
		Statement statement = null;
		try {
			conn = ConnectionUtil.getConnection();
			//compress partition sql
			//add partition sql
			String addPartitionSql = "alter table "+FulltableName+" add partition "+currentHourPartitionName+" values ("+Integer.parseInt(currentHourValue)+") TABLESPACE "+Constant.DB_OWNER+"";
//			String addPartitionSql = "alter table "+FulltableName+" add partition F_RY_TCP_PERF_P_2017091009 values ("+Integer.parseInt(currentHourValue)+") TABLESPACE "+Constant.DB_OWNER+"";
			//drop partition sql
			String dropPartitionSql = "alter table "+FulltableName+" drop partition "+currentHourPartitionName;
//			String dropPartitionSql = "alter table "+FulltableName+" drop partition F_RY_TCP_CELL_P_2017091009";   
//			String dropPartitionSql2 = "alter table "+FulltableName+" drop partition F_RY_TCP_PERF_P_2017091009";  
			System.out.println("addPartitionSql\t"+addPartitionSql+"\n dropPartitionSql"+dropPartitionSql);         
			statement = conn.createStatement();           
			ResultSet rs = statement.executeQuery("select 1 from user_tab_partitions where table_name='"+tableName+"' and partition_name='"+currentHourPartitionName+"' and compression='DISABLED'");
			if(rs.next()){
				statement.execute(dropPartitionSql);
			} 
			    
			
			//if last hour's partition is not compressed
			statement.execute(addPartitionSql);
			ConnectionUtil.commit();
			ConnectionUtil.close();
		}catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				ConnectionUtil.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * save into TmpHttpUserCell
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void saveResultIntoTmpHttpUserCell(List<String> rows, String quartertime) {

		Connection conn = ConnectionUtil.getConnection();
		PreparedStatement preparedStatement = null;
		Statement stmt = null;
		String quarterFormat = Utility.getQuarterFormat(quartertime);
		String createSql = "create table USERCELL_HTTP_" + quarterFormat
				+ " ( ttime timestamp(3),city_name varchar2(50), cell_id varchar2(50), HTTPUserNbr integer) ";
		System.out.println("createSql --" + createSql);
		try {
			conn.setAutoCommit(false);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			stmt = conn.createStatement();
			stmt.executeUpdate(createSql);

			String insertSql = "insert into USERCELL_HTTP_" + quarterFormat + " values (?,?,?,?)";
			System.out.println("insertSql --" + insertSql);
			preparedStatement = conn.prepareStatement(insertSql);
			for (String row : rows) {
				String[] ss = row.split(",", -1);
				// TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFIC500,TOTALRESPTIME500
				preparedStatement.setTimestamp(1, new Timestamp(df.parse(ss[0]).getTime()));
				preparedStatement.setString(2, ss[1]);
				preparedStatement.setString(3, ss[2]);
				preparedStatement.setLong(4, Long.parseLong(ss[3]));
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			// insert into select
			String updateSql = "insert into HW4G_HOTAREA_USERNBRCELL_15MIN select mme.ttime,mme.city_name,mme.scene,mme.hotarea,mme.tac,mme.cell_id,mme.cell_Name,mme.S1mmeUserNbr,http.HTTPUserNbr from USERCELL_S1MME_"
					+ quarterFormat + "  mme " + " join USERCELL_HTTP_" + quarterFormat + "  http"
					+ " on mme.ttime=http.ttime and mme.city_name=http.city_name and mme.cell_id=http.cell_id";
			System.out.println("updateSql --" + updateSql);
			stmt.executeUpdate(updateSql);
			// String dropSql = "drop table
			// USERCELL_S1MME_"+quarterFormat+";drop table
			// USERCELL_HTTP_"+quarterFormat;
			String dropSql = "drop table USERCELL_S1MME_" + quarterFormat;
			stmt.executeUpdate(dropSql);
			dropSql = "drop table USERCELL_HTTP_" + quarterFormat;
			stmt.executeUpdate(dropSql);
			System.out.println("dropSql --" + dropSql);

			ConnectionUtil.commit();
			ConnectionUtil.close();

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				ConnectionUtil.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	

	/**
	 * save into TmpHttpUserNbr
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void saveResultIntoTmpHttpUserNbr(List<String> rows, String quartertime) {

		Connection conn = ConnectionUtil.getConnection();
		PreparedStatement preparedStatement = null;
		Statement stmt = null;
		String quarterFormat = Utility.getQuarterFormat(quartertime);
		String createSql = "create table USERNBR_HTTP_" + quarterFormat
				+ " ( ttime timestamp(3),city_name varchar2(50), scene varchar2(50),hotarea varchar2(50), HTTPUserNbr integer) ";
		System.out.println("createSql --" + createSql);
		try {
			conn.setAutoCommit(false);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			stmt = conn.createStatement();
			stmt.executeUpdate(createSql);
			String insertSql = "insert into USERNBR_HTTP_" + quarterFormat + " values (?,?,?,?,?)";
			System.out.println("insertSql --" + insertSql);
			preparedStatement = conn.prepareStatement(insertSql);
			for (String row : rows) {
				String[] ss = row.split(",", -1);
				// TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFIC500,TOTALRESPTIME500
				preparedStatement.setTimestamp(1, new Timestamp(df.parse(ss[0]).getTime()));
				preparedStatement.setString(2, ss[1]);
				preparedStatement.setString(3, ss[2]);
				preparedStatement.setString(4, ss[3]);
				preparedStatement.setLong(5, Long.parseLong(ss[4]));
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			// insert into select
			String updateSql = "insert into HW4G_HOTAREA_USERNBR_15MIN  select mme.ttime,mme.city_name,mme.scene,mme.hotarea,mme.S1mmeUserNbr,http.HTTPUserNbr"
					+ " from USERNBR_S1MME_" + quarterFormat + " mme join USERNBR_HTTP_" + quarterFormat + " http "
					+ " on mme.ttime=http.ttime and mme.city_name=http.city_name and mme.scene=http.scene and mme.hotarea=http.hotarea ";
			System.out.println("updateSql --" + updateSql);
			stmt.executeUpdate(updateSql);
			// drop tmp table
			// String dropSql = "drop table USERNBR_S1MME_"+quarterFormat+";drop
			// table HOTAREA_USERNBR_HTTP_"+quarterFormat;
			String dropSql = "drop table USERNBR_S1MME_" + quarterFormat;

			stmt.executeUpdate(dropSql);
			dropSql = "drop table USERNBR_HTTP_" + quarterFormat;
			stmt.executeUpdate(dropSql);
			System.out.println("dropSql --" + dropSql);

			ConnectionUtil.commit();
			ConnectionUtil.close();

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				ConnectionUtil.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * save into TmpHttpUserNbr
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void saveResultIntoTmpSmmeUserNbr(List<String> rows, String quartertime) {

		Connection conn = ConnectionUtil.getConnection();
		PreparedStatement preparedStatement = null;
		Statement stmt = null;
		String quarterFormat = Utility.getQuarterFormat(quartertime);
		String createSql = "create table USERNBR_S1MME_" + quarterFormat
				+ " ( ttime timestamp(3),city_name varchar2(50), scene varchar2(50),hotarea varchar2(50), S1mmeUserNbr integer) ";
		System.out.println("createSql --" + createSql);
		try {
			conn.setAutoCommit(false);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			stmt = conn.createStatement();
			stmt.executeUpdate(createSql);
			String insertSql = "insert into USERNBR_S1MME_" + quarterFormat + " values (?,?,?,?,?)";
			System.out.println("insertSql --" + insertSql);
			preparedStatement = conn.prepareStatement(insertSql);
			for (String row : rows) {
				String[] ss = row.split(",", -1);
				// TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFIC500,TOTALRESPTIME500
				preparedStatement.setTimestamp(1, new Timestamp(df.parse(ss[0]).getTime()));
				preparedStatement.setString(2, ss[1]);
				preparedStatement.setString(3, ss[2]);
				preparedStatement.setString(4, ss[3]);
				preparedStatement.setLong(5, Long.parseLong(ss[4]));
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();

			ConnectionUtil.commit();
			ConnectionUtil.close();

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				ConnectionUtil.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void saveToOracleTable(DataFrame df, final String table) {
		final StructType structType = df.schema();
		df.foreachPartition(new ForeachPartitionImp() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 8795320811441887790L;

			@Override
			public void call(Iterator<Row> rows) {
				try {
					savePartition(table, rows, structType, 1000);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
			}

		});
	}

	public static void savePartition(String table, Iterator<Row> iterator, StructType rddSchema, int batchSize) {
		boolean committed = false;
		Connection conn;
		try {
			conn = ConnectionUtil.getConnection();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		// try {

		try {  
			conn.setAutoCommit(false);  
			PreparedStatement stmt = insertStatement(conn, table, rddSchema);
			if (stmt == null) {
				return;
			}
  
			int rowCount = 0;
			while (iterator.hasNext()) {
				Row row = iterator.next();
				int numFields = rddSchema.fields().length;
				int i = 0;
				while (i < numFields) {
					String dataType = rddSchema.fields()[i].dataType().typeName().toLowerCase();
					switch (dataType) {
					case "timestamp":
						stmt.setTimestamp(i + 1, row.getTimestamp(i));
						break;
					case "string":
						stmt.setString(i + 1, row.getString(i));
						break;
					case "long":
						stmt.setLong(i + 1, row.getLong(i));
						break;
					case "integer":
						stmt.setInt(i + 1, row.getInt(i));
						break;
					case "float":
						stmt.setFloat(i + 1, row.getFloat(i));
						break;
					case "double":  
						stmt.setDouble(i + 1, row.getDouble(i));
						break;	
					default:
						/// TODO:需要增加处理逻辑
						break;
					}
					i = i + 1;
				}
				stmt.addBatch();
				rowCount += 1;
				if (rowCount % batchSize == 0) {
					stmt.executeBatch();
					rowCount = 0;
					conn.commit();
				}
			}
			if (rowCount > 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {  
				System.out.println("hello \t"+e.getMessage());    
				e1.printStackTrace(); 
			}
			e.printStackTrace();
		} finally {
			// try {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// } catch (Exception ignored) {
			// }
		}
		// try {
		// conn.commit();
		// committed = true;
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// } catch (Exception e) {
		// e.printStackTrace();
		// } finally {
		// if (!committed) {
		// try {
		// conn.rollback();
		// conn.close();
		// }catch (Exception e){
		// e.printStackTrace();
		// }
		// }else{
		// try {
		// conn.close();
		// }catch (Exception e){
		// e.printStackTrace();
		// }
		// }
		// }
	}
  
	public static PreparedStatement insertStatement(Connection conn, String table, StructType rddSchema) {
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(table);
		sql.append("(");
		int fieldsLeft = rddSchema.fields().length;
		for (int i = 0; i < fieldsLeft; i++) {
			if (i == 0) {
				sql.append(rddSchema.fields()[i].name());
			} else {
				sql.append(",");
				sql.append(rddSchema.fields()[i].name());
			}
		}
		sql.append(") VALUES(");
		while (fieldsLeft > 0) {
			sql.append("?");
			if (fieldsLeft > 1)
				sql.append(", ");
			else
				sql.append(")");
			fieldsLeft = fieldsLeft - 1;
		}
		try {
			//System.out.println("sql.toString()\t" + sql.toString());  
			return conn.prepareStatement(sql.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * save into TmpSmmeUserCell
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void createTmpTable(String flag, String tmpTableName) {

		Connection conn = null;
		PreparedStatement preparedStatement = null;
		Statement stmt = null;
		String createSql = "";
		if(Constant.F_RY_HTTP_CELL_15M.equals(flag)){
			createSql = "create table " + tmpTableName
					+ " ( "
					+ "HOUR_ID number(19),"
					+ "procedure_starttime timestamp(3), "
					+ "interface varchar2(10),"
					+ "transaction_type smallint,"
					+ "App_Type number(10),"
					+ "App_Sub_type number(10),"
					+ "App_Content integer,"
					+ "USER_IPv4 varchar2(20),"
					+ "USER_IPv6 varchar2(50),"
					+ "USER_PORT integer,"
					+ "App_Server_IP_IPv4 varchar2(20),"
					+ "App_Server_IP_IPv6 varchar2(50),"
					+ "App_Server_Port integer,"
					+ "buss_Browser integer,"
					+ "Portal_App_set integer,"
					+ "adsl_account varchar2(100),"
					+ "BAS_IPv4 varchar2(20),"
					+ "BRAS_Name varchar2(100),"
					+ "HTTP_wap_affair_status smallint,"
					+ "HTTP_content_type varchar2(200),"
					+ "buss_Behavior_flag smallint,"
					+ "App_Status smallint,"
					+ "buss_finish_flag smallint,"
					+ "Content_Length integer,"
					+ "Response_Time integer,"
					+ "buss_Delay integer) ";
		}else if(Constant.F_RY_TCP_CELL_15M.equals(flag)){
			createSql = "create table " + tmpTableName
					+ " ( "
					+ "HOUR_ID number(19),"
					+ "procedure_starttime timestamp(3), "
					+ "INTERFACE varchar2(10),"
					+ "USER_IPv4 varchar2(20),"
					+ "USER_IPv6 varchar2(50),"
					+ "USER_PORT integer,"
					+ "App_Server_IP_IPv4 varchar2(20),"
					+ "App_Server_IP_IPv6 varchar2(50),"
					+ "App_Server_Port integer,"
					+ "adsl_account varchar2(50),"
					+ "BAS_IPv4 varchar2(20),"
					+ "BRAS_Name varchar2(100),"
					+ "TCP_link_Status smallint,"
					+ "tcp_createlink_trytimes integer,"
					+ "tcp_createlink_response_Delay integer,"
					+ "tcp_createlink_confirm_Delay integer,"
					+ "Success_Delay integer) ";
		}
		 
		System.out.println("createSql --" + createSql);
		try {
			conn = ConnectionUtil.getConnection();
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			stmt = conn.createStatement();
			stmt.executeUpdate(createSql);

			ConnectionUtil.commit();
			ConnectionUtil.close();

		}  catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				ConnectionUtil.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
