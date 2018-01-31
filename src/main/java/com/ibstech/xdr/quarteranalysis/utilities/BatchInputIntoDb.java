package com.ibstech.xdr.quarteranalysis.utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

public class BatchInputIntoDb {
//	private static final String HOTAREA_APP_SQL = "INSERT INTO HW4G_HOTAREA_APP_15MIN(TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFIC500,TOTALRESPTIME500) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String HOTAREA_APP_SQL = "INSERT INTO HW4G_HOTAREA_APP_15MIN(TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALRESPTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFIC500,TOTALRESPTIME500) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//	private static final String HOTAREA_APP_SQL = "INSERT INTO HW4G_HOTAREA_CELL_15MIN(TTIME,CITY_NAME,SCENE,HOTAREA,APP_TYPE_NAME,APP_SUB_TYPE_NAME,HTTPUSERNBR,ATTNBR,SUCCNBR,TOTALACKDELAYTIME,ULTRAFFIC,DLTRAFFIC,DLTRAFFICHUNDRED,RESPTIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String URL = AppCfg.get("oracle_url");          
	private static final String USERNAME = AppCfg.get("oracle_user");
	private static final String PASSWORD = AppCfg.get("oracle_password");
	private static final String DRIVERNAME = "oracle.jdbc.driver.OracleDriver";      
	/**
	 * save into HW4G_HOTAREA_APP_15MIN
	 * 
	 * @param rows
	 * @throws Exception
	 */
	public static void saveResultToAreaApp(List<String> rows) throws Exception {
		                             
		Class.forName(DRIVERNAME);
		Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		conn.setAutoCommit(false);  
               
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		PreparedStatement preparedStatement = conn.prepareStatement(HOTAREA_APP_SQL);
		System.out.println("-----------------world------------------"+rows.size());
		for (String row : rows) {
			//System.out.println("row --------"+row);
			String[] ss = row.split(",", -1);
			try {  
				preparedStatement.setTimestamp(1, new Timestamp(df.parse(ss[0]).getTime()));
				preparedStatement.setString(2, ss[1]);
				preparedStatement.setString(3, ss[2]);
				preparedStatement.setString(4, ss[3]);
				preparedStatement.setString(5, ss[4]);
				preparedStatement.setString(6, ss[5]);
				preparedStatement.setLong(7, Long.parseLong(ss[6]));
				preparedStatement.setLong(8, Long.parseLong(ss[7]));
				preparedStatement.setLong(9, Long.parseLong(ss[8]));
				preparedStatement.setLong(10, Long.parseLong(ss[9]));
				preparedStatement.setLong(11, Long.parseLong(ss[10]));
				preparedStatement.setLong(12, Long.parseLong(ss[11]));
				preparedStatement.setLong(13, Long.parseLong(ss[12]));
				preparedStatement.setLong(14, Long.parseLong(ss[13]));
				preparedStatement.addBatch();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("---world---"+e.getMessage());
			}    
		}      
		System.out.println("-----------------world------------------");
		preparedStatement.executeBatch();    
		conn.commit();        
		conn.close();      
	}
	
}
