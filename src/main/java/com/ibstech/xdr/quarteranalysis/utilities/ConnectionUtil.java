package com.ibstech.xdr.quarteranalysis.utilities;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
public class ConnectionUtil {
	private static ThreadLocal<Connection> local = new ThreadLocal<Connection>();  
	 
	private static final String URL = AppCfg.get("oracle_url");
	private static final String USERNAME = AppCfg.get("oracle_user");
	private static final String PASSWORD = AppCfg.get("oracle_password");
	private static final String DRIVERNAME = AppCfg.get("drive_name");
//    public static void beginTransaction() throws SQLException {  
//        Connection conn = getConnection();  
//        conn.setAutoCommit(false);   
//    }  
    
    // »Ø¹öÊÂÎñ           
    public static void rollback() throws SQLException {  
        Connection conn = local.get();  
        if (conn != null) {  
            conn.rollback();  
            conn.close();  
            local.remove();  
        }  
    }  
       
    // commit transaction 
    public static void commit() throws SQLException {  
        Connection conn = local.get();  
        if (conn != null) {  
            conn.commit();  
            // clear threadLocal  
            local.remove();  
        }  
    }  
      
    public static void close() throws SQLException {  
    	Connection conn = local.get();  
        if(conn != null){  
            try{  
                conn.close();  
                //remove connection from threadlocal's set after connection is closed
                local.remove();  
            }catch(SQLException e){  
                e.printStackTrace();  
            }  
        }  
    }  
  
    public static Connection getConnection() {  
    	Connection conn = local.get(); 
    	if(conn == null){  
            try {  
				Class.forName(DRIVERNAME);
                conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);  
                //save current thread's connection to threadlocal  
                local.set(conn);  
            }catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
        return conn;  
    }  
}
