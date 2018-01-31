package com.ibstech.xdr.quarteranalysis.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ibstech.xdr.quarteranalysis.entities.AccountMatchEntity;
import com.ibstech.xdr.quarteranalysis.entities.AppTypeEntity;
import com.ibstech.xdr.quarteranalysis.entities.FlowDirectEntity;
import com.ibstech.xdr.quarteranalysis.entities.HttpEntity;

public class AppCfg {
	private static Properties prop;

	public static String get(String key) {
		return changeCode(prop.getProperty(key));
	}

	static {
		prop = new Properties();
		try {
			InputStream inputStream = AppCfg.class.getResourceAsStream("/app.ini");
			prop.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String changeCode(String value) {
		if (value == null)
			return null;
		try {
			return new String(value.getBytes("ISO8859-1"), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	static String getAppConfigPath() {
		URL url = AppCfg.class.getProtectionDomain().getCodeSource().getLocation();
		String filePath = null;
		try {
			filePath = URLDecoder.decode(url.getPath(), "utf-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (filePath.endsWith(".jar")) {
			filePath = filePath.substring(0, filePath.lastIndexOf(File.separator) + 1);
		}

		File file = new File(filePath);
		filePath = file.getAbsolutePath() + File.separator + "app.ini";
		return filePath;
	}

	public static HashMap<String, String> getAppTypeMap(List<AppTypeEntity> collectAppType, String field) {
		HashMap<String, String> result = new HashMap<String, String>();
		for (AppTypeEntity hcp : collectAppType) {
			if (Constant.APP_TYPE.equals(field)) {
				result.put(Integer.toString(hcp.getAppType()), hcp.getAppTypeName());
			} else if (Constant.APP_SUB_TYPE.equals(field)) {
				result.put(Integer.toString(hcp.getAppSubType()), hcp.getAppSubTypeName());
			}
		}
		return result;
	}

	public static HashMap<String, String> getAccountMatchMap(List<AccountMatchEntity> collectAppType, String field) {
		HashMap<String, String> result = new HashMap<String, String>();

		for (AccountMatchEntity hcp : collectAppType) {
			if (Constant.BRAS_NAME.equals(field)) {
				result.put(hcp.getBRAS_IP(), hcp.getBRAS_NAME());
			} else if (Constant.OLT_NAME.equals(field)) {
				// String key = String.format("%s%s", hcp.getADSL_ACCOUNT(),Constant.GD_139);
				result.put(hcp.getADSL_ACCOUNT(), hcp.getOLT_NAME());
			} else if (Constant.BRANCH.equals(field)) {
				// String key = String.format("%s%s", hcp.getADSL_ACCOUNT(),Constant.GD_139);
				result.put(hcp.getADSL_ACCOUNT(), hcp.getOLT_NAME().substring(2, 4));
			} else if (Constant.ONU_NAME.equals(field)) {
				// String key = String.format("%s%s", hcp.getADSL_ACCOUNT(),Constant.GD_139);
				result.put(hcp.getADSL_ACCOUNT(), hcp.getONU_NAME());
			}

		}
		return result;
	}

	public static HashMap<String, String> getUserOnlineTime(List<HttpEntity> collectAppType) {
		HashMap<String, String> result = new HashMap<String, String>();
		for (HttpEntity hcp : collectAppType) {
			if (result.get(hcp.getADSL_ACCOUNT()) != null) {
				result.put(hcp.getADSL_ACCOUNT(), String.format("%s%s", "",
						(Long.parseLong(result.get(hcp.getADSL_ACCOUNT())) + hcp.getONLINE_TIME())));
			} else {
				result.put(hcp.getADSL_ACCOUNT(), String.format("%s%s", "", hcp.getONLINE_TIME()));
			}
		}
		return result;
	}

	public static void main(String[] args) {
		// HashMap<String, String> map = getCellParameterMap(Constant.CELL_NAME);
		//
		// for (Map.Entry<String, String> entry : map.entrySet()) {
		// System.out.println("key= " + entry.getKey() + " and value= " +
		// entry.getValue());
		// }

		String str = "lte_gndns_2_20160816130839340_0614.tar.gz";
		int index = str.indexOf(Constant.STAR);
		int index2 = str.indexOf(Constant.LEFT_BRACKET);
		int index3 = str.indexOf(Constant.RIGHT_BRACKET);
		// System.out.println(str.substring(index+1, index3+1));
		System.out.println(str.matches("lte_gndns[_]{0,1}[0-9]{0,1}_20160816130[0-9]{1}[\\s\\S]*gz"));

		String quarter = "2016-08-24 12:30:00.000";
		System.out.println(quarter.substring(8, 16).replaceAll(" ", "").replaceAll(":", ""));
	}

}
