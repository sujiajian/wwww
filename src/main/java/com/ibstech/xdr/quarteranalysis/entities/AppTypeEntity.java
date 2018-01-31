package com.ibstech.xdr.quarteranalysis.entities;

import java.io.Serializable;

public class AppTypeEntity implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7332043767270814134L;
	private int appType;
	private int appSubType;
	private String appTypeName;
	private String appSubTypeName;
	
	
	public int getAppType() {
		return appType;
	}
	public void setAppType(int appType) {
		this.appType = appType;
	}
	public int getAppSubType() {
		return appSubType;
	}
	public void setAppSubType(int appSubType) {
		this.appSubType = appSubType;
	}
	public String getAppTypeName() {
		return appTypeName;
	}
	public void setAppTypeName(String appTypeName) {
		this.appTypeName = appTypeName;
	}
	public String getAppSubTypeName() {
		return appSubTypeName;
	}
	public void setAppSubTypeName(String appSubTypeName) {
		this.appSubTypeName = appSubTypeName;
	}
	
	
}
