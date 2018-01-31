package com.ibstech.xdr.quarteranalysis.entities;

import java.io.Serializable;

public class AccountMatchEntity implements Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7540442732579461459L;
	private String ADSL_ACCOUNT;
	private String BRAS_IP;
	private String BRAS_NAME;
	private String OLT_NAME;
	private String ONU_NAME;
	public String getADSL_ACCOUNT() {
		return ADSL_ACCOUNT;
	}
	public void setADSL_ACCOUNT(String aDSL_ACCOUNT) {
		ADSL_ACCOUNT = aDSL_ACCOUNT;
	}
	public String getBRAS_IP() {
		return BRAS_IP;
	}
	public void setBRAS_IP(String bRAS_IP) {
		BRAS_IP = bRAS_IP;
	}
	public String getBRAS_NAME() {
		return BRAS_NAME;
	}
	public void setBRAS_NAME(String bRAS_NAME) {
		BRAS_NAME = bRAS_NAME;
	}
	public String getOLT_NAME() {
		return OLT_NAME;
	}
	public void setOLT_NAME(String oLT_NAME) {
		OLT_NAME = oLT_NAME;
	}
	public String getONU_NAME() {
		return ONU_NAME;
	}
	public void setONU_NAME(String oNU_NAME) {
		ONU_NAME = oNU_NAME;
	}
	
	
}
