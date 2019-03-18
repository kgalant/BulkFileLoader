package com.kgal.SFLogin;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.LoginResult;

import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class LoginUtil {

	private static final String APIVERSION = "45.0"; 
	private static PartnerConnection myPartnerConnection;

	public static PartnerConnection soapLogin(String url, String user, String pwd) {

		if (myPartnerConnection == null) {

			try {
				ConnectorConfig config = new ConnectorConfig();
				config.setUsername(user);
				config.setPassword(pwd);

				//System.out.println("AuthEndPoint: " + url);
				config.setAuthEndpoint(url);

				myPartnerConnection = new PartnerConnection(config);

			} catch (ConnectionException ce) {
				ce.printStackTrace();
			} 
		}
		return myPartnerConnection;
	}

	private static LoginResult loginToSalesforce(
			final String username,
			final String password,
			final String loginUrl) throws ConnectionException {
		final ConnectorConfig config = new ConnectorConfig();
		config.setAuthEndpoint(loginUrl);
		config.setServiceEndpoint(loginUrl);
		config.setManualLogin(true);
		return (new PartnerConnection(config)).login(username, password);
	}

	/**
	 * Create the BulkConnection used to call Bulk API operations.
	 */
	public static BulkConnection getBulkConnection(String url, String user, String pwd, String api)
			throws ConnectionException, AsyncApiException {
		ConnectorConfig partnerConfig = new ConnectorConfig();
		partnerConfig.setUsername(user);
		partnerConfig.setPassword(pwd);
		partnerConfig.setAuthEndpoint(url);
		//System.out.println("AuthEndPoint: " + url);
		// Creating the connection automatically handles login and stores
		// the session in partnerConfig
		myPartnerConnection = new PartnerConnection(partnerConfig);
		// When PartnerConnection is instantiated, a login is implicitly
		// executed and, if successful,
		// a valid session is stored in the ConnectorConfig instance.
		// Use this key to initialize a BulkConnection:
		ConnectorConfig config = new ConnectorConfig();
		config.setSessionId(partnerConfig.getSessionId());
		// The endpoint for the Bulk API service is the same as for the normal
		// SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
		String soapEndpoint = partnerConfig.getServiceEndpoint();
		String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
				+ "async/" + api;
		config.setRestEndpoint(restEndpoint);
		// This should only be false when doing debugging.
		config.setCompression(true);
		// Set this to true to see HTTP requests and responses on stdout
		config.setTraceMessage(false);
		BulkConnection connection = new BulkConnection(config);
		return connection;
	}

}

