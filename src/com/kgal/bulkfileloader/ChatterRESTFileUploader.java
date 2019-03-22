package com.kgal.bulkfileloader;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.FormBodyPartBuilder;
import org.apache.http.entity.mime.MIME;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.sforce.soap.partner.PartnerConnection;

public class ChatterRESTFileUploader {

	private List<FileInventoryItem> failedItems;
	private PartnerConnection connection;
	private boolean isRunning = false;
	final String service = "/services/data/v45.0/connect/files/users/me";
	private String endpoint;
	private HttpClient httpclient = null;
	private File successFolder;
	private Logger logger;
	
	public ChatterRESTFileUploader (List<FileInventoryItem> failedItems, File successFolder, PartnerConnection c, Logger l) {
		this.failedItems = failedItems;
		this.connection = c;
		this.successFolder = successFolder;
		this.logger = l;
		setupConnectionDetails();
	}
	
	private HttpResponse chatterRESTUploadFile(File f) {
		HttpPost httpPost = new HttpPost(endpoint);
//		HttpPost httpPost = new HttpPost("https://www.putsreq.com/6QxzX6djtIehf2jM4pNt");
		HttpResponse response = null;

		String boundaryString = "---------------------asfkjhsdgfaksdgfk182763";
		httpPost.addHeader("Authorization", "OAuth " + connection.getSessionHeader().getSessionId());
		httpPost.addHeader("Content-Type", "multipart/form-data; boundary=" + boundaryString);

		String jsonString = new JSONObject()
				.put("title", f.getName())
				.toString();

		StringBody content = new StringBody(jsonString, ContentType.APPLICATION_JSON);
		FileBody uploadFilePart = new FileBody(f);
		String contentDisposition = "form-data; name=\"fileData\"; filename=\"" + f.getName() + "\"";

		FormBodyPartBuilder partBuilder = FormBodyPartBuilder.create("file", uploadFilePart);
		partBuilder.setField(MIME.CONTENT_DISPOSITION, contentDisposition);

		FormBodyPart fbp = partBuilder.build();
		MultipartEntityBuilder reqEntity = MultipartEntityBuilder.create();
		reqEntity.addPart("json", content);
		reqEntity.addPart(fbp);
		reqEntity.setBoundary(boundaryString);
		httpPost.setEntity(reqEntity.build());

		try {
			// Execute the login POST request
			logger.log(Level.FINE, "Trying to send file " + f.getAbsolutePath() + " to Salesforce");
			response = httpclient.execute(httpPost);
		} catch (ClientProtocolException cpException) {
			cpException.printStackTrace();
		} catch (IOException ioException) {
			ioException.printStackTrace();
		}
		return response;
		
	}

	private String getContentDocumentIDFromRestResponse(HttpResponse response) {
		String getResult = null;
		JSONObject jsonObject = null;
		try {
			getResult = EntityUtils.toString(response.getEntity());
			jsonObject = (JSONObject) new JSONTokener(getResult).nextValue();
			String contentDocumentID = jsonObject.getString("id");
			logger.log(Level.FINE, "Retrieved ID from Salesforce response: " + contentDocumentID);
			return contentDocumentID;
		} catch (IOException ioException) {
			logger.log(Level.INFO, "Cannot retrieve ContentDocumentID from HTTP response. Exception: " + ioException.getMessage());
		}
		return null;
	}

	public boolean isRunning() {
		return isRunning;
	}

	public UploadResult run() throws IOException {
		isRunning = true;
		
		UploadResult result = new UploadResult();
		
		long totalFileSize = 0;
		long totalFileSizeProcessed = 0;
		long totalFileSizeErrored = 0;
		int totalFileCount = failedItems.size();
		int totalFileCountProcessed = 0;
		int totalFileCountErrors = 0;
		int fileCount = 0;
		long startTime = System.currentTimeMillis();
		
		// first, tally the total size
		
		for (FileInventoryItem fii : failedItems) {
			totalFileSize += fii.getSize();
		}
		
		
		for (FileInventoryItem fii : failedItems) {
			fileCount++;
			File fileToUpload = new File(fii.getTempFilePath());
			if (fii.getSize() == 0) {
				logger.log(Level.FINE, "File " + fii.getSourceFilePath() + " is size 0 - cannot be uploaded to Salesforce. Skipping");
				fii.setSuccess(false);
				fii.setError("Size 0");
				totalFileCountErrors++;
				totalFileCountProcessed++;
				continue;
			}
			logger.log(Level.INFO, "Uploading file " + fii.getSourceFilePath() + " to Salesforce using REST API.");
			HttpResponse response = chatterRESTUploadFile(fileToUpload);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK &&
					response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED	) {
				logger.log(Level.INFO, "Uploading file " + fii.getSourceFilePath() + " to Salesforce failed.");
				fii.setError("Error uploading through Chatter REST API");
				fii.setSuccess(false);
				totalFileCountErrors++;
				totalFileCountProcessed++;
				totalFileSizeErrored += fii.getSize();
				totalFileSizeProcessed += fii.getSize();
				continue;
			} else {
				// move item to success single file folder
				
				File successFolderFile = BulkFileLoader.getSafeFilename(new File(successFolder.getAbsolutePath() + File.separator + fileToUpload.getName()));
				String contentDocumentID = getContentDocumentIDFromRestResponse(response);
				if (contentDocumentID == null) {
					logger.log(Level.INFO, "Error fetching contentDocumentID for file: " + fii.getSourceFilePath() + " File might have been uploaded, please check before retrying");
					fii.setSuccess(false);
					fii.setError("Uploaded, but unable to retrieve contentDocumentID from response.");
					totalFileCountErrors++;
					totalFileCountProcessed++;
					totalFileSizeErrored += fii.getSize();
					totalFileSizeProcessed += fii.getSize();
				} else {
				FileUtils.moveFile(fileToUpload, successFolderFile);
				fii.setSuccess(true);
				fii.setTempFilePath(successFolderFile.getAbsolutePath());
				fii.setContentDocumentID(contentDocumentID);
				totalFileCountProcessed++;
				totalFileSizeProcessed += fii.getSize();
				}
			}
			long elapsedTime = System.currentTimeMillis() - startTime;
			double millisPerByte = ((double)totalFileSizeProcessed) / elapsedTime;
			double bytesRemaining = totalFileSize - totalFileSizeProcessed;
			long millisRemaining = (long)(bytesRemaining / millisPerByte); 
			final String hmsElapsed = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(elapsedTime),
					TimeUnit.MILLISECONDS.toMinutes(elapsedTime) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(elapsedTime)),
					TimeUnit.MILLISECONDS.toSeconds(elapsedTime)
					- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsedTime)));			 
			final String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millisRemaining),
					TimeUnit.MILLISECONDS.toMinutes(millisRemaining) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millisRemaining)),
					TimeUnit.MILLISECONDS.toSeconds(millisRemaining)
					- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millisRemaining)));
			logger.log(Level.INFO, "Status: files: " + fileCount + " / " + totalFileCount + " (" + String.format("%.2f", (float) (((float)fileCount*100)/totalFileCount)) + "%) "
					+ " data: " + String.format("%.1f", ((float)totalFileSizeProcessed)/1024/1024) + " / " + String.format("%.1f", ((float)totalFileSize)/1024/1024) + " Mb " 
					+ " (" + String.format("%.2f",  ((((float)totalFileSizeProcessed/1024/1024)*100)/((float)totalFileSize/1024/1024))) + "%)"
					+ " Elapsed: " + hmsElapsed + " ETC: " + hms);
		}
		result.setTimeElapsed(System.currentTimeMillis() - startTime);
		result.setBytesTotal(totalFileSize);
		result.setBytesUploaded(totalFileSizeProcessed);
		result.setBytesErrored(totalFileSizeErrored);
		result.setNumberOfFilesErrored(totalFileCountErrors);
		result.setNumberOfFilesTotal(totalFileCount);
		result.setNumberOfFilesUploaded(totalFileCountProcessed - totalFileCountErrors);	
		
		isRunning = false;
		return result;
	}

	private void setupConnectionDetails() {
		endpoint = connection.getConfig().getServiceEndpoint().substring(0,connection.getConfig().getServiceEndpoint().indexOf("/services")) + service;
		httpclient = HttpClientBuilder.create().build();
	}
}
