package com.kgal.bulkfileloader;

public class FileInventoryItem {
	private int fileNumber;
	private String sourceFilePath;
	private String contentVersionID;
	private String contentDocumentID;
	private String tempFilePath;
	private int batchNumber;
	private int numberInBatch;
	private String batchId;
	private boolean success;
	private String error;
	
	public String getBatchId() {
		return batchId;
	}

	public void setBatchId(String batchId) {
		this.batchId = batchId;
	}


	public int getFileNumber() {
		return fileNumber;
	}

	public void setFileNumber(int fileNumber) {
		this.fileNumber = fileNumber;
	}
	
	public String getSourceFilePath() {
		return sourceFilePath;
	}

	public void setSourceFilePath(String sourceFilePath) {
		this.sourceFilePath = sourceFilePath;
	}

	public String getContentVersionID() {
		return contentVersionID;
	}

	public void setContentVersionID(String contentVersionID) {
		this.contentVersionID = contentVersionID;
	}

	public String getContentDocumentID() {
		return contentDocumentID;
	}

	public void setContentDocumentID(String contentDocumentID) {
		this.contentDocumentID = contentDocumentID;
	}

	public String getTempFilePath() {
		return tempFilePath;
	}

	public void setTempFilePath(String tempFilePath) {
		this.tempFilePath = tempFilePath;
	}
	
	public int getBatchNumber() {
		return batchNumber;
	}

	public void setBatchNumber(int batchNumber) {
		this.batchNumber = batchNumber;
	}

	public int getNumberInBatch() {
		return numberInBatch;
	}

	public void setNumberInBatch(int numberInBatch) {
		this.numberInBatch = numberInBatch;
	}
	
	/*
	 * 
	 * returns an output line corresponding to the header
	 * ContentVersionID,PathOnClient,TempPath,ContentDocumentId,Success,Error
	 * 
	 */

	public String getOutputLine() {
		String line = contentVersionID + "," + sourceFilePath + "," + tempFilePath + "," + contentDocumentID + "," + (success ? "true" : "false") + "," + error;
		return line;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public void setError(String error) {
		this.error = error;
	}

}
