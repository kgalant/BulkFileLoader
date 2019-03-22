package com.kgal.bulkfileloader;

import java.util.concurrent.TimeUnit;

public class UploadResult {
	private int numberOfFilesUploaded;
	private int numberOfFilesTotal;
	private int numberOfFilesErrored;
	long elapsedTime;
	private long bytesUploaded;
	private long bytesTotal;
	private long bytesErrored;
	public int getNumberOfFilesUploaded() {
		return numberOfFilesUploaded;
	}
	public void setNumberOfFilesUploaded(int numberOfFilesUploaded) {
		this.numberOfFilesUploaded = numberOfFilesUploaded;
	}
	public int getNumberOfFilesTotal() {
		return numberOfFilesTotal;
	}
	public void setNumberOfFilesTotal(int numberOfFilesTotal) {
		this.numberOfFilesTotal = numberOfFilesTotal;
	}
	public int getNumberOfFilesErrored() {
		return numberOfFilesErrored;
	}
	public void setNumberOfFilesErrored(int numberOfFilesErrored) {
		this.numberOfFilesErrored = numberOfFilesErrored;
	}
	public long getTimeElapsed() {
		return elapsedTime;
	}
	public String getTimeElapsedHMS() {
		return String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(elapsedTime),
				TimeUnit.MILLISECONDS.toMinutes(elapsedTime) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(elapsedTime)),
				TimeUnit.MILLISECONDS.toSeconds(elapsedTime)
				- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsedTime)));			 
	}
	public void setTimeElapsed(long timeElapsed) {
		this.elapsedTime = timeElapsed;
	}
	public long getBytesUploaded() {
		return bytesUploaded;
	}
	public void setBytesUploaded(long bytesUploaded) {
		this.bytesUploaded = bytesUploaded;
	}
	public long getBytesTotal() {
		return bytesTotal;
	}
	public String getTotalBytesInMb() {
		return String.format("%.1f", ((float)bytesTotal)/1024/1024) + " Mb ";
	}
	public String getBytesUploadedInMb() {
		return String.format("%.1f", ((float)bytesUploaded)/1024/1024) + " Mb ";
	}
	public String getBytesFailedInMb() {
		return String.format("%.1f", ((float)bytesErrored)/1024/1024) + " Mb ";
	}
	public void setBytesTotal(long bytesTotal) {
		this.bytesTotal = bytesTotal;
	}
	public long getBytesErrored() {
		return bytesErrored;
	}
	public void setBytesErrored(long bytesErrored) {
		this.bytesErrored = bytesErrored;
	}
}
