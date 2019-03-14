package com.kgal.bulkfileloader;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import com.kgal.SFLogin.LoginUtil;
import com.kgal.bulkfileloader.BulkFileLoaderCommandLine;
import com.kgal.bulkfileloader.Utils;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;


public class BulkFileLoader {

	public enum Loglevel {
		VERBOSE(2), NORMAL(1), BRIEF(0);
		private final int level;

		Loglevel(final int level) {
			this.level = level;
		}

		int getLevel() {
			return this.level;
		}

	}
	public static final int     MAXREQUESTSIZE      = 20920000;;


	public static final int     MAXZIPPEDBATCHSIZE      = 10000000;

	public static final double   API_VERSION            = 45.0;
	private static final String  URLBASE                = "/services/Soap/u/";
	private static final String  BATCHFOLDERPREFIX		= "Batch_";
	private static final long MAXSINGLEFILELENGTH = 10485760;
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	private Loglevel          loglevel;

	private double                                  myApiVersion;
	private final Map<String, String> parameters    = new HashMap<>();

	private final Map<String, BatchInfo> batchMap    = new HashMap<>();
	private int maxRequestSize;
	private BulkConnection                      	bulkConnection;

	private PartnerConnection 					partnerConnection = null;
	private String srcFolder;
	private String tmpFolder;
	private boolean failsReprocessed = false;
	private Integer tempFolderCounter = 1;
	private Integer tooBigFolderCounter = 1;
	private int totalFiles = 0;
	private int totalFilesSent = 0;
	private double averageCompressionRatio = 0.75;
	private double compressedSize = 0;
	private double uncompressedSize = 0;
	private File failedFolder;
	private int totalSuccesses = 0;
	private int totalFails = 0;
	private int totalProcessed = 0;
	private int totalToProcess = 0;
	private long totalProcessedBytes = 0;

	private long totalToProcessBytes = 0;
	private Map<Integer, FileInventoryItem> fileInventoryByNumber = new HashMap<Integer, FileInventoryItem>();
	private Map<String, FileInventoryItem> fileInventoryBySourceName = new HashMap<String, FileInventoryItem>();
	private Map<String, FileInventoryItem> fileInventoryByTempName = new HashMap<String, FileInventoryItem>();
	private final Map<String, ArrayList<FileInventoryItem>> batchInventoryMapByBatchId    = new HashMap<String, ArrayList<FileInventoryItem>>();
	private List<BatchBin> batchBins = new ArrayList<BatchBin>();

	private final List<FileInventoryItem> completeFileList = new ArrayList<>();

	private long startTime;

	public BulkFileLoader(final Map<String, String> parameters) {
		this.parameters.putAll(parameters);

	}

	/**
	 * Wait for a job to complete by polling the Bulk API.
	 * 
	 * @param connection
	 *            BulkConnection used to check results.
	 * @param job
	 *            The job awaiting completion.
	 * @param batchInfoList
	 *            List of batches for this job.
	 * @throws AsyncApiException
	 * @throws IOException 
	 * @throws ConnectionException 
	 */
	private void awaitCompletion(BulkConnection connection, JobInfo job, ArrayList<BatchInfo> batchInfoList) throws AsyncApiException, IOException, ConnectionException {
		long sleepTime = 0L;
		HashSet<String> incomplete = new HashSet<String>();
		for (BatchInfo bi : batchInfoList) {
			incomplete.add(bi.getId());
		}
		while (!incomplete.isEmpty()) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {}
			log("Waiting on results for " + incomplete.size() + " remaining batches.", Loglevel.BRIEF);
			sleepTime = 10000L;
			BatchInfo[] statusList =
					connection.getBatchInfoList(job.getId()).getBatchInfo();
			for (BatchInfo b : statusList) {
				if (b.getState() == BatchStateEnum.Completed
						|| b.getState() == BatchStateEnum.Failed) {
					if (incomplete.remove(b.getId())) {
						CSVReader rdr =	new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
						ArrayList<String> resultHeader = rdr.nextRecord();
						int resultCols = resultHeader.size();
						List<FileInventoryItem> batchFileList = batchInventoryMapByBatchId.get(b.getId());
						Iterator<FileInventoryItem> fiiIterator = batchFileList.iterator();

						ArrayList<String> row;
						int fails = 0;
						int successes = 0;
						while ((row = rdr.nextRecord()) != null) {
							totalProcessed++;
							FileInventoryItem fii = null;
							if (fiiIterator.hasNext()) {
								fii = fiiIterator.next();
							}
							if (fii == null) {
								log("Something's wrong. No more files left in inventory list, but more result rows remain.", Loglevel.NORMAL);
							}
							Map<String, String> resultInfo = new HashMap<String, String>();
							for (int i = 0; i < resultCols; i++) {
								resultInfo.put(resultHeader.get(i), row.get(i));
							}
							boolean success = Boolean.valueOf(resultInfo.get("Success"));
							boolean created = Boolean.valueOf(resultInfo.get("Created"));

							String contentVersionId = resultInfo.get("Id");
							String error = resultInfo.get("Error");
							if (!fii.getBatchId().equals(b.getId())) {
								log("Something's wrong. BatchID and the file's batch ID recorded in inventory don't match.", Loglevel.NORMAL);
							} else {
								fii.setContentVersionID(contentVersionId);
								fii.setSuccess(success);
								fii.setError(error);
							}

							if (success && created) {
								//					System.out.println("Created row with id " + id);
								successes++;
								totalSuccesses++;
							} else if (!success) {
								System.out.println("Failed with error: " + error);
								fails++;
								totalFails++;
							}
						}
						log("Batch: " + b.getId() + " Successes: " + successes + " Fails: " + fails + " API time: " + b.getApiActiveProcessingTime(), Loglevel.BRIEF);
						getContentDocumentIDsForBatch(b);

						//						System.out.println("BATCH STATUS:\n" + b);
						log("Batch: " + b.getId() + " Successes: " + (b.getNumberRecordsProcessed() - b.getNumberRecordsFailed()) + 
								" Fails: " + b.getNumberRecordsFailed() + " API time: " + b.getApiActiveProcessingTime(), Loglevel.NORMAL);
					}
				}
			}
		}
	}

	/**
	 * Gets the results of the operation and checks for errors.
	 * @throws ConnectionException 
	 */
	private void checkResults(BulkConnection connection, JobInfo job, ArrayList<BatchInfo> batchInfoList) throws AsyncApiException, IOException, ConnectionException {
		// batchInfoList was populated when batches were created and submitted

		log("******************************************************", Loglevel.BRIEF);
		log("*       Job Results                          *", Loglevel.BRIEF);
		log("******************************************************", Loglevel.BRIEF);
		log("Total processed: " + totalProcessed + " Total successes: " + totalSuccesses + " Fails: " + totalFails, Loglevel.BRIEF);
	}

	private void closeJob(BulkConnection connection, JobInfo myJob)	throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setId(myJob.getId());
		job.setState(JobStateEnum.Closed);
		connection.updateJob(job);
	}

	private ArrayList<BatchInfo> createBatchesFromBins(JobInfo myJob, String sourceDirectory, String batchPrefix, boolean moveFiles, int maxNumberOfFilesInBatch, int currentMaxRequestSize) throws IOException {
		ArrayList<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
		File tempFolder = null;
		Integer numFiles = 0;
		// initialize first temp folder, variables
		final String headerRow = "Title,Description,VersionData,PathOnClient";

		for (BatchBin b : batchBins) {
			// reinitialize temp folder, variables
			numFiles = 0;
			int bytesLeft = currentMaxRequestSize;
			ArrayList<String> requesttxt = new ArrayList<String>();
			ArrayList<FileInventoryItem> filesInThisBatch = new ArrayList<FileInventoryItem>();
			requesttxt.add(headerRow);
			bytesLeft -= (headerRow.length() + 2); // adding space for CRLF 
			tempFolder = new File(tmpFolder + File.separator + batchPrefix + tempFolderCounter);
			FileUtils.deleteDirectory(tempFolder);
			tempFolder.mkdirs();
			log("Created batch folder: " + tempFolder.getAbsolutePath(), Loglevel.NORMAL);
			for (FileInventoryItem fii : b.fileList) {
				File f = new File(fii.getSourceFilePath());

				if (f.length() > currentMaxRequestSize || f.length() == 0 || f.length() > MAXSINGLEFILELENGTH) {
					// file too big altogether, stop processing
					File failedFile = new File(failedFolder.toString() + File.separator + f.getName());
					if (this.parameters.containsKey(BulkFileLoaderCommandLine.MOVEFILES_LONGNAME)) {
						FileUtils.moveFile(f, failedFile);
					} else {
						FileUtils.copyFile(f, failedFile);				
					}		
					log("File " + f.getAbsolutePath() + " (size " + f.length() + " bytes) is too big or has size 0, cannot continue. Moving to failed: " + failedFile.getAbsolutePath(), Loglevel.BRIEF);
					continue;
				}
				if (f == null || !f.isFile() || f.getName().startsWith(".")) { 
					continue;
				}
				// move this file into the temp directory, but check if we have something of the same name there alredy,
				// if we do, rename

				File targetFile = getSafeFilename(new File(tempFolder + File.separator + f.getName()));
				if (this.parameters.containsKey(BulkFileLoaderCommandLine.MOVEFILES_LONGNAME)) {
					FileUtils.moveFile(f, targetFile);
				} else {
					FileUtils.copyFile(f, targetFile);				
				}		
				fileInventoryByTempName.put(targetFile.getAbsolutePath(), fii);
				fii.setTempFilePath(targetFile.getAbsolutePath());
				fii.setBatchNumber(tempFolderCounter);
				fii.setNumberInBatch(numFiles++);
				filesInThisBatch.add(fii);

				// deduct size of this file to keep track of what we have left

				bytesLeft -= targetFile.length();
				// add to requesttxt
				String requestLine = 
						targetFile.getName().trim() + "," + 			// Title
								targetFile.getName().trim() + "," + 			// Description
								"#" + targetFile.getName().trim() + "," + 			// VersionData
								targetFile.getAbsolutePath();			// PathOnClient
				requesttxt.add(requestLine);
				bytesLeft -= (requestLine.length() + 2);	
				//				log("Batch: " + tempFolderCounter + " adding file: " + targetFile.getName(), Loglevel.NORMAL);
			}
			// write request.txt into temp folder
			try {
				if (numFiles > 0) {
					String requestFilename = tempFolder + File.separator + "request.txt";	
					Utils.writeFile(requestFilename, requesttxt);
					finishABatch(myJob,tempFolder, batchPrefix, tempFolderCounter, currentMaxRequestSize, batchInfos, filesInThisBatch, moveFiles, maxNumberOfFilesInBatch);
					tempFolderCounter++;
				}
			} catch (AsyncApiException e) {
				log("Tried to upload batch of size: " + (maxRequestSize - bytesLeft) + " files: " + numFiles + "but failed.", Loglevel.NORMAL);
				log(e.getMessage(), Loglevel.NORMAL);
				System.out.println(e.getCause());
				System.out.println(e.getExceptionCode());
				System.out.println(e.getStackTrace());
				tempFolder.renameTo(new File(tempFolder.getAbsolutePath() + "_failed"));
			}

		}
		return batchInfos;
	}

	private ArrayList<BatchInfo> createBatchesFromDirectory(JobInfo myJob, String sourceDirectory, String batchPrefix, boolean moveFiles, int maxNumberOfFilesInBatch, int currentMaxRequestSize) throws IOException {
		//// open directory
		//// iterate over files until we hit capacity limit, build request.txt as we go

		File myDirectory = new File(sourceDirectory);
		//		ArrayList<File> myFiles = new ArrayList<File>(Arrays.asList(myDirectory.listFiles()));
		Collection<File> myFiles = FileUtils.listFiles(myDirectory, null, true);

		ArrayList<BatchInfo> batchInfos = new ArrayList<BatchInfo>();

		ArrayList<String> failedBatchFolderNames = new ArrayList<String>();

		Iterator<File> i = myFiles.iterator();

		File tempFolder = null;
		Integer numFiles = 0;



		// initialize first temp folder, variables
		final String headerRow = "Title,Description,VersionData,PathOnClient";

		// reinitialize temp folder, variables
		numFiles = 0;
		int bytesLeft = currentMaxRequestSize;

		ArrayList<String> requesttxt = new ArrayList<String>();
		ArrayList<FileInventoryItem> filesInThisBatch = new ArrayList<FileInventoryItem>();
		requesttxt.add(headerRow);
		bytesLeft -= (headerRow.length() + 2); // adding space for CRLF 
		tempFolder = new File(tmpFolder + File.separator + batchPrefix + tempFolderCounter);
		FileUtils.deleteDirectory(tempFolder);
		tempFolder.mkdirs();
		log("Created batch folder: " + tempFolder.getAbsolutePath(), Loglevel.NORMAL);

		// set up where fails go


		while (i.hasNext()) {
			File f = i.next();

			// locate it in inventory
			FileInventoryItem fii = fileInventoryBySourceName.get(f.getAbsolutePath());
			if (fii == null) {
				fii = fileInventoryByTempName.get(f.getAbsolutePath());
			}

			if (fii == null) {
				File failedFile = new File(failedFolder.toString() + File.separator + f.getName());
				f.renameTo(failedFile);
				log("File " + f.getAbsolutePath() + " not found in inventory - something is wrong. Moving to failed: " + failedFile.getAbsolutePath(), Loglevel.BRIEF);
				continue;
			}

			if (f.length() > currentMaxRequestSize || f.length() == 0 || f.length() > MAXSINGLEFILELENGTH) {
				// file too big altogether, stop processing
				File failedFile = new File(failedFolder.toString() + File.separator + f.getName());
				f.renameTo(failedFile);
				log("File " + f.getAbsolutePath() + " (size " + f.length() + " bytes) is too big or has size 0, cannot continue. Moving to failed: " + failedFile.getAbsolutePath(), Loglevel.BRIEF);
				continue;
			} else if (f.length() * getCompressionRatio() > bytesLeft || numFiles > maxNumberOfFilesInBatch){
				// finish processing this batch
				// write request.txt into temp folder
				String requestFilename = tempFolder + File.separator + "request.txt";	
				Utils.writeFile(requestFilename, requesttxt);
				// now create batch out of temp folder

				try {
					finishABatch(myJob,tempFolder, batchPrefix, tempFolderCounter, currentMaxRequestSize, batchInfos, filesInThisBatch, moveFiles, maxNumberOfFilesInBatch);
				} catch (AsyncApiException e) {
					log("Tried to upload batch of size: " + (maxRequestSize - bytesLeft) + " files: " + numFiles + "but failed.", Loglevel.NORMAL);
					log(e.getMessage(), Loglevel.NORMAL);
					System.out.println(e.getCause());
					System.out.println(e.getExceptionCode());
					System.out.println(e.getStackTrace());
					tempFolder.renameTo(new File(tempFolder.getAbsolutePath() + "_failed"));
					failedBatchFolderNames.add(tempFolder.getAbsolutePath() + "_failed");
				}

				// now reinitialize everything for next batch

				numFiles = 0;
				bytesLeft = maxRequestSize;

				requesttxt.clear();
				requesttxt.add(headerRow);
				filesInThisBatch = new ArrayList<>();
				bytesLeft -= (headerRow.length() + 2); // adding space for CRLF 
				tempFolder = new File(tmpFolder + File.separator + batchPrefix + ++tempFolderCounter);
				tempFolder.mkdirs();
				log("Created batch folder: " + tempFolder.getName(), Loglevel.NORMAL);

			}
			if (!f.isFile() || f.getName().startsWith(".")) { 
				continue;
			}

			// move this file into the temp directory, but check if we have something of the same name there alredy,
			// if we do, rename

			File targetFile = getSafeFilename(new File(tempFolder + File.separator + f.getName()));
			if (this.parameters.containsKey(BulkFileLoaderCommandLine.MOVEFILES_LONGNAME)) {
				FileUtils.moveFile(f, targetFile);
			} else {
				FileUtils.copyFile(f, targetFile);				
			}		
			fileInventoryByTempName.put(targetFile.getAbsolutePath(), fii);
			fii.setTempFilePath(targetFile.getAbsolutePath());
			fii.setBatchNumber(tempFolderCounter);
			fii.setNumberInBatch(numFiles++);
			filesInThisBatch.add(fii);

			// deduct size of this file to keep track of what we have left

			bytesLeft -= targetFile.length();
			// add to requesttxt
			String requestLine = 
					targetFile.getName().trim() + "," + 			// Title
							targetFile.getName().trim() + "," + 			// Description
							"#" + targetFile.getName().trim() + "," + 			// VersionData
							targetFile.getAbsolutePath();			// PathOnClient
			requesttxt.add(requestLine);
			bytesLeft -= (requestLine.length() + 2);	
			//				log("Batch: " + tempFolderCounter + " adding file: " + targetFile.getName(), Loglevel.NORMAL);
		}


		// now deal with any last batch

		if (numFiles > 0) {
			// finish processing this batch
			// write request.txt into temp folder
			String requestFilename = tempFolder + File.separator + "request.txt";	
			Utils.writeFile(requestFilename, requesttxt);
			// now create batch out of temp folder
			try {
				finishABatch(myJob,tempFolder, batchPrefix, tempFolderCounter, currentMaxRequestSize, batchInfos, filesInThisBatch, moveFiles, maxNumberOfFilesInBatch);
			} catch (AsyncApiException e) {
				log(e.getMessage(), Loglevel.BRIEF);

				// rename tempfolder
				tempFolder.renameTo(new File(tempFolder.getAbsolutePath() + "_failed"));
			}

		}

		// now repackage anything that failed into batches, try to reprocess

		if (!failedBatchFolderNames.isEmpty() && !failsReprocessed) {
			failsReprocessed = true;

			// copy all files into new failed folder

			for (String folderName : failedBatchFolderNames) {
				log("Reprocessing folder: " + folderName, Loglevel.NORMAL);
				for (File f : new File(folderName).listFiles()) {
					FileUtils.copyFileToDirectory(f, failedFolder);
				}
			}

			// reprocess fails folder

			batchInfos.addAll(createBatchesFromDirectory(myJob, failedFolder.getAbsolutePath(), "Failed_", true, 20, currentMaxRequestSize));

		}


		return batchInfos;
	}

	private BatchInfo createBatchFromZippedDirectory(JobInfo job, Path fileDir, String batchFilenamePrefix, int batchNumber, int currentMaxSize)
			throws AsyncApiException, IOException
	{
		String zipTarget = fileDir.getParent().toString() + File.separator + batchFilenamePrefix + batchNumber + ".zip";
		long startTime = startTiming();
		Utils.zipIt(zipTarget, fileDir.toString());
		int size = 0;
		int filesCount = 0;
		for (File f : FileUtils.listFiles(fileDir.toFile(), null, true)) {
			if (!f.getName().equals("request.txt")) {
				size += f.length();
				filesCount++;
			}
		}
		endTiming(startTime, "Zip time");

		// check if the batch zipped is bigger than 10Mb, if so try to split in half and try again

		File zippedBatch = new File(zipTarget);
		if (zippedBatch.length() > MAXZIPPEDBATCHSIZE) {
			log("Zipped batch over " + MAXZIPPEDBATCHSIZE + " byte size limit. Must split into smaller chunks.", Loglevel.NORMAL);
			// remove old zip file
			zippedBatch.delete();
			return null;
		} else {
			startTime = startTiming();
			BatchInfo b = bulkConnection.createBatchFromZipStream(job, Files.newInputStream(Paths.get(zipTarget)));
			endTiming(startTime, "Upload time");
			totalFiles += filesCount;
			totalFilesSent += filesCount;
			totalProcessedBytes += size;
			long elapsedTime = startTiming() - this.startTime;
			double millisPerByte = ((double)totalProcessedBytes) / elapsedTime;
			double bytesRemaining = totalToProcessBytes - totalProcessedBytes;
			long millisRemaining = (long)(bytesRemaining / millisPerByte); 
			final String hmsElapsed = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(elapsedTime),
					TimeUnit.MILLISECONDS.toMinutes(elapsedTime) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(elapsedTime)),
					TimeUnit.MILLISECONDS.toSeconds(elapsedTime)
					- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsedTime)));			 
			final String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millisRemaining),
					TimeUnit.MILLISECONDS.toMinutes(millisRemaining) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millisRemaining)),
					TimeUnit.MILLISECONDS.toSeconds(millisRemaining)
					- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millisRemaining)));
			log("Created batch " + batchNumber + " ID: " + b.getId() + " " + Paths.get(zipTarget).getFileName() + " files: " + filesCount 
					+ " data: " + String.format("%.2f", ((float)size)/1024/1024) + " Mb"
					+ " Total Files: " + totalFiles + " / " + totalToProcess + " (" + String.format("%.2f", (float) (((float)totalFiles*100)/totalToProcess)) + "%) "
					+ " Total Data: " + String.format("%.2f", ((float)totalProcessedBytes)/1024/1024) + " / " + String.format("%.2f", ((float)totalToProcessBytes)/1024/1024) + " Mb ("
					+ String.format("%.2f",  ((((float)totalProcessedBytes/1024/1024)*100)/((float)totalToProcessBytes/1024/1024))) + "%)"
					+ " Elapsed: " + hmsElapsed + " ETC: " + hms 
					, Loglevel.BRIEF);

			int compressedSizeOfThisZip = (int) new File(zipTarget).length();
			compressedSize += compressedSizeOfThisZip;
			uncompressedSize += size;
			log("Compression ratio now: " + String.format("%.2f",getCompressionRatio()), Loglevel.NORMAL);
			log("Total files tried so far: " + totalFiles + " uploaded: " + totalFilesSent + " last batch size uncompressed: " + String.format("%,d",size)
			+ " bytes, compressed: " + String.format("%,d", compressedSizeOfThisZip) + " ratio: " + String.format("%.2f", (float)compressedSizeOfThisZip/size), Loglevel.NORMAL);
			return b;
		}
	}

	private void createFileInventory(String sourceFolder) {
		long startTime = startTiming();
		Collection<File> myFiles = FileUtils.listFiles(new File(sourceFolder), null, true);
		// first, run through all the files and inventory them
		int fileCounter = 0;

		Iterator<File> i = myFiles.iterator(); 
		while (i.hasNext()) {
			File f = i.next();
			// set up the inventory
			FileInventoryItem fii = new FileInventoryItem();
			fii.setFileNumber(fileCounter);
			fii.setSourceFilePath(f.getAbsolutePath());
			fii.setSize(f.length());
			fileInventoryByNumber.put(fileCounter++, fii);
			fileInventoryBySourceName.put(f.getAbsolutePath(), fii);
			totalToProcess++;
			totalToProcessBytes += f.length();
		}
		endTiming(startTime, "Inventory");
	}

	/*
	 * 
	 * Method sorts the entire file inventory using bin sort algorithm
	 * 
	 */

	private void doBinSort() {
		long startTime = startTiming();
		int oversizeCount = 0;
		for (FileInventoryItem fii : fileInventoryByNumber.values()) {
			long size = fii.getSize();
			if (fii.getSize() > MAXSINGLEFILELENGTH) {
				// TODO: we should move off into FailedFiles right away
				// for now, just pack off into a bin and get on with it
				oversizeCount++;
				BatchBin b = new BatchBin();
				b.fileList.add(fii);
				b.isOversizeBin = true;
				b.totalSize += size;
				batchBins.add(batchBins.size()-1, b);
				continue;
			} else {
				boolean binFound = false;
				Iterator<BatchBin> binIterator = batchBins.iterator(); 
				while (!binFound && binIterator.hasNext()) {
					BatchBin b = binIterator.next();
					if (b.totalSize + fii.getSize() < MAXZIPPEDBATCHSIZE) {
						b.fileList.add(fii);
						b.totalSize += size;
						binFound = true;
					}
				}
				if (!binFound) {
					BatchBin b = new BatchBin();
					b.fileList.add(fii);
					b.totalSize += fii.getSize();
					batchBins.add(batchBins.size(), b);
				}
			}
		}
		log("Batches: " + batchBins.size() + " of which oversize: " + oversizeCount, Loglevel.BRIEF);
		endTiming(startTime, "Bin sort", Loglevel.BRIEF);
		long counter = 1;
		long totalSize = 0;
		for (BatchBin b : batchBins) {
			log("Batch " + counter++ + " files: " + b.fileList.size() + " total size: " + b.totalSize + " bytes", Loglevel.NORMAL);
			totalSize += b.totalSize;
		}
		log("Total file size: " + String.format("%,d", totalSize) + " bytes", Loglevel.NORMAL);
	}

	/**
	 * Create a new job using the Bulk API - will always upload ContentVersion
	 * 
	 * @param connection
	 *            BulkConnection used to create the new job.
	 * @return The JobInfo for the new job.
	 * @throws AsyncApiException
	 */
	private JobInfo createJob(BulkConnection connection) throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setObject("ContentVersion");
		job.setOperation(OperationEnum.insert);
		job.setContentType(ContentType.ZIP_CSV);
		job = connection.createJob(job);
		//System.out.println(job);
		return job;
	}

	private void endTiming(long startTime, String message) {
		endTiming(startTime, message, Loglevel.NORMAL);
	}

	private void endTiming(long startTime, String message, Loglevel level) {
		final long end = System.currentTimeMillis();
		final long diff = ((end - startTime));
		final String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(diff),
				TimeUnit.MILLISECONDS.toMinutes(diff) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(diff)),
				TimeUnit.MILLISECONDS.toSeconds(diff)
				- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(diff)));
		this.log(message + " duration: " + hms, level);
	}

	private void finishABatch(JobInfo myJob, File tempFolder, String batchPrefix, Integer tempFolderCounter2,
			int currentMaxRequestSize, ArrayList<BatchInfo> batchInfos, ArrayList<FileInventoryItem> filesInThisBatch,
			boolean moveFiles, int maxNumberOfFilesInBatch) throws AsyncApiException, IOException {
		BatchInfo batchInfo = null;
		batchInfo = createBatchFromZippedDirectory(myJob,Paths.get(tempFolder.toURI()), batchPrefix, tempFolderCounter2, currentMaxRequestSize);
		if (batchInfo != null) {
			batchMap.put("" + tempFolderCounter, batchInfo);
			batchInfos.add(batchInfo);
			// update the file inventory items in this batch so they know which batch they're part of
			for (FileInventoryItem fi : filesInThisBatch) {
				fi.setBatchId(batchInfo.getId());
				completeFileList.add(fi);
			}
			batchInventoryMapByBatchId.put(batchInfo.getId(), filesInThisBatch);
		} else {
			// zipped file too big, need to split it up

			// remove request.txt file
			FileUtils.deleteQuietly(new File(tempFolder.getAbsoluteFile() + File.separator + "request.txt"));

			// first rename the current temp folder to a different filename
			File renamedFolder = new File(tmpFolder + File.separator + "toobig_" + tooBigFolderCounter++);
			tempFolder.renameTo(renamedFolder);
			for (FileInventoryItem fi : filesInThisBatch) {
				fileInventoryByTempName.remove(fi.getTempFilePath());
				fi.setTempFilePath(fi.getTempFilePath().replace(tempFolder.getAbsolutePath(), renamedFolder.getAbsolutePath()));
				fileInventoryByTempName.put(fi.getTempFilePath(), fi);

			}

			// try to run the same thing with a smaller max source file size
			batchInfos.addAll(createBatchesFromDirectory(myJob, renamedFolder.toString(), batchPrefix, moveFiles, maxNumberOfFilesInBatch, MAXZIPPEDBATCHSIZE - 50000)); // 50K for the zip index

			// assume that we ultimately got all the files into a batch or dumped into error folder
			FileUtils.deleteDirectory(renamedFolder);
		}

	}

	private double getCompressionRatio() {
		if (uncompressedSize == 0.0 || compressedSize == 0.0) {
			return averageCompressionRatio;
		} else {
			return compressedSize/uncompressedSize;
		}
	}

	private void getContentDocumentIDsForBatch(BatchInfo b) throws ConnectionException {
		log("Looking for contentDocumentIDs for batch" + b.getId(), Loglevel.NORMAL);
		// find the right batch to populate IDs for
		List<FileInventoryItem> batchFileList = batchInventoryMapByBatchId.get(b.getId());

		// first get the contentversionIDs

		final Set<String> cvIDs = new HashSet<>();

		for (final FileInventoryItem fii : batchFileList) {
			cvIDs.add(fii.getContentVersionID());
		}

		// remove the null ID if it appears

		cvIDs.remove(null);

		// now call salesforce to get the contentversions and contentDocuments

		final Map<String, String> contentDocumentsMapByCVID = new HashMap<>();

		// login if needed 

		if (partnerConnection == null) {
			partnerConnection = LoginUtil.soapLogin(
					this.parameters.get(BulkFileLoaderCommandLine.SERVERURL_LONGNAME) + BulkFileLoader.URLBASE + this.myApiVersion,
					this.parameters.get(BulkFileLoaderCommandLine.USERNAME_LONGNAME),
					this.parameters.get(BulkFileLoaderCommandLine.PASSWORD_LONGNAME)
					);
		}


		// build the query

		final String queryStart = "SELECT ContentDocumentId,Id,Title FROM ContentVersion WHERE ID IN(";
		final String queryEnd = ")";
		final String[] myIDs = cvIDs.toArray(new String[cvIDs.size()]);
		final String queryMid = "'" + String.join("','", myIDs) + "'";

		final String query = queryStart + queryMid + queryEnd;

		log("Looking for contentDocumentIDs for " + cvIDs.size() + " documents.", Loglevel.NORMAL);
		log("Query: " + query, Loglevel.NORMAL);

		// run the query

		QueryResult qResult = partnerConnection.query(query);

		boolean done = false;
		if (qResult.getSize() > 0) {
			log("Found " + qResult.getSize() + " documents.", Loglevel.NORMAL);
			while (!done) {
				final SObject[] records = qResult.getRecords();
				for (final SObject o : records) {
					contentDocumentsMapByCVID.put((String)o.getField("Id"), (String)o.getField("ContentDocumentId"));
				}
				if (qResult.isDone()) {
					done = true;
				} else {
					qResult = partnerConnection.queryMore(qResult.getQueryLocator());
				}
			}
		} else {
			System.out.println("No records found.");
		}

		// now run through the files and update the FileInventoryItem

		for (FileInventoryItem fii : batchFileList) {
			fii.setContentDocumentID(contentDocumentsMapByCVID.get(fii.getContentVersionID()));
		}

	}

	//	private BatchInfo createBatchFromDirectory(JobInfo job, Path fileDir, int batchNumber)
	//			throws AsyncApiException, IOException
	//	{
	//		String zipTarget = fileDir.getParent().toString() + File.separator + "batch_" + batchNumber + ".zip";
	//		long startTime = startTiming();
	//		Utils.zipIt(zipTarget, fileDir.toString());
	//		endTiming(startTime, "Zip time");
	//		startTime = startTiming();
	//		BatchInfo b = bulkConnection.createBatchFromStream(job, Files.newInputStream(Paths.get(zipTarget)));
	//		endTiming(startTime, "Upload time");
	//		return b;
	//	}
	//
	//	private BatchInfo createBatchFromFiles(JobInfo job, Path fileDir, Path newCsv)
	//			throws AsyncApiException, IOException
	//	{
	//
	//		Map<String, InputStream> attachments = new HashMap<>();
	//		for (File f : fileDir.toFile().listFiles())
	//		{
	//			Path filePath = Paths.get(f.toURI());
	//			attachments.put(f.getName(), Files.newInputStream(filePath));
	//		}
	//
	//		return bulkConnection.createBatchWithInputStreamAttachments(job, Files.newInputStream(newCsv), attachments);
	//	}

	private File getSafeFilename(File file) {
		if (file.exists()) {
			String name = file.getName();
			// append a counter to the name just before the suffix
			// myfile.pdf -> myfile_1.pdf
			// if no suffix, just append at the end
			int fileExtensionStart = name.lastIndexOf(".");
			String newName = null;
			if (fileExtensionStart == -1) {
				newName = name + "_1";
			} else {
				newName = name.replace(name.substring(fileExtensionStart), "_1" + name.substring(fileExtensionStart));
			}
			return getSafeFilename(new File(file.getParent() + File.separator + newName));

		} else return file;
	}

	private void log(final String logText, final Loglevel level) {
		if ((this.loglevel == null) || (level.getLevel() <= this.loglevel.getLevel())) {
			System.out.println(logText);
		}
	}
	public void run() throws RemoteException, Exception {

		// set loglevel based on parameters
		this.loglevel = ("verbose".equals(this.parameters.get("loglevel"))) ? Loglevel.NORMAL : Loglevel.BRIEF;

		maxRequestSize = Integer.valueOf(this.parameters.get(BulkFileLoaderCommandLine.MAXREQUESTSIZE_LONGNAME));
		if (maxRequestSize < 1) {
			maxRequestSize = MAXREQUESTSIZE;
		}

		this.myApiVersion = Double.parseDouble(this.parameters.get(BulkFileLoaderCommandLine.APIVERSION_LONGNAME));

		this.startTime = startTiming();

		// do starting setup

		setupBasicStuff();

		// make an inventory of all the files

		createFileInventory(srcFolder);
		doBinSort();

		// get connection

		bulkConnection = LoginUtil.getBulkConnection(
				this.parameters.get(BulkFileLoaderCommandLine.SERVERURL_LONGNAME) + BulkFileLoader.URLBASE + this.myApiVersion,
				this.parameters.get(BulkFileLoaderCommandLine.USERNAME_LONGNAME),
				this.parameters.get(BulkFileLoaderCommandLine.PASSWORD_LONGNAME),
				String.valueOf(this.myApiVersion)
				);

		// create job



		JobInfo myJob = createJob(bulkConnection);

		// create batches from directory

		//ArrayList<BatchInfo> batchInfos = createBatchesFromDirectory(myJob, srcFolder, BATCHFOLDERPREFIX, false, 998, maxRequestSize);
		ArrayList<BatchInfo> batchInfos = createBatchesFromBins(myJob, srcFolder, BATCHFOLDERPREFIX, false, 998, maxRequestSize);
		// close job

		closeJob(bulkConnection, myJob);

		// await completion

		log("******************************************************", Loglevel.BRIEF);
		log("*       Batch Results                          *", Loglevel.BRIEF);
		log("******************************************************", Loglevel.BRIEF);

		awaitCompletion(bulkConnection, myJob, batchInfos);

		// check results

		checkResults(bulkConnection, myJob, batchInfos);

		writeResultsFile();

		endTiming(this.startTime, "Overall operation", Loglevel.BRIEF);
		log("Number of batches too big:" + (tooBigFolderCounter - 1), Loglevel.BRIEF);
		log("Uncompressed size:" + String.format("%,d", (int)uncompressedSize) + " bytes", Loglevel.BRIEF);
		log("Compressed size:" + String.format("%,d", (int)compressedSize) + " bytes", Loglevel.BRIEF);
		log("Overall compression ratio:" + String.format("%.2f", getCompressionRatio()), Loglevel.BRIEF);
		log("Rate:" + String.format("%.2f", uncompressedSize/1024/1024/((System.currentTimeMillis()-startTime)/1000)) + "Mb/s", Loglevel.BRIEF);

	}

	private void setupBasicStuff() throws IOException {

		this.srcFolder = this.parameters.get(BulkFileLoaderCommandLine.BASEDIRECTORY_LONGNAME);
		this.tmpFolder = this.parameters.get(BulkFileLoaderCommandLine.TEMPDIRECTORY_LONGNAME);

		//clean out tempfolder

		FileUtils.deleteDirectory(new File(tmpFolder));

		failedFolder = new File(tmpFolder + File.separator + "FailedFiles");
		if (!failedFolder.exists()) {
			failedFolder.mkdirs();
		}

	}

	private long startTiming() {
		return System.currentTimeMillis();
	}

	private void writeResultsFile() throws IOException {
		// initialize output file
		String outputFilename = srcFolder + File.separator + "output.csv";	
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilename));

		// write output file into source folder
		final String headerRow = "ContentVersionID,PathOnClient,TempPath,ContentDocumentId,Success,Error";
		bw.write(headerRow);
		bw.newLine();

		for (FileInventoryItem fii : completeFileList) {
			bw.write(fii.getOutputLine());
			bw.newLine();
		}
		bw.flush();
		bw.close();

	}

}
