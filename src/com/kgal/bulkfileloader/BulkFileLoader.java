package com.kgal.bulkfileloader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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


public class BulkFileLoader {

	private Loglevel          loglevel;
	public enum Loglevel {
		VERBOSE(2), NORMAL(1), BRIEF(0);
		private final int level;

		Loglevel(final int level) {
			this.level = level;
		}

		int getLevel() {
			return this.level;
		}

	};


	private double                                  myApiVersion;

	public static final int     MAXREQUESTSIZE      = 20920000;
	public static final int     MAXZIPPEDBATCHSIZE      = 10000000;
	public static final double     BACKOFFFACTOR      = 0.66;
	public static final double   API_VERSION            = 45.0;
	private static final String  URLBASE                = "/services/Soap/u/";
	private static final String  BATCHFOLDERPREFIX		= "Batch_";

	private final Map<String, String> parameters    = new HashMap<>();
	private final Map<String, BatchInfo> batchMap    = new HashMap<>();
	private int maxRequestSize;
	private BulkConnection                      bulkConnection;

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


	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	private long startTiming() {
		return System.currentTimeMillis();
	}

	private void endTiming(long startTime, String message) {
		final long end = System.currentTimeMillis();
		final long diff = ((end - startTime));
		final String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(diff),
				TimeUnit.MILLISECONDS.toMinutes(diff) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(diff)),
				TimeUnit.MILLISECONDS.toSeconds(diff)
				- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(diff)));
		this.log(message + " duration: " + hms, Loglevel.NORMAL);
	}

	private void log(final String logText, final Loglevel level) {
		if ((this.loglevel == null) || (level.getLevel() <= this.loglevel.getLevel())) {
			System.out.println(logText);
		}
	}

	public BulkFileLoader(final Map<String, String> parameters) {
		this.parameters.putAll(parameters);

	}

	public void run() throws RemoteException, Exception {

		// set loglevel based on parameters
		this.loglevel = ("verbose".equals(this.parameters.get("loglevel"))) ? Loglevel.VERBOSE : Loglevel.NORMAL;

		maxRequestSize = Integer.valueOf(this.parameters.get(BulkFileLoaderCommandLine.MAXREQUESTSIZE_LONGNAME));
		if (maxRequestSize < 1) {
			maxRequestSize = MAXREQUESTSIZE;
		}

		this.myApiVersion = Double.parseDouble(this.parameters.get(BulkFileLoaderCommandLine.APIVERSION_LONGNAME));

		// get connection

		bulkConnection = LoginUtil.getBulkConnection(
				this.parameters.get(BulkFileLoaderCommandLine.SERVERURL_LONGNAME) + BulkFileLoader.URLBASE + this.myApiVersion,
				this.parameters.get(BulkFileLoaderCommandLine.USERNAME_LONGNAME),
				this.parameters.get(BulkFileLoaderCommandLine.PASSWORD_LONGNAME),
				String.valueOf(this.myApiVersion)
				);

		this.srcFolder = this.parameters.get(BulkFileLoaderCommandLine.BASEDIRECTORY_LONGNAME);
		this.tmpFolder = this.parameters.get(BulkFileLoaderCommandLine.TEMPDIRECTORY_LONGNAME);

		// create job

		long startTime = startTiming();

		JobInfo myJob = createJob(bulkConnection);
		
		// do starting setup
		
		setupBasicStuff();

		// create batches from directory

		ArrayList<BatchInfo> batchInfos = createBatchesFromDirectory(myJob, srcFolder, BATCHFOLDERPREFIX, false, 999, maxRequestSize);

		// close job

		closeJob(bulkConnection, myJob);

		// await completion

		awaitCompletion(bulkConnection, myJob, batchInfos);

		// check results

		checkResults(bulkConnection, myJob, batchInfos);

		endTiming(startTime, "Overall operation");
		log("Number of batches too big:" + (tooBigFolderCounter - 1), Loglevel.NORMAL);
		log("Uncompressed size:" + uncompressedSize/1024/1024 + " Mb", Loglevel.NORMAL);
		log("Overall compression ratio:" + String.format("%.2f", getCompressionRatio()), Loglevel.NORMAL);
		log("Rate:" + String.format("%.2f", uncompressedSize/1024/1024/((System.currentTimeMillis()-startTime)/1000)) + "Mb/s", Loglevel.NORMAL);

	}

	private void setupBasicStuff() throws IOException {
		
		//clean out tempfolder
		
		FileUtils.deleteDirectory(new File(tmpFolder));
		
		failedFolder = new File(tmpFolder + File.separator + "FailedFiles");
		if (!failedFolder.exists()) {
			failedFolder.mkdirs();
		}
		
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
		requesttxt.add(headerRow);
		bytesLeft -= (headerRow.length() + 2); // adding space for CRLF 
		tempFolder = new File(tmpFolder + File.separator + batchPrefix + tempFolderCounter);
		FileUtils.deleteDirectory(tempFolder);
		tempFolder.mkdirs();
		log("Created batch folder: " + tempFolder.getAbsolutePath(), Loglevel.NORMAL);

		while (i.hasNext()) {
			File f = i.next();

			if (f.length() > currentMaxRequestSize || f.length() == 0) {
				// file too big altogether, stop processing
				File failedFile = new File(failedFolder.toString() + File.separator + f.getName());
				f.renameTo(failedFile);
				log("File " + f.getAbsolutePath() + " is too big or has size 0, cannot continue. Moving to failed: " + failedFile.getAbsolutePath(), Loglevel.BRIEF);
				continue;
			} else if (f.length() * getCompressionRatio() > bytesLeft || numFiles > maxNumberOfFilesInBatch){
				// finish processing this batch
				// write request.txt into temp folder
				String requestFilename = tempFolder + File.separator + "request.txt";	
				Utils.writeFile(requestFilename, requesttxt);
				// now create batch out of temp folder
				BatchInfo batchInfo = null;
				totalFiles += numFiles;
				try {
					batchInfo = createBatchFromZippedDirectory(myJob,Paths.get(tempFolder.toURI()), batchPrefix, tempFolderCounter, currentMaxRequestSize);
					if (batchInfo != null) {
						batchMap.put("" + tempFolderCounter, batchInfo);
						batchInfos.add(batchInfo);
						uncompressedSize += (currentMaxRequestSize - bytesLeft);
						log("Compression ratio now: " + getCompressionRatio(), Loglevel.BRIEF);
						log("Total files tried so far: " + totalFiles + " uploaded: " + totalFilesSent, Loglevel.NORMAL);
					} else {
						// zipped file too big, need to split it up
						
						// first rename the current temp folder to a different filename
						File renamedFolder = new File(tmpFolder + File.separator + "toobig_" + tooBigFolderCounter++);
						tempFolder.renameTo(renamedFolder);
						
						// try to run the same thing with a smaller max source file size
						batchInfos.addAll(createBatchesFromDirectory(myJob, renamedFolder.toString(), batchPrefix, moveFiles, maxNumberOfFilesInBatch, (int) (currentMaxRequestSize * BACKOFFFACTOR)));
						
						// assume that we ultimately got all the files into a batch or dumped into error folder
						FileUtils.deleteDirectory(renamedFolder);
					}

					totalFilesSent += numFiles;
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
				bytesLeft -= (headerRow.length() + 2); // adding space for CRLF 
				tempFolder = new File(tmpFolder + File.separator + batchPrefix + ++tempFolderCounter);
				tempFolder.mkdirs();
				log("Created batch folder: " + tempFolder.getName(), Loglevel.NORMAL);

			} else {
				if (!f.isFile()) { 
					continue;
				}
				// move this file into the temp directory
				File targetFile = new File(tempFolder + File.separator + f.getName()); 
				//				f.renameTo(targetFile);
				FileUtils.copyFile(f, targetFile);
				numFiles++;
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
		}

		// now deal with any last batch

		if (numFiles > 0) {
			// finish processing this batch
			// write request.txt into temp folder
			String requestFilename = tempFolder + File.separator + "request.txt";	
			Utils.writeFile(requestFilename, requesttxt);
			// now create batch out of temp folder
			BatchInfo batchInfo = null;
			totalFiles += numFiles;
			try {
				long startTime = startTiming();
				//				batchInfo = bulkConnection.createBatchFromDir(myJob, null, tempFolder);
				endTiming(startTime, "Upload");
				//				batchInfo = createBatchFromFiles(myJob, Paths.get(tempFolder.toURI()), Paths.get(requestFilename));
				batchInfo = createBatchFromZippedDirectory(myJob,Paths.get(tempFolder.toURI()), batchPrefix, tempFolderCounter, currentMaxRequestSize);
				//				System.out.println(batchInfo);
				batchInfos.add(batchInfo);
				batchMap.put("" + tempFolderCounter, batchInfo);
				totalFilesSent += numFiles;
				uncompressedSize += (currentMaxRequestSize - bytesLeft);
				log("Compression ratio now: " + getCompressionRatio(), Loglevel.BRIEF);
				log("Total files tried so far: " + totalFiles + " uploaded: " + totalFilesSent, Loglevel.NORMAL);
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
		endTiming(startTime, "Zip time");

		// check if the batch zipped is bigger than 10Mb, if so try to split in half and try again

		File zippedBatch = new File(zipTarget);
		if (zippedBatch.length() > MAXZIPPEDBATCHSIZE) {
			log("Zipped batch over " + MAXZIPPEDBATCHSIZE + " byte size limit. Must split into smaller chunks.", Loglevel.BRIEF);
			// remove old zip file
			zippedBatch.delete();
			return null;
		} else {
			startTime = startTiming();
			BatchInfo b = bulkConnection.createBatchFromZipStream(job, Files.newInputStream(Paths.get(zipTarget)));
			endTiming(startTime, "Upload time");
			compressedSize += new File(zipTarget).length();
			return b;
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
	
	private void closeJob(BulkConnection connection, JobInfo myJob)	throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setId(myJob.getId());
		job.setState(JobStateEnum.Closed);
		connection.updateJob(job);
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
	 */
	private void awaitCompletion(BulkConnection connection, JobInfo job, ArrayList<BatchInfo> batchInfoList) throws AsyncApiException {
		long sleepTime = 0L;
		HashSet<String> incomplete = new HashSet<String>();
		for (BatchInfo bi : batchInfoList) {
			incomplete.add(bi.getId());
		}
		while (!incomplete.isEmpty()) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {}
			System.out.println("Awaiting results..." + incomplete.size() + " batches outstanding.");
			sleepTime = 10000L;
			BatchInfo[] statusList =
					connection.getBatchInfoList(job.getId()).getBatchInfo();
			for (BatchInfo b : statusList) {
				if (b.getState() == BatchStateEnum.Completed
						|| b.getState() == BatchStateEnum.Failed) {
					if (incomplete.remove(b.getId())) {
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
	 */
	private void checkResults(BulkConnection connection, JobInfo job, ArrayList<BatchInfo> batchInfoList) throws AsyncApiException, IOException {
		// batchInfoList was populated when batches were created and submitted

		int totalSuccesses = 0;
		int totalFails = 0;
		for (BatchInfo b : batchInfoList) {
			CSVReader rdr =	new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
			ArrayList<String> resultHeader = rdr.nextRecord();
			int resultCols = resultHeader.size();

			ArrayList<String> row;
			int fails = 0;
			int successes = 0;
			while ((row = rdr.nextRecord()) != null) {
				Map<String, String> resultInfo = new HashMap<String, String>();
				for (int i = 0; i < resultCols; i++) {
					resultInfo.put(resultHeader.get(i), row.get(i));
				}
				boolean success = Boolean.valueOf(resultInfo.get("Success"));
				boolean created = Boolean.valueOf(resultInfo.get("Created"));
				String id = resultInfo.get("Id");
				String error = resultInfo.get("Error");
				if (success && created) {
					//					System.out.println("Created row with id " + id);
					successes++;
				} else if (!success) {
					System.out.println("Failed with error: " + error);
					fails++;
				}
			}
			log("Batch: " + b.getId() + " Successes: " + successes + " Fails: " + fails + " API time: " + b.getApiActiveProcessingTime(), Loglevel.NORMAL);
			totalSuccesses += successes;
			totalFails += fails;

		}
		log("Total successes: " + totalSuccesses + " Fails: " + totalFails, Loglevel.NORMAL);
	}

	private double getCompressionRatio() {
		if (uncompressedSize == 0.0 || compressedSize == 0.0) {
			return averageCompressionRatio;
		} else {
			return compressedSize/uncompressedSize;
		}
	}

}
