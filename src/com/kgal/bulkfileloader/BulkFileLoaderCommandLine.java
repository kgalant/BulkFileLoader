package com.kgal.bulkfileloader;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.kgal.bulkfileloader.BulkFileLoader;
import com.kgal.bulkfileloader.BulkFileLoaderCommandLine;
import com.salesforce.migrationtoolutils.Utils;

public class BulkFileLoaderCommandLine {
	// command line static strings

	public static final String MAXREQUESTSIZE = "mx";
	public static final String MAXREQUESTSIZE_LONGNAME = "maxsize";
	public static final String BASEDIRECTORY = "s";
	public static final String BASEDIRECTORY_LONGNAME = "sourcedirectory";
	public static final String TEMPDIRECTORY = "t";
	public static final String TEMPDIRECTORY_LONGNAME = "tempdirectory";
	public static final String APIVERSION = "a";
	public static final String APIVERSION_LONGNAME = "apiversion";
	public static final String ORGFILE = "o";
	public static final String ORGFILE_LONGNAME = "orgfile";
	public static final String USERNAME = "u";
	public static final String USERNAME_LONGNAME = "username";
	public static final String PASSWORD = "p";
	public static final String PASSWORD_LONGNAME = "password";
	public static final String SERVERURL = "s";
	public static final String SERVERURL_LONGNAME = "serverurl";
	public static final String SKIPPATTERNS = "sp";	
	public static final String MOVEFILES_LONGNAME = "movefiles";
	public static final String MOVEFILES = "mf";
	public static final String VERBOSE = "v";
	public static final String VERBOSE_LONGNAME = "verbose";

	/**
	 * @param args
	 * @throws Exception
	 * @throws RemoteException
	 */
	public static void main(final String[] args) throws RemoteException, Exception {
		final BulkFileLoaderCommandLine pbc = new BulkFileLoaderCommandLine();

		if (pbc.parseCommandLine(args)) {
			final BulkFileLoader pb = new BulkFileLoader(pbc.getParameters());
			pb.run();
		}

	}

	private final Map<String, String> parameters = new HashMap<>();

	private final Options options = new Options();

	public BulkFileLoaderCommandLine() {
		this.setupOptions();
	}

	/**
	 * @return the parameters collected from command line or parameter file
	 */
	public Map<String, String> getParameters() {
		return this.parameters;
	}

	public boolean parseCommandLine(final String[] args) {

		boolean canProceed = false;

		// put in default parameters
		this.parameters.put(APIVERSION_LONGNAME, "" + BulkFileLoader.API_VERSION);

		// now parse the command line

		final CommandLineParser parser = new DefaultParser();
		CommandLine line = null;
		try {
			// parse the command line arguments
			line = parser.parse(this.options, args);
		} catch (final ParseException exp) {
			// oops, something went wrong
			System.err.println("Command line parsing failed.  Reason: " + exp.getMessage());
			System.exit(-1);
		}

		// first, add any parameters from any property files provided on command line

		if (line != null) {
			// first initialize parameters from any parameter files provided

			if (line.hasOption(ORGFILE) && (line.getOptionValue(ORGFILE) != null) && (line.getOptionValue(ORGFILE).length() > 0)) {
				final String paramFilesParameter = line.getOptionValue(ORGFILE);
				for (final String paramFileName : paramFilesParameter.split(",")) {
					final Properties props = Utils.initProps(paramFileName.trim());
					System.out.println("Loading parameters from file: " + paramFileName);
					this.addParameterFromProperty(props, "sf.apiversion");
					this.addParameterFromProperty(props, "sf.serverurl");
					this.addParameterFromProperty(props, "sf.username");
					this.addParameterFromProperty(props, "sf.password");
					this.addParameterFromProperty(props, MAXREQUESTSIZE_LONGNAME);
					this.addParameterFromProperty(props, BASEDIRECTORY_LONGNAME);
					this.addParameterFromProperty(props, TEMPDIRECTORY_LONGNAME);
					this.addBooleanParameterFromProperty(props, MOVEFILES_LONGNAME);
					this.addBooleanParameterFromProperty(props, VERBOSE_LONGNAME);

				}
			}
		}

		// now add any parameters from command line
		// will supersede anything provided in property files

		this.addCmdlineParameter(line, MAXREQUESTSIZE, MAXREQUESTSIZE_LONGNAME);
		this.addCmdlineParameter(line, BASEDIRECTORY, BASEDIRECTORY_LONGNAME);
		this.addCmdlineParameter(line, TEMPDIRECTORY, TEMPDIRECTORY_LONGNAME);

		this.addCmdlineParameter(line, APIVERSION, APIVERSION_LONGNAME);
		this.addCmdlineParameter(line, USERNAME, USERNAME_LONGNAME);
		this.addCmdlineParameter(line, SERVERURL, SERVERURL_LONGNAME);
		this.addCmdlineParameter(line, PASSWORD, PASSWORD_LONGNAME);
		this.addBooleanParameter(line, MOVEFILES, MOVEFILES_LONGNAME);
		this.addBooleanParameter(line, VERBOSE, VERBOSE_LONGNAME);


		////////////////////////////////////////////////////////////////////////
		//
		// from here on down, any special treatment for individual parameters
		//
		////////////////////////////////////////////////////////////////////////

		// set maxitems to default value if nothing provided
		if (!this.isParameterProvided(MAXREQUESTSIZE_LONGNAME)) {
			this.parameters.put(MAXREQUESTSIZE_LONGNAME, String.valueOf(BulkFileLoader.MAXREQUESTSIZE));
		} 

		// if verbose parameter is provided, set loglevel to verbose, else it will default to normal
		if (isOptionSet(VERBOSE_LONGNAME)) {
			this.parameters.put("loglevel", VERBOSE_LONGNAME);
		}        
		
		////////////////////////////////////////////////////////////////////////
		//
		// now check that we have minimum parameters needed to run
		//
		////////////////////////////////////////////////////////////////////////

		// check that we have the minimum parameters
		// either b(asedir) and d(estinationdir)
		// or s(f_url), p(assword), u(sername)

		if (this.isParameterProvided(BASEDIRECTORY_LONGNAME) && 
				this.isParameterProvided(SERVERURL_LONGNAME) &&
				this.isParameterProvided(USERNAME_LONGNAME) &&
				this.isParameterProvided(PASSWORD_LONGNAME)) {
			canProceed = true;
		} else {
			System.out.println("Mandatory parameters not provided in files or commandline -"
					+ " basedir and serverurl, username and password required as minimum");
		}

		for (final String key : this.parameters.keySet()) {
			if (key.equals("password")) {
				System.out.println(key + ":" + this.parameters.get(key).replaceAll(".", "*"));
			} else {
				System.out.println(key + ":" + this.parameters.get(key));
			}
		}

		if (!canProceed) {
			this.printHelp();
		}
		return canProceed;
	}

	/**
	 * Extract parameters if provided
	 *
	 * @param cmdLineName
	 * @param tagName
	 */
	private void addCmdlineParameter(final CommandLine line, final String cmdLineName, final String tagName) {
		if (line.hasOption(cmdLineName) && (line.getOptionValue(cmdLineName) != null)
				&& (line.getOptionValue(cmdLineName).length() > 0)) {
			this.parameters.put(tagName, line.getOptionValue(cmdLineName));
		}
	}

	private void addParameterFromProperty(final Properties props, final String propName) {
		// Some properties start with "sf.", but we only use the name behind
		final String paramName = (propName.startsWith("sf.")) ? propName.substring(3) : propName;
		if (props.getProperty(propName) != null) {
			this.parameters.put(paramName, props.getProperty(propName));
		}
	}

	private boolean addBooleanParameter(final CommandLine line, final String optionName, final String paramName) {
		boolean result = false;
		if (line.hasOption(optionName)) {
			this.parameters.put(paramName, "true");
			result = true;
		}
		return result;
	}

	private void addBooleanParameterFromProperty(final Properties props, final String propName) {
		// Some properties start with "sf.", but we only use the name behind
		final String paramName = (propName.startsWith("sf.")) ? propName.substring(3) : propName;
		if (props.getProperty(propName) != null) {
			this.parameters.put(paramName, "true");
		}
	}

	// wraps the generations/fetching of an org id for database purposes

	private boolean isParameterProvided(final String parameterName) {
		return ((this.parameters.get(parameterName) != null) && (this.parameters.get(parameterName).length() > 0));
	}

	private boolean isOptionSet(final String parameterName) {
		return (this.parameters.get(parameterName) != null);
	}

	private void printHelp() {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.setOptionComparator(null);

		formatter.printHelp(
				"java -jar BulkFileLoader.jar [-b basedirectory] [-o <parameter file1>,<parameter file2>] [-u <SF username>] [-p <SF password>]",
				this.options);
	}

	private void setupOptions() {
		this.options.addOption(Option.builder(ORGFILE).longOpt(ORGFILE_LONGNAME)
				.desc("file containing org parameters (see below)")
				.hasArg()
				.build());
		this.options.addOption(Option.builder(USERNAME).longOpt(USERNAME_LONGNAME)
				.desc("username for the org (someuser@someorg.com)")
				.hasArg()
				.build());
		this.options.addOption(Option.builder(PASSWORD).longOpt(PASSWORD_LONGNAME)
				.desc("password for the org (t0pSecr3t)")
				.hasArg()
				.build());
		this.options.addOption(Option.builder(SERVERURL).longOpt(SERVERURL_LONGNAME)
				.desc("server URL for the org (https://login.salesforce.com)")
				.hasArg()
				.build());
		this.options.addOption(Option.builder(APIVERSION).longOpt(APIVERSION_LONGNAME)
				.desc("api version to use, will default to " + BulkFileLoader.API_VERSION)
				.hasArg()
				.build());

		// handling of max items per package
		this.options.addOption(Option.builder(MAXREQUESTSIZE).longOpt(MAXREQUESTSIZE_LONGNAME)
				.desc("max number of files to put in a single batch (defaults to 1000 if not provided)")
				.hasArg()
				.build());

		// handling for building a package from a directory

		this.options.addOption(Option.builder(BASEDIRECTORY).longOpt(BASEDIRECTORY_LONGNAME)
				.desc("base directory from which to generate package.xml")
				.hasArg()
				.build());     

		this.options.addOption(Option.builder(BASEDIRECTORY).longOpt(BASEDIRECTORY_LONGNAME)
				.desc("source directory for upload all files and files in directories below this directory will be attempted uploaded. Output.csv file will be placed in this directory")
				.hasArg()
				.build());

		this.options.addOption(Option.builder(TEMPDIRECTORY).longOpt(TEMPDIRECTORY_LONGNAME)
				.desc("where the temporary files (temp folders and zipped batches) will be placed")
				.hasArg()
				.build());

		this.options.addOption(Option.builder(TEMPDIRECTORY).longOpt(TEMPDIRECTORY_LONGNAME)
				.desc("if this flag is set, files to upload will be moved (rather than copied) to the temporary directory")
				.build());

		// adding handling for brief output parameter

		this.options.addOption(Option.builder(VERBOSE).longOpt(VERBOSE_LONGNAME)
				.desc("output verbose logging instead of just core output")
				.build());
	}
}
