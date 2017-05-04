/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ftp.mapred;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ftp.FTPParser;
import org.apache.hadoop.ftp.ZCopyBookFTPClient;
import org.apache.hadoop.ftp.password.FTP2HDFSCredentialProvider;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class FTP2HDFSDriver {

	static FTPClient ftp = null;
	static boolean zftp = false;
	static boolean zpds = false;
	static boolean ftpTransferLimitTrue = false;
	static boolean setKrb = false;
	static String ftptype = "binary";
	static String ftptypeopts = null;
	static String keytab = null;
	static String keytabupn = null;
	static Configuration conf = null;
	static FileSystem fileSystem = null;
	static ZCopyBookFTPClient ftpDownloader;
	static String hdfsPath = null;
	static String ftpfolder = null;
	static String ftppds = null;
	String path = null;
	static String fileName = null;
	String fileNameOut = null;
	static String fileType = null;
	static String ftpFileName = null;
	static String ftphost = null;
	static String userId = null;
	static String pwd = null;
	static String pwdAlias = null;
	static String pwdCredPath = null;
	static String downloadFile = null;
	static String ftpTransferLimit = null;
	static UserGroupInformation ugi = null;
	static Options options = new Options();
	String ftpHost = null;
	static String ftpTapeTransferType = null;


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			e4.printStackTrace();
		}

		options = new Options();
		options.addOption("ftp_host", true, "FTP Hostname --ftp_host (zhost.example.com)");
		options.addOption("transfer_type", true,
				"FTP TransferType --transfer_type requires (vb,fb,ascii,binary,zascii,zbinary)");
		options.addOption("transfer_type_opts", true,
				"FTP TransferType Options --transfer_type_opts FIXrecfm=80,LRECL=80,BLKSIZE=27920");
		options.addOption("tape_transfer_type", true,
				"FTP TransferType Options --tape_transfer_type F,V,S,X");
		options.addOption("ftp_folder", true, "FTP Server Folder --ftp_folder /foldername/ ");
		options.addOption("ftp_pds", true,
				"FTP Partitioned Data set Z/os Folder --ftp_pds TEST.PDS.DATASET.MNTH.M201209 ");
		options.addOption("ftp_userid", true, "FTP Userid --ftp_userid userid");
		options.addOption("ftp_pwd", true, "FTP Password --ftp_pwd password");
		options.addOption("ftp_transfer_limit", true,
				"FTP Client # of transfers to execute simultaneously should not transfer Note: 2-4 = optimal");
		options.addOption("hdfs_outdir", true, "HDFS Output Dir --hdfs_outdir");
		options.addOption("ftp_filename", true, "FTPFileName --filename G* or --filename PAID2011");
		options.addOption("krb_keytab", true, "KeyTab File to Connect to HDFS --krb_keytab $HOME/S00000.keytab");
		options.addOption("krb_upn", true,
				"Kerberos Princpial for Keytab to Connect to HDFS --krb_upn S00000@EXAMP.EXAMPLE.COM");
		options.addOption("help", false, "Display help");
		CommandLineParser parser = new FTPParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, otherArgs);
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		if (cmd.hasOption("ftp_host") && cmd.hasOption("hdfs_outdir") && cmd.hasOption("ftp_filename")
				&& cmd.hasOption("transfer_type")) {
			ftphost = cmd.getOptionValue("ftp_host");
			if (cmd.hasOption("ftp_userid") && (cmd.hasOption("ftp_pwd")
					|| (cmd.hasOption("ftp_pwd_alias") && cmd.hasOption("ftp_hadoop_cred_path")))) {
				userId = cmd.getOptionValue("ftp_userid");
				if (cmd.hasOption("ftp_pwd")) {
					pwd = cmd.getOptionValue("ftp_pwd");
				} else if (cmd.hasOption("ftp_pwd_alias") && cmd.hasOption("ftp_hadoop_cred_path")) {
					pwdAlias = cmd.getOptionValue("ftp_pwd_alias");
					pwdCredPath = cmd.getOptionValue("ftp_hadoop_cred_path");

				} else {
					System.out.println("Missing FTP Password / FTP Password Alias / FTP Hadoop Cred Path");
					missingParams();
					System.exit(0);
				}
			} else {
				System.out.println("Missing FTP Host / HDFS OutDir / FTP FileName / Transfter Type");
				missingParams();
				System.exit(0);
			}
			if (cmd.hasOption("ftp_folder") || cmd.hasOption("ftp_pds")) {
				if (cmd.hasOption("transfer_type")) {
					fileType = cmd.getOptionValue("transfer_type");
					System.out.println("fileType: " + fileType);
					if (cmd.hasOption("ftp_pds")
							&& (fileType.equalsIgnoreCase("vb") || fileType.equalsIgnoreCase("fb"))) {
						zftp = true;
						zpds = true;
						ftptype = "binary";
						ftppds = cmd.getOptionValue("ftp_pds");
						if (cmd.hasOption("transfer_type_opts")) {
							ftptypeopts = cmd.getOptionValue("transfer_type_opts");
						}
					}
					if (((cmd.hasOption("ftp_folder") || cmd.hasOption("ftp_pds"))
							&& (fileType.equalsIgnoreCase("zascii") || fileType.equalsIgnoreCase("zbinary")))) {
						zftp = true;
						if (fileType.equalsIgnoreCase("zascii")) {
							zftp = true;
							ftptype = "zascii";
						}
						if (fileType.equalsIgnoreCase("zbinary")) {
							zftp = true;
							ftptype = "zbinary";
						}
						if (cmd.hasOption("ftp_pds")) {
							zpds = true;
							ftppds = cmd.getOptionValue("ftp_pds");
						}
						if (cmd.hasOption("ftp_folder")) {
							ftpfolder = cmd.getOptionValue("ftp_folder");
						}
						if (zftp && cmd.hasOption("tape_transfer_type")) {
							ftpTapeTransferType = cmd.getOptionValue("tape_transfer_type");
						}
					}
					if (fileType.equalsIgnoreCase("ascii") || fileType.equalsIgnoreCase("binary")) {
						zftp = false;
						ftpfolder = cmd.getOptionValue("ftp_folder");
						if (fileType.equalsIgnoreCase("ascii")) {
							ftptype = "ascii";
						}
						if (fileType.equalsIgnoreCase("binary")) {
							ftptype = "binary";
						}
					}
				} else {
					System.out.println("Missing FTP Transfer Type");
					missingParams();
					System.exit(0);
				}
			} else {
				System.out.println("Missing FTP Folder or Partitioned Data Set");
				missingParams();
				System.exit(0);
			}
			if (cmd.hasOption("ftp_filename")) {
				ftpFileName = cmd.getOptionValue("ftp_filename");
			}
			if (cmd.hasOption("ftp_transfer_limit")) {
				ftpTransferLimitTrue = true;
				ftpTransferLimit = cmd.getOptionValue("ftp_transfer_limit");
			}
			if (cmd.hasOption("hdfs_outdir")) {
				hdfsPath = cmd.getOptionValue("hdfs_outdir");
			}
			if (cmd.hasOption("help")) {
				missingParams();
				System.exit(0);
			}
		} else {
			missingParams();
			System.exit(0);
		}

		if (cmd.hasOption("krb_keytab") && cmd.hasOption("krb_upn")) {
			setKrb = true;
			keytab = cmd.getOptionValue("krb_keytab");
			keytabupn = cmd.getOptionValue("krb_upn");
			File keytabFile = new File(keytab);
			if (keytabFile.exists()) {

				if (!(keytabFile.canRead())) {
					System.out.println("KeyTab  exists but cannot read it - exiting");
					missingParams();
					System.exit(1);
				}
			} else {
				System.out.println("KeyTab doesn't exist  - exiting");
				missingParams();
				System.exit(1);
			}
		}

		if (System.getProperty("oozie.action.conf.xml") != null) {
			conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
		}
		conf.set(Constants.FTP2HDFS_HOST, ftphost);
		conf.set(Constants.FTP2HDFS_USERID, userId);
		if (pwd != null) {
			conf.set(Constants.FTP2HDFS_PASS, pwd);
		}
		if (fileType != null) {
			conf.set(Constants.FTP2HDFS_TRANSFERTYPE, fileType);
		}
		if (ftptypeopts != null) {
			conf.set(Constants.FTP2HDFS_TRANSFERTYPE_OPTS, ftptypeopts);
		}
		if (ftppds != null) {
			conf.set(Constants.FTP2HDFS_PDS, ftppds);
		}
		if (ftpFileName != null) {
			conf.set(Constants.FTP2HDFS_FILENAME, ftpFileName);
		}
		if (ftpfolder != null) {
			conf.set(Constants.FTP2HDFS_FOLDER, ftpfolder);
		}
		if (ftpTapeTransferType != null ) {
			conf.set(Constants.FTP2HDFS_TAPE_OPTS, ftpTapeTransferType.trim());
		}
		conf.setBoolean(Constants.FTP2HDFS_ZPDS, zpds);
		conf.setBoolean(Constants.FTP2HDFS_ZFTP, zftp);
		conf.set("mapreduce.map.java.opts", "-Xmx5120m");
		if (pwdCredPath != null && pwdAlias != null) {
			String pwdCredPathHdfs = pwdCredPath;
			conf.set("hadoop.security.credential.provider.path", pwdCredPathHdfs);
			conf.set(Constants.FTP2HDFS_PASS_ALIAS, pwdAlias);
			String pwdAlias = conf.get(Constants.FTP2HDFS_PASS_ALIAS);
			if (pwdAlias != null) {
				FTP2HDFSCredentialProvider creds = new FTP2HDFSCredentialProvider();
				char[] pwdChars = creds.getCredentialString(conf.get("hadoop.security.credential.provider.path"),
						conf.get(Constants.FTP2HDFS_PASS_ALIAS), conf);
				if (pwdChars == null) {
					System.out.println("Invalid URI for Password Alias or CredPath");
					System.exit(1);
				}
			}
		}

		// propagate delegation related props from launcher job to MR job
		if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
			System.out
					.println("HADOOP_TOKEN_FILE_LOCATION is NOT NULL: " + System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
			conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
		}

		String jobname = null;
		if (ftpfolder != null) {
			jobname = ftphost + "_" + ftpfolder;
		} else {
			jobname = ftphost;
		}
		if (ftppds != null) {
			jobname = ftphost + "_" + ftppds.replaceAll("\'", "");
		} else {
			jobname = ftphost;
		}
		if (ftpTransferLimitTrue) {
			conf.set("mapreduce.job.running.map.limit", ftpTransferLimit);
		}
	    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);


		Job job = Job.getInstance(conf, "FTP2HDFS-" + jobname);
		job.addCacheFile(new Path("/apps/ftp2hdfs/commons-net.jar").toUri());
		job.addArchiveToClassPath(new Path("/apps/ftp2hdfs/commons-net.jar"));
		job.setJarByClass(FTP2HDFSDriver.class);
		job.setInputFormatClass(FTP2HDFSInputFormat.class);
		job.setOutputFormatClass(FTP2HDFSOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(FTP2HDFSCoreMapper.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(hdfsPath));

		job.waitForCompletion(true);
	}

	private static void missingParams() {
		String header = "FTP to HDFS Client";
		String footer = "\nPlease report issues at http://github.com/gss2002";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
		System.exit(0);
	}
}
