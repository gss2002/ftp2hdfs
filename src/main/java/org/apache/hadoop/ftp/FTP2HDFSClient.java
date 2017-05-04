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

package org.apache.hadoop.ftp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ftp.mapred.Constants;
import org.apache.hadoop.ftp.password.FTP2HDFSCredentialProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class FTP2HDFSClient {
	static FTPClient ftp = null;
	static boolean zftp = false;
	static boolean zpds = false;
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
	static String ftpTapeTransferType = null;
	static UserGroupInformation ugi = null;
	static Options options = new Options();

	String ftpHost = null;

	public static void downloadFile0(String remoteFilePath, String hdfsPath) {
		try {

			InputStream in = null;

			String remoteFile = remoteFilePath;

			Path path = new Path(hdfsPath);
			if (fileSystem.exists(path)) {
				System.out.println("File " + hdfsPath + " already exists");
				return;
			}

			FSDataOutputStream out = fileSystem.create(path);

			try {
				byte[] buf = new byte[1048576];
				int bytes_read = 0;
				in = ftp.retrieveFileStream(remoteFile);

				do {
					bytes_read = in.read(buf, 0, buf.length);

					if (bytes_read < 0) {
						/* Handle EOF however you want */
					}

					if (bytes_read > 0)
						out.write(buf, 0, bytes_read);
					out.flush();

				} while (bytes_read >= 0);

			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
			out.close();
			in.close();
			boolean success = ftp.completePendingCommand();
			if (success) {
				System.out.println("File #2 has been downloaded successfully.");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void disconnect() {
		if (this.ftp.isConnected()) {
			try {
				this.ftp.logout();
				this.ftp.disconnect();
			} catch (IOException f) {
				// do nothing as file is already downloaded from FTP server
			}
		}
	}

	public static void downloadFile() {
		UserGroupInformation.setConfiguration(conf);
		if (UserGroupInformation.isSecurityEnabled()) {
			try {
				if (setKrb == true) {
					ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabupn, keytab);
				} else {
					ugi = UserGroupInformation.getCurrentUser();
				}
				System.out.println("UserId for Hadoop: " + ugi.getUserName());
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
				System.out.println("Exception Getting Credentials Exiting!");
				System.exit(1);
			}

			getFS();

			try {
				System.out.println("HasCredentials: " + ugi.hasKerberosCredentials());
				System.out.println("UserShortName: " + ugi.getShortUserName());
				System.out.println("Login KeyTab Based: " + UserGroupInformation.isLoginKeytabBased());
				System.out.println("Login Ticket Based: " + UserGroupInformation.isLoginTicketBased());

				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						System.out.println("Downloading: " + downloadFile + " to HDFS: " + hdfsPath + "/"
								+ downloadFile.replaceAll("\'", ""));
						downloadFile0(downloadFile, hdfsPath + "/" + downloadFile.replaceAll("\'", ""));
						return null;
					}
				});
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			getFS();
			System.out.println(
					"Downloading: " + downloadFile + " to HDFS: " + hdfsPath + "/" + downloadFile.replaceAll("\'", ""));
			downloadFile0(downloadFile, hdfsPath + "/" + downloadFile.replaceAll("\'", ""));
		}
	}

	public static void main(String[] args) {
		ArrayList<String> ftpFileLst = new ArrayList<String>();
		Configuration mainConf = new Configuration();
		String[] otherArgs = null;

		try {
			otherArgs = new GenericOptionsParser(mainConf, args).getRemainingArgs();
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
		options.addOption("ftp_pwd_alias", true, "FTP Password Alias --ftp_pwd_alias password.alias");
		options.addOption("ftp_hadoop_cred_path", true,
				"FTP Password --ftp_hadoop_cred_path /user/username/credstore.jceks");
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
			if (cmd.hasOption("hdfs_outdir")) {
				hdfsPath = cmd.getOptionValue("hdfs_outdir");
			}
			if (cmd.hasOption("help")) {
				missingParams();
				System.exit(0);
			}
		} else {
			System.out.println("Missing FTP Host / HDFS OutDir / FTP FileName / Transfter Type");
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

		setConfig();

		if (pwdCredPath != null && pwdAlias != null) {
			String pwdCredPathHdfs = pwdCredPath;
			conf.set("hadoop.security.credential.provider.path", pwdCredPathHdfs);
			conf.set(Constants.FTP2HDFS_PASS_ALIAS, pwdAlias);
			String pwdAlias = conf.get(Constants.FTP2HDFS_PASS_ALIAS);
			if (pwdAlias != null) {
				char[] pwdChars = null;
				try {
					if (UserGroupInformation.isSecurityEnabled()) {
						System.out.println("SecEnabled: " + true);
						pwdChars = ugi.doAs(new PrivilegedExceptionAction<char[]>() {
							public char[] run() throws Exception {
								FTP2HDFSCredentialProvider creds = new FTP2HDFSCredentialProvider();
								char[] pwdChar = creds.getCredentialString(
										conf.get("hadoop.security.credential.provider.path"),
										conf.get(Constants.FTP2HDFS_PASS_ALIAS), conf);
								return pwdChar;
							}
						});
					} else {
						System.out.println("SecEnabled: " + false);
						FTP2HDFSCredentialProvider creds = new FTP2HDFSCredentialProvider();
						pwdChars = creds.getCredentialString(conf.get("hadoop.security.credential.provider.path"),
								conf.get(Constants.FTP2HDFS_PASS_ALIAS), conf);
					}
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (pwdChars == null) {
					System.out.println("Invalid URI for Password Alias or CredPath");
					System.exit(1);
				} else {
					pwd = new String(pwdChars);
				}
			}
		}

		try {
			if ((zftp) && (zpds)) {
				ftpDownloader = new ZCopyBookFTPClient(ftphost, userId, pwd, fileType, ftpFileName, ftptypeopts, ftpTapeTransferType);
				ftp = ftpDownloader.getFtpClient();
				System.out.println("PDS: " + ftppds);
				ftpFileLst = ftpDownloader.listFiles("\'" + ftppds + "\'");
				for (int i = 0; i < ftpFileLst.size(); i++) {
					String fileNameOut = ftpFileLst.get(i);
					fileName = "\'" + ftppds + "." + fileNameOut + "\'";
					System.out.println("PDS FileName: " + fileName);

					downloadFile = fileName;
					downloadFile();
				}
			}
		} catch (FTPLoginException e) {
			System.out.println("FTP Login is invalid");
			System.exit(1);
		} catch (FTPConnectionClosedException e) {
			// TODO Auto-generated catch block
			System.out.println("FTP Server Closed Connection/Unavailable");
			System.exit(1);		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("FTP Server Closed Connection/Unavailable or IO/Socket Exception");
			System.exit(1);		
		}

		System.out.println("FTP File downloaded successfully");
		try {
			ftp.disconnect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void setConfig() {
		conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
	}

	public static void getFS() {
		try {
			fileSystem = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void missingParams() {
		String header = "FTP to HDFS Client";
		String footer = "\nPlease report issues at http://github.com/gss2002";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
		System.exit(0);
	}
}
