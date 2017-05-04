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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.commons.net.ftp.FTPReply;

public class ZCopyBookFTPClient {
	FTPClient ftp = null;
	String fileType = null;
	String host = null;
	String user = null;
	String pwd = null;
	String fileTransferList = null;
	String fileName = null;
	String ftpTapeOptions = null;
	String ftpTypeOpts = null;

	private static final Log LOG = LogFactory.getLog(ZCopyBookFTPClient.class.getName());

	public ZCopyBookFTPClient(String ftphostIn, String userIdIn, String pwdIn, String fileTypeIn, String fileIn,
			String ftpTypeOptsIn, String ftpTapeOptions) {
		this.fileType = fileTypeIn;
		this.host = ftphostIn;
		this.user = userIdIn;
		this.pwd = pwdIn;
		this.ftpTapeOptions = ftpTapeOptions;
		this.fileName = fileIn;
		this.ftpTypeOpts = ftpTypeOptsIn;
	}

	public FTPClient getFtpClient() throws FTPConnectionClosedException, IOException, FTPLoginException {
		ftp = new FTPClient();
		FTPClientConfig config = new FTPClientConfig(FTPClientConfig.SYST_MVS);
		ftp.configure(config);
		if (LOG.isDebugEnabled()) {
			ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));
		}
		int reply;
		ftp.setBufferSize(1048576);
		ftp.setReceiveBufferSize(1048576);
		ftp.connect(host);
		reply = ftp.getReplyCode();
		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			throw new FTPConnectionClosedException("Exception in connecting to FTP Server");
		}
		System.out.println(Charset.defaultCharset());
		boolean loginSuccess = ftp.login(user, pwd);
		if (!(loginSuccess)) {
			String loginReplyString = ftp.getReplyString();
			System.out.println("Ftp Login Failed: "+loginReplyString);
			throw new FTPLoginException("Login Failed to FTP Server");
		}
		if (fileType.equalsIgnoreCase("vb")) {
			ftp.setFileType(FTP.BINARY_FILE_TYPE);
			ftp.doCommandAsStrings("SITE", "RDW");
			ftp.doCommandAsStrings("SITE", "RECFM=VB");
			if (ftpTapeOptions != null) {
				ftp.doCommandAsStrings("SITE", "READTAPEFormat="+ftpTapeOptions.trim());
			} else {
				ftp.doCommandAsStrings("SITE", "READTAPEFormat=V");
			}
			ftp.doCommandAsStrings("SITE", "TAPEREADSTREAM");
			ftp.doCommandAsStrings("type", "E");
			ftp.doCommandAsStrings("mode", "S");
			ftp.setFileType(FTP.BINARY_FILE_TYPE);
		}
		if (fileType.equalsIgnoreCase("fb")) {
			ftp.setFileType(FTP.BINARY_FILE_TYPE);
			ftp.doCommandAsStrings("SITE", "RECFM=FB");
			if (ftpTypeOpts != null) {
				String ftpTypeOptsOut = ftpTypeOpts.replace(",", " ");
				ftp.doCommandAsStrings("SITE", "RECFM=FB " + ftpTypeOptsOut);
			}
			if (ftpTapeOptions != null) {
				ftp.doCommandAsStrings("SITE", "READTAPEFormat="+ftpTapeOptions.trim());
			} else {
				ftp.doCommandAsStrings("SITE", "READTAPEFormat=F");
			}
			ftp.doCommandAsStrings("SITE", "TAPEREADSTREAM");
			ftp.doCommandAsStrings("type", "E");
			ftp.doCommandAsStrings("mode", "S");
			ftp.setFileType(FTP.BINARY_FILE_TYPE);
		}
		if (fileType.equalsIgnoreCase("zascii")) {
			ftp.setFileType(FTP.ASCII_FILE_TYPE);
			ftp.doCommandAsStrings("SITE", "RECFM=FB");
			if (ftpTapeOptions != null) {
				ftp.doCommandAsStrings("SITE", "READTAPEFormat="+ftpTapeOptions.trim());
			} else {
				ftp.doCommandAsStrings("SITE", "READTAPEFormat=F");
			}
			ftp.doCommandAsStrings("SITE", "TAPEREADSTREAM");
			ftp.doCommandAsStrings("type", "E");
			ftp.doCommandAsStrings("mode", "S");
			ftp.setFileType(FTP.ASCII_FILE_TYPE);
		}
		if (fileType.equalsIgnoreCase("zbinary")) {
			ftp.setFileType(FTP.BINARY_FILE_TYPE);
			ftp.doCommandAsStrings("SITE", "RECFM=FB");
			if (ftpTapeOptions != null) {
				ftp.doCommandAsStrings("SITE", "READTAPEFormat="+ftpTapeOptions.trim());
			} else {
				ftp.doCommandAsStrings("SITE", "READTAPEFormat=F");
			}
			ftp.doCommandAsStrings("SITE", "TAPEREADSTREAM");
			ftp.doCommandAsStrings("type", "E");
			ftp.doCommandAsStrings("mode", "S");
			ftp.setFileType(FTP.BINARY_FILE_TYPE);
		}
		ftp.enterLocalPassiveMode();
		return ftp;
	}

	public ArrayList<String> listFiles(String remoteFilePath) {

		ArrayList<String> ftpFileLst = new ArrayList<String>();
		try {
			ftp.cwd(remoteFilePath);
			boolean workingDirChanged = ftp.changeWorkingDirectory(remoteFilePath);
			int returnCode = ftp.getReplyCode();
			String replyString = ftp.getReplyString();
			System.out.println("FTP Change Working Directory Reply Code: "+returnCode);
			if (workingDirChanged) {
				System.out.println("Current Working Direcotory: "+ftp.printWorkingDirectory());
			} else {
				System.out.println("Working Directory does not exist");
				System.out.println("FTP Working Directory Reply: "+replyString);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			System.out.println("FileNames: " + ftp.listNames());
			int returnCode = ftp.getReplyCode();
			System.out.println("FTP listNames in WorkingDirectory: "+returnCode);
			String ftpFile[] = ftp.listNames(fileName);
			returnCode = ftp.getReplyCode();
			String replyString = ftp.getReplyString();
			System.out.println("FTP listNames ("+fileName+") in WorkingDirectory: "+returnCode);
			if (ftpFile != null) {
				if (ftpFile.length > 0) {
					System.out.println("Files in the DataSet: " + ftpFile.length);
					for (String f : ftpFile) {
						System.out.println("FileName: " + f.toString());
						ftpFileLst.add(f.toString());
					}
				}
			} else {
				System.out.println("No Files Available for Download");
				System.out.println("FTP List Files Reply: "+replyString);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ftpFileLst;
	}
}
