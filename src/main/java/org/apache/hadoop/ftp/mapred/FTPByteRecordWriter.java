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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ftp.ZCopyBookFTPClient;
import org.apache.hadoop.ftp.password.FTP2HDFSCredentialProvider;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FTPByteRecordWriter extends RecordWriter<Text, NullWritable> {
	private DataOutputStream out;
	private ZCopyBookFTPClient ftpDownloader;
	private FTPClient ftp = null;
	private InputStream in = null;
	private static final Log LOG = LogFactory.getLog(FTP2HDFSOutputFormat.class.getName());
	private Configuration conf;
	private Path path;
	private FileSystem fs;

	public FTPByteRecordWriter(Path path, Configuration confIn, TaskAttemptContext taskAttemptContext) {
		String pwd = confIn.get(Constants.FTP2HDFS_PASS);
		String pwdAlias = confIn.get(Constants.FTP2HDFS_PASS_ALIAS);
		if (pwdAlias != null) {
			LOG.info("Cred Provider: " + confIn.get("hadoop.security.credential.provider.path"));
			LOG.info("Cred Alias: " + confIn.get(Constants.FTP2HDFS_PASS_ALIAS));

			FTP2HDFSCredentialProvider creds = new FTP2HDFSCredentialProvider();
			pwd = new String(creds.getCredentialString(confIn.get("hadoop.security.credential.provider.path"),
					confIn.get(Constants.FTP2HDFS_PASS_ALIAS), confIn));
		}
		LOG.info("FTP2HDFS_HOST: " + confIn.get(Constants.FTP2HDFS_HOST));
		LOG.info("FTP2HDFS_TRANSFERTYPE: " + confIn.get(Constants.FTP2HDFS_TRANSFERTYPE));
		LOG.info("FTP2HDFS_TRANSFERTYPE_OPTS: " + confIn.get(Constants.FTP2HDFS_TRANSFERTYPE_OPTS));

		this.ftpDownloader = new ZCopyBookFTPClient(confIn.get(Constants.FTP2HDFS_HOST),
				confIn.get(Constants.FTP2HDFS_USERID), pwd, confIn.get(Constants.FTP2HDFS_TRANSFERTYPE), null,
				confIn.get(Constants.FTP2HDFS_TRANSFERTYPE_OPTS));
		try {
			this.ftp = ftpDownloader.getFtpClient();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.conf = confIn;
		this.path = path;
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException {

		String remoteFile = key.toString();
		key = null;
		value = null;

		String parentPath = path.getParent().toString();
		String fileString = parentPath + "/" + remoteFile.replaceAll("\'", "");
		Path file = new Path(fileString);
		this.fs = file.getFileSystem(conf);
		this.out = fs.create(file, false);

		LOG.info("FTP CLient Downloading" + remoteFile);

		try {
			byte[] buf = new byte[1048576];
			int bytes_read = 0;
			LOG.info("FTP CLient is Connected" + this.ftp.isConnected());
			this.in = this.ftp.retrieveFileStream(remoteFile);

			do {
				bytes_read = this.in.read(buf, 0, buf.length);

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
		boolean success = this.ftp.completePendingCommand();
		if (success) {
			LOG.info("File " + remoteFile + " has been downloaded successfully.");
		}
	}

	@Override
	public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		this.out.close();
		this.in.close();
		this.ftp.disconnect();
	}
}
