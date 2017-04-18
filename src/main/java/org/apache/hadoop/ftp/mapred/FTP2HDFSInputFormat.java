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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ftp.ZCopyBookFTPClient;
import org.apache.hadoop.ftp.password.FTP2HDFSCredentialProvider;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FTP2HDFSInputFormat extends FileInputFormat<Text, NullWritable> {
	private static final Log LOG = LogFactory.getLog(FTP2HDFSInputFormat.class.getName());
	private static ZCopyBookFTPClient ftpDownloader;
	private static FTPClient ftp = null;
	private static boolean zftp = false;
	private static boolean zpds = false;
	private static String ftppds = null;
	private static ArrayList<String> ftpFileLst = new ArrayList<String>();

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Configuration conf = job.getConfiguration();
		zftp = conf.getBoolean(Constants.FTP2HDFS_ZFTP, false);
		zpds = conf.getBoolean(Constants.FTP2HDFS_ZPDS, false);
		ftppds = conf.get(Constants.FTP2HDFS_PDS);
		String fileName = null;
		String pwd = conf.get(Constants.FTP2HDFS_PASS);
		String pwdAlias = conf.get(Constants.FTP2HDFS_PASS_ALIAS);
		if (pwdAlias != null) {
			FTP2HDFSCredentialProvider creds = new FTP2HDFSCredentialProvider();
			pwd = new String(creds.getCredentialString(conf.get("hadoop.security.credential.provider.path"),
					conf.get(Constants.FTP2HDFS_PASS_ALIAS), conf));
		}

		if ((zftp) && (zpds)) {
			ftpDownloader = new ZCopyBookFTPClient(conf.get(Constants.FTP2HDFS_HOST),
					conf.get(Constants.FTP2HDFS_USERID), pwd, conf.get(Constants.FTP2HDFS_TRANSFERTYPE),
					conf.get(Constants.FTP2HDFS_FILENAME), conf.get(Constants.FTP2HDFS_TRANSFERTYPE_OPTS));
			try {
				ftp = ftpDownloader.getFtpClient();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("PDS: " + ftppds);
			ftpFileLst = ftpDownloader.listFiles("\'" + ftppds + "\'");
			int count = ftpFileLst.size();
			LOG.info("PDS Splits: " + count);

			for (int i = 0; i < ftpFileLst.size(); i++) {
				LOG.info("Adding Split: " + i);
				String fileNameOut = ftpFileLst.get(i);
				fileName = "\'" + ftppds + "." + fileNameOut + "\'";
				LOG.info("PDS FileName: " + fileName);
				splits.add((InputSplit) new FTP2HDFSInputSplit(fileName));
			}
			ftp.disconnect();
		}
		return splits;
	}

	public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		RecordReader<Text, NullWritable> reader = new FTP2HDFSFileRecordReader(split, context);
		reader.initialize(split, context);
		return reader;
	}

}
