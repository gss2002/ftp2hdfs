package org.apache.hadoop.ftp.mapred;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FTP2HDFSOutputFormat extends FileOutputFormat<Text, NullWritable> {
	
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        String extension = "";
        Path file = getDefaultWorkFile(taskAttemptContext, extension);
	
        return new FTPByteRecordWriter(file, conf, taskAttemptContext);
    }

 
}
