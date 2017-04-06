package org.apache.hadoop.ftp.mapred;


import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;



public class FTP2HDFSCoreMapper extends AutoProgressMapper<Text, NullWritable, Text, NullWritable> {

	public FTP2HDFSCoreMapper() {
	  }

	  protected void setup(Context context)
	      throws IOException, InterruptedException {
	    super.setup(context);


	  }
	
	
	
	protected void map(Text key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);

		
	}
	
	
}

