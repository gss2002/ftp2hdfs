package org.apache.hadoop.ftp.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FTP2HDFSInputSplit extends InputSplit implements Writable {
	private String currentDataset;

	public FTP2HDFSInputSplit() {
	}

	public FTP2HDFSInputSplit(String split) {
		this.currentDataset = split;
	}

	public String getCurrentDataset() {
		return currentDataset;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0]; // No locations
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(this.currentDataset);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.currentDataset = dataInput.readUTF();

	}
}
