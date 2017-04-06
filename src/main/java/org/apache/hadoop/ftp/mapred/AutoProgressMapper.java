package org.apache.hadoop.ftp.mapred;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Identity mapper that continuously reports progress via a background thread.
 */
public class AutoProgressMapper<KEYIN, VALIN, KEYOUT, VALOUT>
    extends FTP2HDFSMapper<KEYIN, VALIN, KEYOUT, VALOUT> {

  public static final Log LOG = LogFactory.getLog(
      AutoProgressMapper.class.getName());

  public static final String MAX_PROGRESS_PERIOD_KEY =
      "ftp2hdfs.mapred.auto.progress.max";
  public static final String SLEEP_INTERVAL_KEY =
      "ftp2hdfs.mapred.auto.progress.sleep";
  public static final String REPORT_INTERVAL_KEY =
      "ftp2hdfs.mapred.auto.progress.report";

  // Sleep for 10 seconds at a time.
  public static final int DEFAULT_SLEEP_INTERVAL = 10000;

  // Report progress every 30 seconds.
  public static final int DEFAULT_REPORT_INTERVAL = 30000;

  // Disable max progress, by default.
  public static final int DEFAULT_MAX_PROGRESS = 0;

  // map() method intentionally omitted; Mapper.map() is the identity mapper.

  /**
   * Run the mapping process for this task, wrapped in an auto-progress system.
   */
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    ProgressThread thread = new ProgressThread(context, LOG);

    try {
      thread.setDaemon(true);
      thread.start();

      // use default run() method to actually drive the mapping.
      super.run(context);
    } finally {
      // Tell the progress thread to exit..
      LOG.debug("Instructing auto-progress thread to quit.");
      thread.signalShutdown();
      try {
        // And wait for that to happen.
        LOG.debug("Waiting for progress thread shutdown...");
        thread.join();
        LOG.debug("Progress thread shutdown detected.");
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted when waiting on auto-progress thread: "
            + ie.toString(), ie);
      }
    }
  }
}

