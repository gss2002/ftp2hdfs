package org.apache.hadoop.ftp;



import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
 
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.parser.MVSFTPEntryParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
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
    static String downloadFile = null;
	static UserGroupInformation ugi = null;


    
    String ftpHost = null;
 
    public static void downloadFile0(String remoteFilePath, String hdfsPath) {
        FileOutputStream fos;
		try {
			//fos = new FileOutputStream(localFilePath);
            //this.ftp.retrieveFile(remoteFilePath, fos);
			
			InputStream in = null;
			//BufferedOutputStream out = null;

            // APPROACH #2: using InputStream retrieveFileStream(String)
            String remoteFile = remoteFilePath;
            //String downloadFile = localFilePath;
            

            Path path = new Path(hdfsPath);
            if (fileSystem.exists(path)) {
                    System.out.println("File " + hdfsPath + " already exists");
                    return;
            }

            // Create a new file and write data to it.
            //FSDataOutputStream out = fileSystem.create(path);
            //InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));

            
            
           // OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
            FSDataOutputStream out = fileSystem.create(path);

            try {
                byte[] buf = new byte[1048576];
                int bytes_read = 0;
                in = ftp.retrieveFileStream(remoteFile);

               
                //out = new BufferedOutputStream(new FileOutputStream(new File(downloadFile)));
                //out = new BufferedOutputStream(new PipedOutputStream());

                do {
                    bytes_read = in.read(buf, 0, buf.length);
                    //System.out.println("Just Read: " + bytes_read + " bytes");

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
	 if (UserGroupInformation.isSecurityEnabled()) {
	    try {
	    	if (setKrb == true) {
	            System.setProperty("sun.security.krb5.debug", "false");
	            UserGroupInformation.setConfiguration(conf);
	            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabupn, keytab);
	            UserGroupInformation.setLoginUser(ugi);
	    	} else {	    	
	    		ugi = UserGroupInformation.getCurrentUser();
	    	}
	    	//ugi = UserGroupInformation.getBestUGI(ticketCachePath, user)
	    	//UserGroupInformation.getUGIFromTicketCache("/tmp/krb5cc_0", args[1]));
	    	System.out.println("UserId for Hadoop: "+ugi.getUserName());
	    } catch (IOException e3) {
	    	// TODO Auto-generated catch block
	    	e3.printStackTrace();
	    	System.out.println("Exception Getting Credentials Exiting!");
	    	System.exit(1);
	    }
	    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
	    //ugi.setConfiguration(conf);

	    getFS();	
      
        try {
        	System.out.println("HasCredentials: "+ugi.hasKerberosCredentials());
        	System.out.println("ShortName: "+ugi.getShortUserName());
        	System.out.println("RealUser: "+ugi.getRealUser());
        	System.out.println("Login KeyTab Based: "+UserGroupInformation.isLoginKeytabBased());


            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {
                	System.out.println("Downloading: "+downloadFile+ " to HDFS: "+hdfsPath+"/"+downloadFile.replaceAll("\'", ""));
                	downloadFile0(downloadFile, hdfsPath+"/"+downloadFile.replaceAll("\'", ""));
                    return null;
                }
            });
        
            //ftpDownloader.downloadFile("\'GSS.RESOLVED.ENR.MNTH.M201406.G0001V00\'", "/user/username/claim/");
            //ftpDownloader.listFiles("\'GSS.RESOLVED.ENR.MNTH'");

        } catch (Exception e) {
        	e.printStackTrace();
        }
	 } else {
		 getFS();
     	 System.out.println("Downloading: "+downloadFile+ " to HDFS: "+hdfsPath+"/"+downloadFile.replaceAll("\'", ""));
     	 downloadFile0(downloadFile, hdfsPath+"/"+downloadFile.replaceAll("\'", ""));
		 
	 }
        
	}

  
 
 
    public static void main(String[] args) {
    	ArrayList<String> ftpFileLst = new ArrayList<String>();
    	
    	
    	Configuration conf = new Configuration();
	    String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			e4.printStackTrace();
		}
		
	    Options options = new Options();
	    options.addOption("ftp_host", true, "FTP Hostname --ftp_host (zhost.example.com)");
	    options.addOption("transfer_type", true, "FTP TransferType --transfer_type requires (vb,fb,ascii,binary,zascii,zbinary)");
	    options.addOption("transfer_type_opts", true, "FTP TransferType Options --transfer_type_opts FIXrecfm=80,LRECL=80,BLKSIZE=27920");
	    options.addOption("ftp_folder", true, "FTP Server Folder --ftp_folder /foldername/ ");
	    options.addOption("ftp_pds", true, "FTP Partitioned Data set Z/os Folder --ftp_pds GSS.RESOLVED.ENR.MNTH.M201209 ");
	    options.addOption("ftp_userid", true, "FTP Userid --ftp_userid userid");
	    options.addOption("ftp_pwd", true, "FTP Password --ftp_pwd password");
	    options.addOption("hdfs_outdir", true, "HDFS Output Dir --hdfs_outdir");
	    options.addOption("ftp_filename", true, "FTPFileName --filename G* or --filename DATA2011");
	    options.addOption("krb_keytab", true, "KeyTab File to Connect to HDFS --krb_keytab $HOME/user.keytab");
	    options.addOption("krb_upn", true, "Kerberos Princpial for Keytab to Connect to HDFS --krb_upn user@EXAMP.EXAMPLE.COM");
	    options.addOption("help", false, "Display help");
	    CommandLineParser parser = new FTPParser();
	    CommandLine cmd = null;
		try {
			cmd = parser.parse( options, otherArgs);
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	    
	    
	    if (cmd.hasOption("ftp_host") && cmd.hasOption("hdfs_outdir") && cmd.hasOption("ftp_filename") && cmd.hasOption("ftp_userid") && cmd.hasOption("ftp_pwd") && cmd.hasOption("transfer_type")) {
	    	ftphost = cmd.getOptionValue("ftp_host");
	    	if (cmd.hasOption("ftp_userid") && cmd.hasOption("ftp_pwd")) {
	    		userId = cmd.getOptionValue("ftp_userid");
	    		pwd = cmd.getOptionValue("ftp_pwd");
	    	} else {
	    		System.exit(0);
	    	}
	    	if (cmd.hasOption("ftp_folder") || cmd.hasOption("ftp_pds")) {
	    		if (cmd.hasOption("transfer_type")) {
	    			fileType = cmd.getOptionValue("transfer_type");
	    			System.out.println("fileType: "+fileType);
	    			if (cmd.hasOption("ftp_pds") && (fileType.equalsIgnoreCase("vb") || fileType.equalsIgnoreCase("fb")))  {
	    				zftp=true;
	    				zpds=true;
	    				ftptype="binary";
	    				ftppds=cmd.getOptionValue("ftp_pds");
	    				if (cmd.hasOption("transfer_type_opts")) {
	    					ftptypeopts = cmd.getOptionValue("transfer_type_opts");
	    				}
	    			}
	    			if (((cmd.hasOption("ftp_folder") || cmd.hasOption("ftp_pds")) && (fileType.equalsIgnoreCase("zascii") || fileType.equalsIgnoreCase("zbinary"))))  {
	    				zftp=true;
	    				if (fileType.equalsIgnoreCase("zascii")) {
		    				zftp=true;
	    					ftptype="zascii";
	    				}
	    				if (fileType.equalsIgnoreCase("zbinary")) {
		    				zftp=true;
	    					ftptype="zbinary";
	    				}
	    				if (cmd.hasOption("ftp_pds"))
	    				{
		    				zpds=true;
		    				ftppds=cmd.getOptionValue("ftp_pds");
	    				}
	    				if (cmd.hasOption("ftp_folder")) {
	    					ftpfolder=cmd.getOptionValue("ftp_folder");
	    				}
	    			}
	    			if (fileType.equalsIgnoreCase("ascii") || fileType.equalsIgnoreCase("binary" )) 
	    			{
	    				zftp=false;
	    				ftpfolder=cmd.getOptionValue("ftp_folder");
	    				if (fileType.equalsIgnoreCase("ascii")) {
	    					ftptype="ascii";
	    				}
	    				if (fileType.equalsIgnoreCase("binary")) {
	    					ftptype="binary";
	    				}
	    			} 		
	    		} else {
		    		System.out.println("Missing FTP Transfer Type");
		    		System.exit(0);	    			
	    		}
	    	} else {
	    		System.out.println("Missing FTP Folder or Partitioned Data Set");
	    		System.exit(0);
	    	}
	    	if (cmd.hasOption("ftp_filename")) {
	    		ftpFileName = cmd.getOptionValue("ftp_filename");
	    	}
		    if (cmd.hasOption("hdfs_outdir")) {
		    	hdfsPath = cmd.getOptionValue("hdfs_outdir");
		    }
		    if (cmd.hasOption("help")) {
				String header = "FTP to HDFS Client";
			  	String footer = "\nPlease report issues at https://github.com/gss2002";	 	 
		    	 HelpFormatter formatter = new HelpFormatter();
		    	 formatter.printHelp("get", header, options, footer, true);
		    	 System.exit(0);
		    }
	    } else {
			String header = "FTP to HDFS Client";
		  	String footer = "\nPlease report issues at https://github.com/gss2002";	 	
	    	 HelpFormatter formatter = new HelpFormatter();
	    	 formatter.printHelp("get", header, options, footer, true);
	    	 System.exit(0);
	    }

    	
	    if (cmd.hasOption("krb_keytab") && cmd.hasOption("krb_upn")) {
	    	setKrb=true;
	    	keytab = cmd.getOptionValue("krb_keytab");
	    	keytabupn = cmd.getOptionValue("krb_upn");
	    	File keytabFile = new File(keytab);
	    	if (keytabFile.exists()) {
	    		
	    		if (!(keytabFile.canRead())) { 
		    		System.out.println("KeyTab  exists but cannot read it - exiting");
		    		System.exit(1);
	    		}
	    	} else {
	    		System.out.println("KeyTab doesn't exist  - exiting");
	    		System.exit(1);
	    	}
           // System.setProperty("java.security.auth.login.config", "./client_jaas.conf");

	    }
    	
    	setConfig();
		try {
			if ((zftp) && (zpds)) {
				ftpDownloader = new ZCopyBookFTPClient(ftphost, userId, pwd, fileType, ftpFileName, ftptypeopts);
				ftp = ftpDownloader.getFtpClient();
				System.out.println("PDS: "+ftppds);
				ftpFileLst = ftpDownloader.listFiles("\'"+ftppds+"\'");
		    	for (int i = 0; i < ftpFileLst.size(); i++) {
		    	    String fileNameOut=ftpFileLst.get(i);
		    	    fileName = "\'"+ftppds+"."+fileNameOut+"\'";
					System.out.println("PDS FileName: "+fileName);

		    	    downloadFile = fileName;
		    	    downloadFile();
		    	}
			}
		} catch (Exception e) {
           	e.printStackTrace();
        }
            

        System.out.println("FTP File downloaded successfully");
        try {
			ftp.disconnect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
    
    
    
    
    
    public static void setConfig(){
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
    
	
 
}
