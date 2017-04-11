package org.apache.hadoop.ftp.password;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

public final class FTP2HDFSCredentialProvider {
	
	private static final Log LOG = LogFactory.getLog(FTP2HDFSCredentialProvider.class);

	private static final FTP2HDFSCredentialProvider CRED_PROVIDER = new FTP2HDFSCredentialProvider();

	public FTP2HDFSCredentialProvider() {
		//
	}

	public static FTP2HDFSCredentialProvider getInstance() {
		return CRED_PROVIDER;
	}

	public char[] getCredentialString(String url, String alias, Configuration conf) {
		List<CredentialProvider> providers = getCredentialProviders(url, conf);

		if (providers != null) {
			for (CredentialProvider provider : providers) {
				try {
					CredentialProvider.CredentialEntry credEntry = provider.getCredentialEntry(alias);

					if (credEntry != null) {
						return credEntry.getCredential();
					}
				} catch(Exception ie) {
					LOG.error("Unable to get the Credential Provider from the Configuration", ie);	
				}
			}
		}
		return null;
	}

	List<CredentialProvider> getCredentialProviders(String url, Configuration conf) {
		try {
			if (conf == null) {
				conf = new Configuration();
			}

			conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, url);

			return CredentialProviderFactory.getProviders(conf);
		} catch(Exception ie) {
			LOG.error("Unable to get the Credential Provider from the Configuration", ie);
		}
		return null;
	}
	
}