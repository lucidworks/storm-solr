package com.lucidworks.storm.utils;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;

public class SolrSecurity {

  private static Logger log = Logger.getLogger(SolrSecurity.class);

  private String solrJaasFile;
  private String solrJaasAppName;

  public void setConfigigurer() {
    if (solrJaasFile != null && !solrJaasFile.isEmpty()) {
      System.setProperty("java.security.auth.login.config", solrJaasFile);
      if (solrJaasAppName != null) {
        System.setProperty("solr.kerberos.jaas.appname", solrJaasAppName);
      }
      HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
    }
  }

  public String getSolrJaasFile() {
    return solrJaasFile;
  }

  public void setSolrJaasFile(String solrJaasFile) {
    this.solrJaasFile = solrJaasFile;
  }

  public String getSolrJaasAppName() {
    return solrJaasAppName;
  }

  public void setSolrJaasAppName(String solrJaasAppName) {
    this.solrJaasAppName = solrJaasAppName;
  }
}

