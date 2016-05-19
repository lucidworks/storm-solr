package com.lucidworks.storm.fusion;

import com.google.common.collect.Sets;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FusionKrb5HttpClientConfigurer extends HttpClientConfigurer {

  private static final Logger logger = LoggerFactory.getLogger(FusionKrb5HttpClientConfigurer.class);
  private String fusionPrincipal = null;

  public static final String LOGIN_CONFIG_PROP = "java.security.auth.login.config";
  public static final String LOGIN_APP_NAME = "fusion.jaas.appname";

  private Configuration jaasConfig = null;

  public Configuration getJaasConfig() {
    return jaasConfig;
  }

  public static synchronized CloseableHttpClient createClient(String fusionPrincipal) {
    if (logger.isDebugEnabled()) {
      System.setProperty("sun.security.krb5.debug", "true");
    }
    if (fusionPrincipal == null) {
      logger.error("fusion.user (principal) must be set in order to use kerberos!");
    }
    HttpClientUtil.setConfigurer(new FusionKrb5HttpClientConfigurer(fusionPrincipal));
    CloseableHttpClient httpClient = HttpClientUtil.createClient(null);
    HttpClientUtil.setMaxConnections(httpClient, 500);
    HttpClientUtil.setMaxConnectionsPerHost(httpClient, 100);
    return httpClient;
  }

  private HttpRequestInterceptor bufferedEntityInterceptor = new HttpRequestInterceptor() {
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      if (request instanceof HttpEntityEnclosingRequest) {
        HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;
        HttpEntity requestEntity = enclosingRequest.getEntity();
        enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
      }
    }
  };

  public FusionKrb5HttpClientConfigurer(String fusionPrincipal) {
    this.fusionPrincipal = fusionPrincipal;
    jaasConfig = new FusionKrb5HttpClientConfigurer.FusionJaasConfiguration(fusionPrincipal);
  }

  public void configure(DefaultHttpClient httpClient, SolrParams config) {
    super.configure(httpClient, config);
    if (System.getProperty(LOGIN_CONFIG_PROP) != null) {
      String configValue = System.getProperty(LOGIN_CONFIG_PROP);
      if (configValue != null) {
        logger.debug("Setting up kerberos auth with config: " + configValue);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        if (fusionPrincipal != null) {
          Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(fusionPrincipal)),
              Collections.emptySet(), Collections.emptySet());
          LoginContext loginContext;
          try {
            loginContext = new LoginContext("", subject, null, jaasConfig);
            loginContext.login();
            logger.debug("Successful Fusion Login with principal: " + fusionPrincipal);
          } catch (LoginException e) {
            String errorMessage = "Unsuccessful Fusion Login with principal: " + fusionPrincipal;
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
          }
        }

        Configuration.setConfiguration(jaasConfig);
        httpClient.getAuthSchemes().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, false));
        Credentials useJaasCreds = new Credentials() {
          public String getPassword() {
            return null;
          }

          public Principal getUserPrincipal() {
            return null;
          }
        };
        httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, useJaasCreds);
        httpClient.addRequestInterceptor(this.bufferedEntityInterceptor);
      } else {
        httpClient.getCredentialsProvider().clear();
      }
    }
  }


  private static class FusionJaasConfiguration extends Configuration {
    private Configuration baseConfig;
    private String fusionPrincipal;
    private AppConfigurationEntry[] globalAppConfigurationEntry;

    public FusionJaasConfiguration(String fusionPrincipal) {
      this.fusionPrincipal = fusionPrincipal;
      try {
        this.baseConfig = Configuration.getConfiguration();
      } catch (SecurityException var2) {
        this.baseConfig = null;
      }
      if (this.baseConfig != null) {
        String clientAppName = System.getProperty(LOGIN_APP_NAME, "Client");
        this.globalAppConfigurationEntry = this.baseConfig.getAppConfigurationEntry(clientAppName);
      }
    }

    private AppConfigurationEntry overwriteOptions(AppConfigurationEntry app) {
      Map<String, ?> options = app.getOptions();
      AppConfigurationEntry.LoginModuleControlFlag flag = app.getControlFlag();
      String loginModule = app.getLoginModuleName();

      // Get the current principal for debug
      Object principal = options.get("principal");
      if (principal != null) {
        FusionKrb5HttpClientConfigurer.logger.debug(principal.getClass() + "not using" + principal);
      }

      // Overwriting options
      Map<String, Object> overwriteOptions = new HashMap<String, Object>(options);
      overwriteOptions.put("principal", fusionPrincipal);
      overwriteOptions.put("doNotPrompt", "true");

      return new AppConfigurationEntry(loginModule, flag, overwriteOptions);
    }

    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (this.baseConfig == null) {
        return null;
      } else {
        FusionKrb5HttpClientConfigurer.logger.debug("Login prop: " + System.getProperty(LOGIN_CONFIG_PROP));
        if (fusionPrincipal == null) {
          FusionKrb5HttpClientConfigurer.logger.debug("fusionPrincipal is null using principal from JAAS file.");
          return globalAppConfigurationEntry;
        }
        if (globalAppConfigurationEntry == null) {
          return null;
        }

        // Must be only one Entry, if more use the first one.
        return new AppConfigurationEntry[]{overwriteOptions(globalAppConfigurationEntry[0])};
      }
    }
  }

}
