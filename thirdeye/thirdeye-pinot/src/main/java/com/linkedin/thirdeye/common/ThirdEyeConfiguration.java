package com.linkedin.thirdeye.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import io.dropwizard.Configuration;

public class ThirdEyeConfiguration extends Configuration {
  /**
   * Root directory for all other configuration
   */
  private String rootDir = "";
  private String dataSources = "data-sources/data-sources-config.yml";

  private List<String> whitelistDatasets = new ArrayList<>();

  private String dashboardHost;
  private SmtpConfiguration smtpConfiguration;

  @JsonProperty("swagger")
  public SwaggerBundleConfiguration swaggerBundleConfiguration;

  private String phantomJsPath = "";
  private String failureFromAddress;
  private String failureToAddress;

  /**
   * allow cross request for local development
   */
  private boolean cors = false;

  /**
   * Convert relative path to absolute URL
   *
   * Supported cases:
   * <pre>
   *   file:/....myDir/data-sources-config.yml
   *   myDir/data-sources-config.yml
   * </pre>
   *
   * @return the url of the data source
   */
  public URL getDataSourcesAsUrl() {
    try {
      return new URL(this.dataSources);
    } catch (MalformedURLException ignore) {
      // ignore
    }

    try {
      URL rootUrl = new URL(String.format("file:%s/", this.rootDir));
      return new URL(rootUrl, this.dataSources);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(String.format("Could not parse relative path for rootDir '%s' and datSources '%s'", this.rootDir, this.dataSources));
    }
  }

  public String getDataSources() {
    return dataSources;
  }

  public String getRootDir() {
    return rootDir;
  }

  public void setRootDir(String rootDir) {
    this.rootDir = rootDir;
  }

  public boolean isCors() {
    return cors;
  }

  public void setCors(boolean cors) {
    this.cors = cors;
  }

  public List<String> getWhitelistDatasets() {
    return whitelistDatasets;
  }

  public void setWhitelistDatasets(List<String> whitelistDatasets) {
    this.whitelistDatasets = whitelistDatasets;
  }

  public String getFunctionConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/functions.properties";
  }

  //alertFilter.properties format: {alert filter type} = {path to alert filter implementation}
  public String getAlertFilterConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertFilter.properties";
  }

  //alertFilterAutotune.properties format: {auto tune type} = {path to auto tune implementation}
  public String getFilterAutotuneConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertFilterAutotune.properties";
  }

  public String getAlertGroupRecipientProviderConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertGroupRecipientProvider.properties";
  }

  public String getAnomalyClassifierConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/anomalyClassifier.properties";
  }

  public String getCalendarApiKeyPath(){
    return getRootDir() + "/holiday-loader-key.json";
  }

  public void setSmtpConfiguration(SmtpConfiguration smtpConfiguration) {
    this.smtpConfiguration = smtpConfiguration;
  }

  public SmtpConfiguration getSmtpConfiguration(){
    return this.smtpConfiguration;
  }

  public String getPhantomJsPath() {
    return phantomJsPath;
  }

  public void setPhantomJsPath(String phantomJsPath) {
    this.phantomJsPath = phantomJsPath;
  }

  public String getDashboardHost() {
    return dashboardHost;
  }

  public void setDashboardHost(String dashboardHost) {
    this.dashboardHost = dashboardHost;
  }

  public String getFailureFromAddress() {
    return failureFromAddress;
  }

  public void setFailureFromAddress(String failureFromAddress) {
    this.failureFromAddress = failureFromAddress;
  }

  public String getFailureToAddress() {
    return failureToAddress;
  }

  public void setFailureToAddress(String failureToAddress) {
    this.failureToAddress = failureToAddress;
  }

  public void setDataSources(String dataSources) {
    this.dataSources = dataSources;
  }

}
