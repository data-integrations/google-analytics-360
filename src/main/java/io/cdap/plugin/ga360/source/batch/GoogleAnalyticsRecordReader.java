/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.ga360.source.batch;

import com.google.api.services.analyticsreporting.v4.model.GetReportsRequest;
import com.google.api.services.analyticsreporting.v4.model.GetReportsResponse;
import com.google.api.services.analyticsreporting.v4.model.Report;
import com.google.api.services.analyticsreporting.v4.model.ReportRequest;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.ga360.source.common.requests.AnalyticsReportingInitializer;
import io.cdap.plugin.ga360.source.common.requests.ReportsRequestFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * RecordReader implementation, which reads {@link Report} instances from Google Analytics reporting API using
 * google-api-services-analyticsreporting.
 */
public class GoogleAnalyticsRecordReader extends RecordReader<NullWritable, Report> {

  private static final Gson gson = new GsonBuilder().create();

  private GoogleAnalyticsBatchSourceConfig gaConfig;
  private Iterator<Report> reportIterator;
  private Report currentReport;
  private String nextPageToken;


  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleAnalyticsFormatProvider.PROPERTY_CONFIG_JSON);
    gaConfig = gson.fromJson(configJson, GoogleAnalyticsBatchSourceConfig.class);
    reportIterator = requestNextReports();
  }

  private Iterator<Report> requestNextReports() {
    try {
      ReportRequest reportRequest = ReportsRequestFactory.createRequest(gaConfig);
      reportRequest.setPageToken(nextPageToken);

      // Create the GetReportsRequest object.
      List<ReportRequest> reportRequests = Collections.singletonList(reportRequest);
      GetReportsRequest getReport = new GetReportsRequest()
        .setReportRequests(reportRequests);

      // Call the batchGet method.
      GetReportsResponse response = AnalyticsReportingInitializer.initializeAnalyticsReporting()
        .reports()
        .batchGet(getReport)
        .setAccessToken(gaConfig.getAuthorizationToken())
        .execute();

      return response.getReports().iterator();
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  @Override
  public boolean nextKeyValue() {
    if (!reportIterator.hasNext()) {
      nextPageToken = currentReport.getNextPageToken();
      if (!Strings.isNullOrEmpty(nextPageToken)) {
        reportIterator = requestNextReports();
        return nextKeyValue();
      }
      return false;
    } else {
      currentReport = reportIterator.next();
      return true;
    }
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public Report getCurrentValue() {
    return currentReport;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() {

  }
}
