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
package io.cdap.plugin.ga360.source.common;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting;
import io.cdap.plugin.ga360.source.batch.GoogleAnalyticsBatchSource;
import io.cdap.plugin.ga360.source.streaming.GoogleAnalyticsStreamingSource;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Initializes an Analytics Reporting API V4 service.
 */
public class AnalyticsReportingInitializer {

  private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

  /**
   * Initializes an Analytics Reporting API V4 service object.
   *
   * @return An authorized Analytics Reporting API V4 service object.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static AnalyticsReporting initializeAnalyticsReporting() throws GeneralSecurityException, IOException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

    // Construct the Analytics Reporting service object.
    return new AnalyticsReporting.Builder(httpTransport, JSON_FACTORY, null)
      .setApplicationName(GoogleAnalyticsBatchSource.NAME)
      .build();
  }

  /**
   * Initializes an Analytics API V3 service object.
   *
   * @return An authorized Analytics API V3 service object.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static Analytics initializeAnalytics() throws GeneralSecurityException, IOException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

    // Construct the Analytics Reporting service object.
    return new Analytics.Builder(httpTransport, JSON_FACTORY, null)
      .setApplicationName(GoogleAnalyticsStreamingSource.NAME)
      .build();
  }
}
