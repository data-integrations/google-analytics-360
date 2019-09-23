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
package io.cdap.plugin.ga360.source.common.requests;

import com.google.api.services.analytics.Analytics;
import io.cdap.plugin.ga360.source.common.AnalyticsReportingInitializer;
import io.cdap.plugin.ga360.source.common.BaseSourceConfig;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Creates real time request based on source configuration.
 */
public class RealTimeRequestFactory {

  /**
   * Creates realtime data get request.
   */
  public static Analytics.Data.Realtime.Get createRequest(BaseSourceConfig config) {
    try {
      // Create the comma-separated Metrics list.
      String metricsList = String.join(",", config.getMetricsList());

      // Create the comma-separated Dimensions list.
      String dimensionsList = String.join(",", config.getDimensionsList());

      // Create the Realtime.Get object.
      final Analytics.Data.Realtime.Get request = AnalyticsReportingInitializer.initializeAnalytics()
        .data()
        .realtime()
        .get(config.getViewId(), metricsList)
        .setOauthToken(config.getAuthorizationToken());

      if (!dimensionsList.isEmpty()) {
        request.setDimensions(dimensionsList);
      }

      return request;

    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }
}
