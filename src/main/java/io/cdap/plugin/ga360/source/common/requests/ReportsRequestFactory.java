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

import com.google.api.services.analyticsreporting.v4.model.DateRange;
import com.google.api.services.analyticsreporting.v4.model.Dimension;
import com.google.api.services.analyticsreporting.v4.model.Metric;
import com.google.api.services.analyticsreporting.v4.model.ReportRequest;
import io.cdap.plugin.ga360.source.batch.GoogleAnalyticsConfig;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Creates request based on source configuration.
 */
public class ReportsRequestFactory {

  /**
   * Creates reports request.
   */
  public static ReportRequest createRequest(GoogleAnalyticsConfig config) {
    // Create the DateRange object.
    DateRange dateRange = new DateRange();
    dateRange.setStartDate(config.getStartDate());
    dateRange.setEndDate(config.getEndDate());

    // Create the Metrics list.
    List<String> metricsList = config.getMetricsList();
    List<Metric> metrics = metricsList.stream()
        .map(metric -> new Metric()
            .setExpression(metric))
        .collect(Collectors.toList());

    // Create the Dimensions list.
    List<String> dimensionsList = config.getDimensionsList();
    List<Dimension> dimensions = dimensionsList.stream()
        .map(dimension -> new Dimension()
            .setName(dimension))
        .collect(Collectors.toList());

    // Create the ReportRequest object.
    ReportRequest reportRequest = new ReportRequest()
        .setViewId(config.getViewId())
        .setDateRanges(Collections.singletonList(dateRange))
        .setMetrics(metrics)
        .setDimensions(dimensions);
    if (config.getSampleSize() != null) {
      reportRequest.setSamplingLevel(config.getSampleSize());
    }
    return reportRequest;
  }
}
