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
package io.cdap.plugin.ga360.source.streaming;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.analytics.Analytics;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.ga360.source.common.BaseSourceConfig;
import io.cdap.plugin.ga360.source.common.requests.RealTimeRequestFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Provides all required configuration for reading Google Analytics data from Streaming Source.
 */
public class GoogleAnalyticsStreamingSourceConfig extends BaseSourceConfig {

  public static final String POLL_INTERVAL = "pollInterval";

  @Name(POLL_INTERVAL)
  @Description("The amount of time to wait between each poll in minutes.")
  private Long pollInterval;

  public GoogleAnalyticsStreamingSourceConfig(String referenceName) {
    super(referenceName);
  }

  public Long getPollInterval() {
    return pollInterval;
  }

  @Override
  protected void validateMetricAndDimensionCombination(FailureCollector failureCollector) {
    super.validateMetricAndDimensionCombination(failureCollector);
    try {
      Analytics.Data.Realtime.Get request = RealTimeRequestFactory.createRequest(this);
      request.execute();
    } catch (GoogleJsonResponseException e) {
      String message = e.getDetails().getMessage();
      if (Objects.nonNull(message)) {
        String[] messages = message.split("\n");
        if (messages.length > 1) {
          failureCollector.addFailure(messages[0], messages[1])
            .withConfigProperty(METRICS)
            .withConfigProperty(DIMENSIONS);
        }
      }
    } catch (IOException e) {
      failureCollector.addFailure(e.getMessage(), "Check your configuration");
    }
  }
}
