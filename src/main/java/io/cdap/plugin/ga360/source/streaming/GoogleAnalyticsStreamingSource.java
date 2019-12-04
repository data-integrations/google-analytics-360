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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Plugin reads data from Google Analytics Reporting API.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(GoogleAnalyticsStreamingSource.NAME)
@Description(GoogleAnalyticsStreamingSource.DESCRIPTION)
public class GoogleAnalyticsStreamingSource extends StreamingSource<StructuredRecord> {

  public static final String NAME = "GoogleAnalyticsStreamingSource";
  public static final String DESCRIPTION = "Reads data from Google Analytics Reporting API.";

  private GoogleAnalyticsStreamingSourceConfig config;

  public GoogleAnalyticsStreamingSource(GoogleAnalyticsStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    IdUtils.validateReferenceName(config.referenceName, failureCollector);
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
    validateConfiguration(failureCollector);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) {
    return streamingContext.getSparkStreamingContext().receiverStream(new GoogleAnalyticsReceiver(config));
  }

  private void validateConfiguration(FailureCollector failureCollector) {
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
  }
}
