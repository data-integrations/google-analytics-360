/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.api.services.analyticsreporting.v4.model.Report;
import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.ga360.source.common.ReportTransformer;
import org.apache.hadoop.io.NullWritable;

import java.util.stream.Collectors;

/**
 * Plugin returns records from Google Analytics Reporting API V4.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(GoogleAnalyticsBatchSource.NAME)
@Description(GoogleAnalyticsBatchSource.DESCRIPTION)
public class GoogleAnalyticsBatchSource extends BatchSource<NullWritable, Report, StructuredRecord> {

  public static final String NAME = "GoogleAnalyticsBatchSource";
  public static final String DESCRIPTION = "Reads data from Google Analytics Reporting API.";

  private final GoogleAnalyticsBatchSourceConfig config;

  public GoogleAnalyticsBatchSource(GoogleAnalyticsBatchSourceConfig config) {
    this.config = config;
  }

  /**
   * Prepare Google Analytics reports.
   * @param batchSourceContext the batch source context
   */
  public void prepareRun(BatchSourceContext batchSourceContext) {
    validateConfiguration(batchSourceContext.getFailureCollector());
    LineageRecorder lineageRecorder = new LineageRecorder(batchSourceContext, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    lineageRecorder.recordRead("Read", "Reading Google Analytics reports",
                               Preconditions.checkNotNull(config.getSchema().getFields()).stream()
                                 .map(Schema.Field::getName)
                                 .collect(Collectors.toList()));

    batchSourceContext.setInput(Input.of(config.referenceName, new GoogleAnalyticsFormatProvider(config)));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    IdUtils.validateReferenceName(config.referenceName, failureCollector);
    validateConfiguration(failureCollector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void transform(KeyValue<NullWritable, Report> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(ReportTransformer.transform(input.getValue(), config.getSchema()));
  }

  private void validateConfiguration(FailureCollector failureCollector) {
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
  }
}
