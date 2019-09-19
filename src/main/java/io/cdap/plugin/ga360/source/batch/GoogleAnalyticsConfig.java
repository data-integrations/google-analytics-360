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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.ga360.source.common.BaseSourceConfig;
import io.cdap.plugin.ga360.source.common.SchemaBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Provides all required configuration for reading Google Analytics data.
 */
public class GoogleAnalyticsConfig extends BaseSourceConfig {

  public static final String START_DATE = "startDate";
  public static final String END_DATE = "endDate";
  public static final String METRICS = "metricsList";
  public static final String DIMENSIONS = "dimensionsList";
  public static final String SAMPLING_LEVEL = "sampleSize";
  private transient Schema schema = null;

  @Name(START_DATE)
  @Description("Start date for the report data")
  @Macro
  protected String startDate;

  @Name(END_DATE)
  @Description("End date for the report data")
  @Macro
  protected String endDate;

  @Name(METRICS)
  @Description("Quantitative measurements. For example, "
      + "the metric ga:users indicates the total number of users for the requested time period")
  @Macro
  protected String metricsList;

  @Name(DIMENSIONS)
  @Description("Attributes of your data. For example, "
      + "the dimension ga:city indicates the city, for example, \"Paris\" or \"New York\"")
  @Macro
  protected String dimensionsList;

  @Name(SAMPLING_LEVEL)
  @Description("Desired report sample size")
  @Nullable
  @Macro
  protected String sampleSize;

  public GoogleAnalyticsConfig(String referenceName) {
    super(referenceName);
  }

  Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(getMetricsList(), getDimensionsList());
    }
    return schema;
  }

  public String getStartDate() {
    return startDate;
  }

  public String getEndDate() {
    return endDate;
  }

  @Nullable
  public String getSampleSize() {
    return sampleSize;
  }

  public List<String> getMetricsList() {
    if (!Strings.isNullOrEmpty(metricsList)) {
      return Arrays.asList(metricsList.split(","));
    } else {
      return Collections.emptyList();
    }
  }

  public List<String> getDimensionsList() {
    if (!Strings.isNullOrEmpty(dimensionsList)) {
      return Arrays.asList(dimensionsList.split(","));
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);
    //TODO extend validation here
  }
}