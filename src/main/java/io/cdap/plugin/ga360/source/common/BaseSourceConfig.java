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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Base configuration for Google Analytics sources.
 */
public class BaseSourceConfig extends ReferencePluginConfig {

  public static final String AUTHORIZATION_TOKEN = "authorizationToken";
  public static final String GOOGLE_ANALYTICS_VIEW = "viewId";
  public static final String START_DATE = "startDate";
  public static final String END_DATE = "endDate";
  public static final String METRICS = "metricsList";
  public static final String DIMENSIONS = "dimensionsList";

  @Name(AUTHORIZATION_TOKEN)
  @Description("Authorization token to access Google Analytics reporting API")
  @Macro
  protected String authorizationToken;

  @Name(GOOGLE_ANALYTICS_VIEW)
  @Description("The Google Analytics view ID from which to retrieve data")
  @Macro
  protected String viewId;

  @Name(START_DATE)
  @Description("Start date for the report data")
  @Nullable
  @Macro
  protected String startDate;

  @Name(END_DATE)
  @Description("End date for the report data")
  @Nullable
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
  @Nullable
  @Macro
  protected String dimensionsList;

  private transient Schema schema = null;

  public BaseSourceConfig(String referenceName) {
    super(referenceName);
  }

  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(getMetricsList(), getDimensionsList());
    }
    return schema;
  }

  public String getAuthorizationToken() {
    return authorizationToken;
  }

  public String getViewId() {
    return viewId;
  }

  @Nullable
  public String getStartDate() {
    return startDate;
  }

  @Nullable
  public String getEndDate() {
    return endDate;
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


  public void validate(FailureCollector failureCollector) {
    if (!containsMacro(authorizationToken) && Strings.isNullOrEmpty(authorizationToken)) {
      failureCollector
        .addFailure(String.format("%s must be specified.", AUTHORIZATION_TOKEN), null)
        .withConfigProperty(AUTHORIZATION_TOKEN);
    }
    if (!containsMacro(viewId) && Strings.isNullOrEmpty(viewId)) {
      failureCollector
        .addFailure(String.format("%s must be specified.", GOOGLE_ANALYTICS_VIEW), null)
        .withConfigProperty(GOOGLE_ANALYTICS_VIEW);
    }
    if ((!containsMacro(startDate) && Strings.isNullOrEmpty(startDate))
      && !Strings.isNullOrEmpty(endDate)) {
      failureCollector
        .addFailure(String.format("Both %s and %s must be specified.", START_DATE, END_DATE),
                    String.format("Specify %s or remove %s for using default date range.", START_DATE, END_DATE))
        .withConfigProperty(START_DATE);
    }
    if (!Strings.isNullOrEmpty(startDate) && (!containsMacro(endDate) && Strings.isNullOrEmpty(endDate))) {
      failureCollector
        .addFailure(String.format("Both %s and %s must be specified.", START_DATE, END_DATE),
                    String.format("Specify %s or remove %s for using default date range.", END_DATE, START_DATE))
        .withConfigProperty(END_DATE);
    }
  }
}
