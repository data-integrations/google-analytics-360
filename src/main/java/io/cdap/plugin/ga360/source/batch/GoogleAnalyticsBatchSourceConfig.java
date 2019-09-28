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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.ga360.source.common.BaseSourceConfig;

import javax.annotation.Nullable;

/**
 * Provides all required configuration for reading Google Analytics data from Batch Source.
 */
public class GoogleAnalyticsBatchSourceConfig extends BaseSourceConfig {

  public static final String SAMPLING_LEVEL = "sampleSize";

  @Name(SAMPLING_LEVEL)
  @Description("Desired report sample size")
  @Nullable
  @Macro
  protected String sampleSize;

  public GoogleAnalyticsBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  @Nullable
  public String getSampleSize() {
    return sampleSize;
  }

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);
  }
}
