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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

/**
 * Base configuration for facebook sources.
 */
public class BaseSourceConfig extends ReferencePluginConfig {

  public static final String AUTHORIZATION_TOKEN = "authorizationToken";
  public static final String GOOGLE_ANALYTICS_VIEW = "viewId";
  @Name(AUTHORIZATION_TOKEN)
  @Description("Authorization token to access Google Analytics reporting API")
  @Macro
  protected String authorizationToken;
  @Name(GOOGLE_ANALYTICS_VIEW)
  @Description("The Google Analytics view ID from which to retrieve data")
  @Macro
  protected String viewId;

  public BaseSourceConfig(String referenceName) {
    super(referenceName);
  }

  public String getAuthorizationToken() {
    return authorizationToken;
  }

  public String getViewId() {
    return viewId;
  }

  public void validate(FailureCollector failureCollector) {
    if (Strings.isNullOrEmpty(authorizationToken)) {
      failureCollector
        .addFailure(String.format("%s must be specified.", AUTHORIZATION_TOKEN), null)
        .withConfigProperty(AUTHORIZATION_TOKEN);
    }
    if (Strings.isNullOrEmpty(viewId)) {
      failureCollector
        .addFailure(String.format("%s must be specified.", GOOGLE_ANALYTICS_VIEW), null)
        .withConfigProperty(GOOGLE_ANALYTICS_VIEW);
    }
  }
}
