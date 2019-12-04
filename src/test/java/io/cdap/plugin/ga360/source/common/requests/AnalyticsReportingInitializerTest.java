package io.cdap.plugin.ga360.source.common.requests;

import com.google.api.services.analyticsreporting.v4.AnalyticsReporting;
import io.cdap.plugin.ga360.source.batch.GoogleAnalyticsBatchSource;
import io.cdap.plugin.ga360.source.common.AnalyticsReportingInitializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;

public class AnalyticsReportingInitializerTest {

  @Test
  public void shouldInitializeAnalyticsReporting() throws GeneralSecurityException, IOException {
    //when
    AnalyticsReporting analyticsReporting = AnalyticsReportingInitializer.initializeAnalyticsReporting();

    //then
    Assert.assertNotNull(analyticsReporting);
    Assert.assertEquals(GoogleAnalyticsBatchSource.NAME, analyticsReporting.getApplicationName());
  }
}
