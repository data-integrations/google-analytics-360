package io.cdap.plugin.ga360.source.batch;

import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.cdap.plugin.ga360.source.batch.GoogleAnalyticsConfig.END_DATE;
import static io.cdap.plugin.ga360.source.batch.GoogleAnalyticsConfig.START_DATE;

public class GoogleAnalyticsConfigTest {

  @Test
  public void testGetMetricsListCaseNotEmpty() {
    //given
    String givenMetric1 = "metric1";
    String givenMetric2 = "metric2";
    GoogleAnalyticsConfig config = new GoogleAnalyticsConfig("ref");
    config.metricsList = String.join(",", Arrays.asList(givenMetric1, givenMetric2));

    //when
    List<String> metricsList = config.getMetricsList();

    //then
    Assert.assertTrue(metricsList.contains(givenMetric1));
    Assert.assertTrue(metricsList.contains(givenMetric2));
  }

  @Test
  public void testGetMetricsListCaseEmpty() {
    //given
    GoogleAnalyticsConfig config = new GoogleAnalyticsConfig("ref");
    config.metricsList = "";

    //when
    List<String> metricsList = config.getMetricsList();

    //then
    Assert.assertTrue(metricsList.isEmpty());
  }

  @Test
  public void testGetMetricsListCaseNull() {
    //given
    GoogleAnalyticsConfig config = new GoogleAnalyticsConfig("ref");

    //when
    List<String> metricsList = config.getMetricsList();

    //then
    Assert.assertTrue(metricsList.isEmpty());
  }

  @Test
  public void testGetDimensionsListCaseNotEmpty() {
    //given
    String givenMetric1 = "dimension1";
    String givenMetric2 = "dimension2";
    GoogleAnalyticsConfig config = new GoogleAnalyticsConfig("ref");
    config.dimensionsList = String.join(",", Arrays.asList(givenMetric1, givenMetric2));

    //when
    List<String> dimensionsList = config.getDimensionsList();

    //then
    Assert.assertTrue(dimensionsList.contains(givenMetric1));
    Assert.assertTrue(dimensionsList.contains(givenMetric2));
  }

  @Test
  public void testGetDimensionsListCaseEmpty() {
    //given
    GoogleAnalyticsConfig config = new GoogleAnalyticsConfig("ref");
    config.dimensionsList = "";

    //when
    List<String> dimensionsList = config.getDimensionsList();

    //then
    Assert.assertTrue(dimensionsList.isEmpty());
  }

  @Test
  public void testGetDimensionsListCaseNull() {
    //given
    GoogleAnalyticsConfig config = new GoogleAnalyticsConfig("ref");

    //when
    List<String> dimensionsList = config.getDimensionsList();

    //then
    Assert.assertTrue(dimensionsList.isEmpty());
  }

  @Test
  public void testDateRangeCaseEndDateNull() {
    //given
    GoogleAnalyticsConfig config = new GoogleAnalyticsConfig("ref");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.startDate = "7DaysAgo";

    //when
    config.validate(failureCollector);

    boolean isStartDateFailure = failureCollector.getValidationFailures().stream()
      .map(ValidationFailure::getCauses)
      .flatMap(Collection::stream)
      .anyMatch(cause -> cause.getAttributes().containsValue(END_DATE));

    //then
    Assert.assertTrue(isStartDateFailure);
  }

  @Test
  public void testDateRangeCaseStartDateNull() {
    //given
    GoogleAnalyticsConfig config = new GoogleAnalyticsConfig("ref");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.endDate = "today";

    //when
    config.validate(failureCollector);

    boolean isStartDateFailure = failureCollector.getValidationFailures().stream()
      .map(ValidationFailure::getCauses)
      .flatMap(Collection::stream)
      .anyMatch(cause -> cause.getAttributes().containsValue(START_DATE));

    //then
    Assert.assertTrue(isStartDateFailure);
  }
}
