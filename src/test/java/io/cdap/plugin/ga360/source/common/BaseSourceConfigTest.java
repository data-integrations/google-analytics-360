package io.cdap.plugin.ga360.source.common;

import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static io.cdap.plugin.ga360.source.common.BaseSourceConfig.END_DATE;
import static io.cdap.plugin.ga360.source.common.BaseSourceConfig.START_DATE;

public class BaseSourceConfigTest {

  private MockFailureCollector failureCollector;

  @Before
  public void setUp() {
    failureCollector = new MockFailureCollector();
  }

  @Test
  public void testValidateFieldsCaseCorrectFields() {
    //given
    BaseSourceConfig config = new BaseSourceConfig("ref");
    config.authorizationToken = "token";
    config.viewId = "123345";
    config.metricsList = "ga:users";

    //when
    config.validate(failureCollector);

    //then
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFieldsCaseEmptyFields() {
    //given
    BaseSourceConfig config = new BaseSourceConfig("ref");

    //when
    config.validate(failureCollector);

    //then
    Assert.assertEquals(3, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testGetMetricsListCaseNotEmpty() {
    //given
    String givenMetric1 = "metric1";
    String givenMetric2 = "metric2";
    BaseSourceConfig config = new BaseSourceConfig("ref");
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
    BaseSourceConfig config = new BaseSourceConfig("ref");
    config.metricsList = "";

    //when
    List<String> metricsList = config.getMetricsList();

    //then
    Assert.assertTrue(metricsList.isEmpty());
  }

  @Test
  public void testGetMetricsListCaseNull() {
    //given
    BaseSourceConfig config = new BaseSourceConfig("ref");

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
    BaseSourceConfig config = new BaseSourceConfig("ref");
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
    BaseSourceConfig config = new BaseSourceConfig("ref");
    config.dimensionsList = "";

    //when
    List<String> dimensionsList = config.getDimensionsList();

    //then
    Assert.assertTrue(dimensionsList.isEmpty());
  }

  @Test
  public void testGetDimensionsListCaseNull() {
    //given
    BaseSourceConfig config = new BaseSourceConfig("ref");

    //when
    List<String> dimensionsList = config.getDimensionsList();

    //then
    Assert.assertTrue(dimensionsList.isEmpty());
  }

  @Test
  public void testDateRangeCaseEndDateNull() {
    //given
    BaseSourceConfig config = new BaseSourceConfig("ref");
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
    BaseSourceConfig config = new BaseSourceConfig("ref");
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
