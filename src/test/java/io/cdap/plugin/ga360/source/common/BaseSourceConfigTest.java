package io.cdap.plugin.ga360.source.common;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    Assert.assertEquals(2, failureCollector.getValidationFailures().size());
  }
}
