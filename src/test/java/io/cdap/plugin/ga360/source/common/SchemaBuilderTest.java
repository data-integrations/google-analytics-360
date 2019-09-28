package io.cdap.plugin.ga360.source.common;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SchemaBuilderTest {

  @Test
  public void testBuildSchemaCaseNotEmpty() {
    //given
    String givenMetric1 = "ga:users";
    String mappedMetric1 = SchemaBuilder.mapGoogleAnalyticsFieldToAvro(givenMetric1);
    String givenMetric2 = "ga:dcmCost";
    String mappedMetric2 = SchemaBuilder.mapGoogleAnalyticsFieldToAvro(givenMetric2);
    String givenDimension1 = "ga:userType";
    String mappedDimension1 = SchemaBuilder.mapGoogleAnalyticsFieldToAvro(givenDimension1);
    String givenDimension2 = "ga:sessionCount";
    String mappedDimension2 = SchemaBuilder.mapGoogleAnalyticsFieldToAvro(givenDimension2);
    List<String> givenMetrics = Arrays.asList(givenMetric1, givenMetric2);
    List<String> givenDimensions = Arrays.asList(givenDimension1, givenDimension2);

    //when
    Schema schema = SchemaBuilder.buildSchema(givenMetrics, givenDimensions);

    //then
    Assert.assertNotNull(schema);
    Assert.assertEquals(mappedMetric1, Objects.requireNonNull(schema.getField(mappedMetric1)).getName());
    Assert.assertEquals(mappedMetric2, Objects.requireNonNull(schema.getField(mappedMetric2)).getName());
    Assert.assertEquals(mappedDimension1, Objects.requireNonNull(schema.getField(mappedDimension1)).getName());
    Assert.assertEquals(mappedDimension2, Objects.requireNonNull(schema.getField(mappedDimension2)).getName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildSchemaCaseEmpty() {
    //when
    SchemaBuilder.buildSchema(new ArrayList<>(), new ArrayList<>());
  }
}
