package io.cdap.plugin.ga360.etl;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.ga360.source.batch.GoogleAnalyticsBatchSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.cdap.plugin.ga360.source.batch.GoogleAnalyticsConfig.METRICS;
import static io.cdap.plugin.ga360.source.common.BaseSourceConfig.AUTHORIZATION_TOKEN;
import static io.cdap.plugin.ga360.source.common.BaseSourceConfig.GOOGLE_ANALYTICS_VIEW;

public class GoogleAnalyticsBatchSourceTest extends HydratorTestBase {

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static String authorizationToken;
  private static String viewId;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // initialize fb api
    authorizationToken = System.getProperty(AUTHORIZATION_TOKEN);
    if (Strings.isNullOrEmpty(authorizationToken)) {
      throw new IllegalArgumentException(String.format("%s system property must not be empty.", AUTHORIZATION_TOKEN));
    }

    viewId = System.getProperty(GOOGLE_ANALYTICS_VIEW);
    if (Strings.isNullOrEmpty(GOOGLE_ANALYTICS_VIEW)) {
      throw new IllegalArgumentException(String.format("%s system property must not be empty.", GOOGLE_ANALYTICS_VIEW));
    }

    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact, GoogleAnalyticsBatchSource.class);
  }

  @Test
  public void testBatchSource() throws Exception {
    String givenMetricName = "ga:users";

    ETLStage source = new ETLStage("source", new ETLPlugin(
      GoogleAnalyticsBatchSource.NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put("referenceName", "ref")
        .put(AUTHORIZATION_TOKEN, authorizationToken)
        .put(GOOGLE_ANALYTICS_VIEW, viewId)
        .put(METRICS, givenMetricName)
        .build(),
      null)
    );
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin("outputSink"));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("HttpBatch_");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset("outputSink");
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(1, outputRecords.size());

    Assert.assertNotNull(outputRecords.get(0).get(givenMetricName));
  }
}
