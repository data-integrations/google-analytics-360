package io.cdap.plugin.ga360.source.streaming;

import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.model.RealtimeData;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.ga360.source.common.ReportTransformer;
import io.cdap.plugin.ga360.source.common.requests.RealTimeRequestFactory;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Receiver that gets data from Google Analytics Real Time API.
 */
public class GoogleAnalyticsReceiver extends Receiver<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleAnalyticsReceiver.class);

  private GoogleAnalyticsStreamingSourceConfig config;

  public GoogleAnalyticsReceiver(GoogleAnalyticsStreamingSourceConfig config) {
    super(StorageLevel.MEMORY_ONLY());
    this.config = config;
  }

  @Override
  public void onStart() {
    new Thread(() -> {
      while (!isStopped()) {
        try {
          fetchRealTimeData();
          TimeUnit.SECONDS.sleep(config.getPollInterval());
        } catch (Exception e) {
          LOG.error("Failed to retrieve Google Analytics real time data!", e);
          throw new RuntimeException(e);
        }
      }
    }).start();
  }

  private void fetchRealTimeData() throws IOException {
    final Analytics.Data.Realtime.Get request = RealTimeRequestFactory.createRequest(config);
    final RealtimeData data = request.execute();
    store(ReportTransformer.transform(data, config.getSchema()));
  }

  @Override
  public void onStop() {
    // no-op
  }
}
