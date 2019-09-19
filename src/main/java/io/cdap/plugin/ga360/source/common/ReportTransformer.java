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

import com.google.api.services.analyticsreporting.v4.model.DateRangeValues;
import com.google.api.services.analyticsreporting.v4.model.MetricHeaderEntry;
import com.google.api.services.analyticsreporting.v4.model.Report;
import com.google.api.services.analyticsreporting.v4.model.ReportRow;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import java.util.List;
import java.util.stream.IntStream;

public class ReportTransformer {

  public static StructuredRecord transform(Report report, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    transformMetrics(report, schema, builder);
    transformDimensions(report, schema, builder);
    return builder.build();
  }

  private static void transformMetrics(Report report, Schema schema, StructuredRecord.Builder builder) {
    List<MetricHeaderEntry> metricHeaderEntries = report.getColumnHeader().getMetricHeader().getMetricHeaderEntries();
    List<ReportRow> rows = report.getData().getRows();
    rows.forEach(row -> {
      List<DateRangeValues> rowMetrics = row.getMetrics();
      IntStream.range(0, rowMetrics.size())
          .forEach(i -> {
            MetricHeaderEntry metricHeaderEntry = metricHeaderEntries.get(i);
            if (schemaContainsField(schema, metricHeaderEntry.getName())) {
              builder.set(metricHeaderEntry.getName(), rowMetrics.get(i).getValues().get(0)); //FIXME use date ranges for metrics
            }
          });
    });
  }

  private static void transformDimensions(Report report, Schema schema, StructuredRecord.Builder builder) {
    List<String> dimensions = report.getColumnHeader().getDimensions();
    List<ReportRow> rows = report.getData().getRows();
    rows.forEach(row -> {
      List<String> rowDimensions = row.getDimensions();
      IntStream.range(0, rowDimensions.size())
          .forEach(i -> {
            String columnHeader = dimensions.get(i);
            if (schemaContainsField(schema, columnHeader)) {
              builder.set(columnHeader, rowDimensions.get(i));
            }
          });
    });
  }

  private static boolean schemaContainsField(Schema schema, String fieldName) {
    return schema.getFields().stream().anyMatch(field -> field.getName().equals(fieldName));
  }
}
