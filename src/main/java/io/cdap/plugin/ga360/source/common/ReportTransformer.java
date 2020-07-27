/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.api.services.analytics.model.RealtimeData;
import com.google.api.services.analyticsreporting.v4.model.DateRangeValues;
import com.google.api.services.analyticsreporting.v4.model.MetricHeaderEntry;
import com.google.api.services.analyticsreporting.v4.model.Report;
import com.google.api.services.analyticsreporting.v4.model.ReportRow;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This is helper class for transforming {@link Report} instance to {@link StructuredRecord}.
 */
public class ReportTransformer {

  /**
   * Transforms {@link Report} instance to {@link StructuredRecord} instance accordingly to given schema.
   */
  public static StructuredRecord transform(Report report, Schema schema) {
    Schema arraySchema = Schema.recordOf("output", Schema.Field.of("rows", Schema.arrayOf(schema)));
    StructuredRecord.Builder builder = StructuredRecord.builder(arraySchema);
    List<MetricHeaderEntry> metricHeaderEntries = report.getColumnHeader().getMetricHeader().getMetricHeaderEntries();
    List<String> dimensions = report.getColumnHeader().getDimensions();
    List<ReportRow> rows = report.getData().getRows();
    List<StructuredRecord> records = rows == null ? Collections.emptyList() : rows.stream()
      .map(row -> transformRow(row, metricHeaderEntries, dimensions, schema))
      .collect(Collectors.toList());
    builder.set("rows", records);
    return builder.build();
  }

  /**
   * Return the StructuredRecord.
   * @param realtimeData the realtimeData
   * @param schema the schema
   * @return the StructuredRecord
   */
  public static StructuredRecord transform(RealtimeData realtimeData, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    transformData(realtimeData, schema, builder);
    return builder.build();
  }

  private static void transformData(RealtimeData realtimeData, Schema schema, StructuredRecord.Builder builder) {
    final List<RealtimeData.ColumnHeaders> columnHeaders = realtimeData.getColumnHeaders();
    Map<String, String> result = realtimeData.getTotalsForAllResults();
    columnHeaders.forEach(header -> {
      String headerName = header.getName();
      if (result.containsKey(headerName)) {
        String name = SchemaBuilder.mapGoogleAnalyticsFieldToAvro(header.getName());
        if (schemaContainsField(schema, name)) {
          builder.set(name, result.get(headerName));
        }
      }
    });
  }

  private static StructuredRecord transformRow(ReportRow row, List<MetricHeaderEntry> metricHeaderEntries,
                                               List<String> dimensions, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<DateRangeValues> rowMetrics = row.getMetrics();
    IntStream.range(0, rowMetrics.size())
      .forEach(i -> {
        MetricHeaderEntry metricHeaderEntry = metricHeaderEntries.get(i);
        String name = SchemaBuilder.mapGoogleAnalyticsFieldToAvro(metricHeaderEntry.getName());
        if (schemaContainsField(schema, name)) {
          builder.set(name, rowMetrics.get(i).getValues().get(0));
        }
      });
    List<String> rowDimensions = row.getDimensions();
    if (rowDimensions != null) {
      IntStream.range(0, rowDimensions.size())
        .forEach(i -> {
          String columnHeader = SchemaBuilder.mapGoogleAnalyticsFieldToAvro(dimensions.get(i));
          if (schemaContainsField(schema, columnHeader)) {
            builder.set(columnHeader, rowDimensions.get(i));
          }
        });
    }
    return builder.build();
  }

  private static boolean schemaContainsField(Schema schema, String fieldName) {
    return schema.getFields().stream().anyMatch(field -> field.getName().equals(fieldName));
  }
}
