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

import com.google.api.services.analyticsreporting.v4.model.Report;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReportTransformer {

  public static StructuredRecord transform(Report report, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    //TODO write transform logic
//    JsonObject insightsJson = insights.getRawResponseAsJsonObject();
//    insightsJson.entrySet().forEach(entry -> {
//      if (schemaContainsField(schema, entry.getKey())) {
//        Schema fieldSchema = schema.getField(entry.getKey()).getSchema();
//
//        if (fieldSchema.isNullable()) {
//          fieldSchema = fieldSchema.getNonNullable();
//        }
//
//        switch (fieldSchema.getType()) {
//          case STRING:
//            builder.set(entry.getKey(), entry.getValue().getAsJsonPrimitive().getAsString());
//            break;
//          case RECORD:
//            builder.set(entry.getKey(), fromJsonObject(fieldSchema, entry.getValue().getAsJsonObject()));
//            break;
//          case ARRAY:
//            Schema componentSchema = fieldSchema.getComponentSchema();
//            List<StructuredRecord> records = StreamSupport
//                .stream(entry.getValue().getAsJsonArray().spliterator(), false)
//                .map(jsonElement -> fromJsonObject(componentSchema, jsonElement.getAsJsonObject()))
//                .collect(Collectors.toList());
//            builder.set(entry.getKey(), records);
//            break;
//        }
//      }
//    });

    return builder.build();
  }
}
