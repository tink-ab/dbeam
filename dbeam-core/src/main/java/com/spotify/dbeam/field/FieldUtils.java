/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam.field;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldUtils {

  private static Logger LOGGER = LoggerFactory.getLogger(FieldUtils.class);

  public static void validateFields(List<String> fields) {
    if (fields != null) {
      for (String field : fields) {
        if (!field.matches("^[a-zA-Z0-9_:-]*$")) {
          final String message = "The field " + field + " includes invalid characters.";
          LOGGER.error(message);
          throw new IllegalArgumentException(message);
        }
      }
    }
  }

  public static Map<String, Optional<String>> parseFields(List<String> fields) {
    ImmutableMap.Builder<String, Optional<String>> fieldConverterMapBuilder =
        ImmutableMap.builder();
    if (fields != null) {
      for (String field : fields) {
        if (field.contains(":")) {
          String[] pair = field.split(":", 2);
          switch (pair[1]) {
            case "UNHEX":
              fieldConverterMapBuilder =
                  fieldConverterMapBuilder.put(pair[0], Optional.of(pair[1]));
              break;
            default:
              LOGGER.error("Unknown field converter {} for field {}.", pair[1], pair[0]);
          }
        } else {
          fieldConverterMapBuilder = fieldConverterMapBuilder.put(field, Optional.empty());
        }
      }
    }
    return fieldConverterMapBuilder.build();
  }

  public static String createSelectExpression(Map<String, Optional<String>> fieldMapper) {
    return fieldMapper.entrySet().stream()
        .map(e -> e.getValue()
                .map(v -> v + "(" + e.getKey() + ") as " + e.getKey())
                .orElse(e.getKey()))
        .collect(Collectors.joining(","));
  }
}
