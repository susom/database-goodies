/*
 * Copyright 2017 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.susom.dbgoodies.etl;

import java.sql.Types;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 
 * @author Biarca.inc
 *
 */
public class AvroRecord {
  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private ArrayList<String> type = new ArrayList<>();
  
  public AvroRecord() {
    type.add("null");
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ArrayList<String> getType() {
    return type;
  }

  public void setType(ArrayList<String> type) {
    this.type = type;
  }

  public static AvroRecord getIntance() {
    return new AvroRecord();
  }

  public static String getType(final int avroType) {
    switch (avroType) {
      case Types.BOOLEAN:
        return "boolean";
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.NUMERIC:
        return "int";

      case Types.BIGINT:
        return "long";

      case Types.REAL:
      case 100:
        return "float";

      case Types.DOUBLE:
      case 101:
        return "double";

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.BLOB:
        return "bytes";

      case Types.CLOB:
      case Types.NCLOB:
        return "string";

      case Types.TIMESTAMP:
        return "string";

      case Types.NVARCHAR:
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.NCHAR:
        return "string";

      default:
        return "string";
    }
  }
}
