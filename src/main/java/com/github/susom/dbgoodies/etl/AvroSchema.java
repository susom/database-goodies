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

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 
 * @author Biarca.inc
 *
 */
public class AvroSchema {
  /**
   * namespace:a string that qualifies the name
   */
  @JsonProperty("namespace")
  private String namespace = "etl.avro";

  /**
   * name: a string providing the name of the record, i.e generally table row
   *   mapped to the record object (or java beans)
   */
  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private String type = "record";

  @JsonProperty("fields")
  private ArrayList<AvroRecord> fields = new ArrayList<>();

  public static AvroSchema getInstance() {
    return new AvroSchema();
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public ArrayList<AvroRecord> getFields() {
    return fields;
  }

  public void setFields(ArrayList<AvroRecord> fields) {
    this.fields = fields;
  }

}
