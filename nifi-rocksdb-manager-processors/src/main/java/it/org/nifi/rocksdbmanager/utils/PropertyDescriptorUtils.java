/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.org.nifi.rocksdbmanager.utils;

import org.apache.nifi.processor.Relationship;

public class PropertyDescriptorUtils {
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a Flowfile cannot be enriched, the unchanged FlowFile" +
                    "will be reouted to this relationship")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All records will be sent to this Relationship if configured to do so," +
                    "unless a failure occurs")
            .build();

    public static final Relationship REL_UNMATCHED = new Relationship.Builder()
            .name("unmatched")
            .description("All records for which the lookup does not have a matching value will" +
                    "be routed to this relationship")
            .build();


    public static final String READANDWRITE = "Read And Write";
    public static final String READONLY = "Read Only";
    public static final String FIND = "Find";
    public static final String ITERATOR = "RocksIterator";
    public static final String FLOWFILE_ATTRIBUTE = "FlowFile Attribute";
    public static final String FLOWFILE_CONTENT = "FlowFile Content";
    public static final String SEEK_PREV = "Seek Prev";
    public static final String SEEK_NEXT = "Seek Next";
    public static final String SEEK_FOR_PREV = "Seek For Prev";
}


