package it.org.nifi.rocksdbmanager.processors;/*
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


import it.org.nifi.rocksdbmanager.services.RocksDbServiceRocks;
import it.org.nifi.rocksdbmanager.utils.RocksDbUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static it.org.nifi.rocksdbmanager.utils.PropertyDescriptorUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProcessorsAndServiceTest {

    private static final String DB_NAME = "./rockstest";
    private TestRunner testRunner;

    @BeforeAll
    public static void setup() throws Exception {
        RocksDbUtils rocksDbUtils = new RocksDbUtils();
        HashMap<String, String> optionsMap = new HashMap<>() {{
            put("setCreateIfMissing", "true");
        }};
        rocksDbUtils.initDbWrite(DB_NAME, optionsMap);
        rocksDbUtils.resetDb();
    }

    @AfterAll
    public static void deleteDb() throws IOException {
        FileUtils.deleteFile(new File(DB_NAME), true);
    }

    @Order(1)
    @Test
    public void testWriter() throws InitializationException {
        RocksDbServiceRocks rocksDbService = new RocksDbServiceRocks();
        testRunner = TestRunners.newTestRunner(RocksDbWriter.class);
        testRunner.addControllerService("service", rocksDbService);
        testRunner.setProperty(rocksDbService, RocksDbServiceRocks.DATABASE_PATH, "./rockstest");
        testRunner.setProperty(rocksDbService, RocksDbServiceRocks.OPEN_MODE, READANDWRITE);
        testRunner.enableControllerService(rocksDbService);
        testRunner.setProperty(RocksDbWriter.ROCKSDB_SERVICE, "service");
        testRunner.setProperty(RocksDbWriter.SAVE_FROM, FLOWFILE_CONTENT);
        testRunner.setProperty(RocksDbWriter.KEY, "thisisatest");
        testRunner.enqueue("{\"ip\":\"192.168.0.1\"}");
        testRunner.run();
        testRunner.disableControllerService(rocksDbService);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals(flowFile.getAttribute("rocksdb.write.success"),
                "true");
    }

    @Order(2)
    @Test
    public void testReader() throws InitializationException {
        RocksDbServiceRocks rocksDbService = new RocksDbServiceRocks();
        testRunner = TestRunners.newTestRunner(RocksDbReader.class);
        testRunner.addControllerService("service", rocksDbService);
        testRunner.setProperty(rocksDbService, RocksDbServiceRocks.DATABASE_PATH, DB_NAME);
        testRunner.setProperty(rocksDbService, RocksDbServiceRocks.OPEN_MODE, READONLY);
        testRunner.enableControllerService(rocksDbService);
        testRunner.setProperty(RocksDbReader.ROCKSDB_SERVICE, "service");
        testRunner.setProperty(RocksDbReader.SEARCH_TYPE, FIND);
        testRunner.setProperty(RocksDbReader.RESULT_DESTINATION, "FlowFile Attribute");
        testRunner.setProperty(RocksDbReader.KEY, "thisisatest");
        testRunner.enqueue("");
        testRunner.run();
        testRunner.disableControllerService(rocksDbService);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals(flowFile.getAttribute("rocksdb.search.value"),
                "{\"ip\":\"192.168.0.1\"}");
    }

}
