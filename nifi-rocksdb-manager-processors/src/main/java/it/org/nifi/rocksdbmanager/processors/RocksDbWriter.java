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
package it.org.nifi.rocksdbmanager.processors;

import it.org.nifi.rocksdbmanager.exception.LookupFailureException;
import it.org.nifi.rocksdbmanager.services.RocksDbServiceRocks;
import it.org.nifi.rocksdbmanager.utils.RocksDbRocksLookupService;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static it.org.nifi.rocksdbmanager.utils.PropertyDescriptorUtils.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"rocksdb", "writer"})
@CapabilityDescription("Processor that allows user to write custom key/values inside a RocksDb using a RocksDbService.")
@SeeAlso({RocksDbReader.class, RocksDbServiceRocks.class})
@WritesAttributes({@WritesAttribute(attribute = "rocksdb.write.success",
        description = "true or false, determines if the writing has been successful"),
        @WritesAttribute(attribute = "rocksdb.write.error",
                description = "contains the error occurred when writing to RocksDB")})
public class RocksDbWriter extends AbstractProcessor {

    public static final PropertyDescriptor ROCKSDB_SERVICE = new PropertyDescriptor
            .Builder().name("rocksdb-service")
            .displayName("RocksDB Service")
            .description("Designed Rocksdb to utilize")
            .required(true)
            .identifiesControllerService(RocksDbRocksLookupService.class)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor
            .Builder().name("key-name")
            .displayName("Key Name")
            .description("Attribute to lookup inside the database. It will be" +
                    " transformed into bytes to search its value inside the Rocksdb.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SAVE_FROM = new PropertyDescriptor
            .Builder().name("save-from")
            .displayName("Save From")
            .description("Destination where to pickup value to save inside the rocksdb.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(Set.of(FLOWFILE_ATTRIBUTE, FLOWFILE_CONTENT))
            .build();
    public static final PropertyDescriptor VALUE = new PropertyDescriptor
            .Builder().name("value")
            .displayName("Value")
            .description("Attribute to save inside the rocksdb. It is used only if \"Save From\" is attribute.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .dependsOn(SAVE_FROM, FLOWFILE_ATTRIBUTE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private final Set<Relationship> relationships = Set.of(REL_SUCCESS,
            REL_FAILURE);

    protected volatile RocksDbRocksLookupService lookupService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ROCKSDB_SERVICE);
        properties.add(KEY);
        properties.add(SAVE_FROM);
        properties.add(VALUE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.lookupService = context.getProperty(ROCKSDB_SERVICE)
                .asControllerService(RocksDbRocksLookupService.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();

        String saveFrom = context.getProperty(SAVE_FROM).getValue();
        String value = "";

        if (FLOWFILE_ATTRIBUTE.equals(saveFrom)) {
            value = context.getProperty(VALUE).evaluateAttributeExpressions(flowFile).getValue();
        } else if (FLOWFILE_CONTENT.equals(saveFrom)) {
            try (InputStream is = session.read(flowFile)) {
                value = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                session.transfer(flowFile, REL_FAILURE);
                throw new RuntimeException(e);
            }
        }
        if (value == null || value.isBlank()) {
            session.transfer(flowFile, REL_UNMATCHED);
            return;
        }


        try {
            lookupService.write(Map.of(
                    "key", key,
                    "value", value
            ));
        } catch (LookupFailureException e) {
            session.transfer(flowFile, REL_FAILURE);
            throw new RuntimeException(e);
        }


        session.putAttribute(flowFile, "rocksdb.write.success", "true");
        session.transfer(flowFile, REL_SUCCESS);


    }
}
