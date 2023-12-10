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

package it.org.nifi.rocksdbmanager.services;

import it.org.nifi.rocksdbmanager.utils.RocksDbUtils;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static it.org.nifi.rocksdbmanager.utils.PropertyDescriptorUtils.READANDWRITE;
import static it.org.nifi.rocksdbmanager.utils.PropertyDescriptorUtils.READONLY;


public class SimpleRocksDbDistributedMapCacheClientService extends AbstractControllerService implements DistributedMapCacheClient {


    public static final PropertyDescriptor DATABASE_PATH = new PropertyDescriptor.Builder()
            .name("database-path")
            .displayName("Database Path")
            .description("The path of the rocksdb to open and utilize.")
            .required(true)
            .addValidator(StandardValidators.DirectoryExistsValidator.VALID)
            .build();

    public static final PropertyDescriptor OPEN_MODE = new PropertyDescriptor.Builder()
            .name("open-mode")
            .displayName("Open Mode")
            .description("Mode to open RocksDB. Choosing \"".concat(READONLY).concat("\" allows just reads on the Rocksdb." +
                    "\"".concat(READANDWRITE).concat("\" mode allows read and writes inside the database.")))
            .required(true)
            .defaultValue(READANDWRITE)
            .allowableValues(Set.of(READANDWRITE, READONLY))
            .build();

    public RocksDbUtils rocksDbUtils;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DATABASE_PATH);
        properties.add(OPEN_MODE);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        String fileDirectory = context.getProperty(DATABASE_PATH).getValue();
        HashMap<String, String> dynamicProperties = new HashMap<>();
        for (PropertyDescriptor entry : context.getProperties().keySet()) {
            if (entry.isDynamic()) {
                dynamicProperties.put(entry.getName(), context.getProperty(entry).getValue());
            }
        }

        rocksDbUtils = new RocksDbUtils();

        try {
            if (READANDWRITE.equals(context.getProperty(OPEN_MODE).getValue())) {
                rocksDbUtils.initDbWrite(fileDirectory, dynamicProperties);
            } else if (READONLY.equals(context.getProperty(OPEN_MODE).getValue())) {
                rocksDbUtils.initDbReadOnly(fileDirectory, dynamicProperties);
            } else {
                throw new RocksDBException("OpenMode not specified is not allowed.");
            }

        } catch (RocksDBException ex) {
            getLogger().error("RocksDB not initialized in {} mode, service cannot start.\n" +
                    "The error is:\n {}", context.getProperty(OPEN_MODE).getValue(), ex.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @OnDisabled
    public void onDisable() {
        rocksDbUtils.resetDb();
    }


    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        try {
            return rocksDbUtils.put(kv.getKey(), kv.getValue());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        try {
            final byte[] existingValue = rocksDbUtils.find(kv.getKey());
            if (existingValue != null) {
                return valueDeserializer.deserialize(existingValue);
            } else {
                rocksDbUtils.put(kv.getKey(), kv.getValue());
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> serializer) throws IOException {
        final byte[] k = serialize(key, serializer);
        try {
            return rocksDbUtils.containsKey(k);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        try {
            rocksDbUtils.put(kv.getKey(), kv.getValue());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final byte[] k = serialize(key, keySerializer);
        try {
            final byte[] existingValue = rocksDbUtils.find(k);
            return existingValue != null ? valueDeserializer.deserialize(existingValue) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() {
        //nothing to do
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> keySerializer) throws IOException {
        final byte[] k = serialize(key, keySerializer);
        try {
            return rocksDbUtils.delete(k);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long removeByPattern(String s) {
        throw new UnsupportedOperationException();
    }

    protected <K, V> Tuple<byte[], byte[]> serialize(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(key, out);
        final byte[] k = out.toByteArray();

        out.reset();

        valueSerializer.serialize(value, out);
        final byte[] v = out.toByteArray();

        return new Tuple<>(k, v);
    }

    protected <K> byte[] serialize(final K key, final Serializer<K> keySerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(key, out);
        return out.toByteArray();
    }
}
