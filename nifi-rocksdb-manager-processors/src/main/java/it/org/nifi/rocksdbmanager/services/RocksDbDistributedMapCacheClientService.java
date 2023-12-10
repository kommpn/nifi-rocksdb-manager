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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Tags({"redis", "distributed", "cache", "map"})
@CapabilityDescription("An implementation of DistributedMapCacheClient that uses Redis as the backing cache. This service relies on " +
        "the WATCH, MULTI, and EXEC commands in Redis, which are not fully supported when Redis is clustered. As a result, this service " +
        "can only be used with a Redis Connection Pool that is configured for standalone or sentinel mode. Sentinel mode can be used to " +
        "provide high-availability configurations.")
public class RocksDbDistributedMapCacheClientService extends SimpleRocksDbDistributedMapCacheClientService implements AtomicDistributedMapCacheClient<byte[]> {

    @Override
    public <K, V> AtomicCacheEntry<K, V, byte[]> fetch(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {

        final byte[] k = serialize(key, keySerializer);

        final byte[] v;
        try {
            v = rocksDbUtils.find(k);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        if (v == null) {
            return null;
        }

        // for Redis we are going to use the raw bytes of the value as the revision
        return new AtomicCacheEntry<>(key, valueDeserializer.deserialize(v), v);

    }

    @Override
    public <K, V> boolean replace(final AtomicCacheEntry<K, V, byte[]> entry, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(entry.getKey(), out);
        final byte[] k = out.toByteArray();
        out.reset();

        valueSerializer.serialize(entry.getValue(), out);
        final byte[] newVal = out.toByteArray();

        try {
            return rocksDbUtils.put(k, newVal);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }


    }

}