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

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.stereotype.Repository;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

import static it.org.nifi.rocksdbmanager.utils.PropertyDescriptorUtils.*;

@Repository
public class RocksDbUtils {
    RocksDB db;

    public static boolean getIfRocksIsSmall(File rocksdb) {
        return (folderSize(rocksdb) / 1024) / 1024 < 150;
    }

    public static long folderSize(File directory) {
        long length = 0;
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            if (file.isFile()) {
                length += file.length();
            } else {
                length += folderSize(file);
            }
        }
        return length;
    }

    public void initDbReadOnly(String dbPath, HashMap<String, String> optionsMap) throws Exception {

        File rocksdb = new File(dbPath);
        Options options = loadOptions(optionsMap);
        if (getIfRocksIsSmall(rocksdb)) {
            options.optimizeForSmallDb();
        }

        db = RocksDB.openReadOnly(options, rocksdb.getAbsolutePath());

    }

    public void initDbWrite(String dbPath, HashMap<String, String> optionsMap) throws Exception {


        db = RocksDB.open(loadOptions(optionsMap), new File(dbPath).getAbsolutePath());


    }

    private Options loadOptions(HashMap<String, String> optionsMap) throws IllegalArgumentException, InvocationTargetException, IllegalAccessException {
        RocksDB.loadLibrary();
        Options options = new Options();
        for (Method declaredMethod : options.getClass().getDeclaredMethods()) {
            if (optionsMap.containsKey(declaredMethod.getName()) && Modifier.toString(declaredMethod.getModifiers()).contains("public")) {
                Class<?> classToConvert = declaredMethod.getParameterTypes()[0];
                String entryValue = optionsMap.get(declaredMethod.getName());
                if (classToConvert.equals(long.class)) {

                    if (entryValue.matches("\\d")) {
                        declaredMethod.invoke(options, Long.parseLong(entryValue));
                        continue;
                    }
                } else if (classToConvert.equals(String.class)) {
                    declaredMethod.invoke(options, entryValue);

                    continue;
                } else if (classToConvert.equals(int.class)) {

                    if (entryValue.matches("\\d")) {
                        declaredMethod.invoke(options, Integer.parseInt(entryValue));
                        continue;
                    }
                } else if (classToConvert.equals(boolean.class)) {
                    if (entryValue.equals("true") || entryValue.equals("false")) {
                        declaredMethod.invoke(options, Boolean.parseBoolean(entryValue));
                        continue;
                    }
                }
                throw new IllegalArgumentException("Wrong argument value passed");
            }
        }

        return options;
    }

    public synchronized void saveEntry(byte[] key, String value) throws RocksDBException {

        db.put(key, SerializationUtils.serialize(value));


    }

    public synchronized boolean put(byte[] key, byte[] value) throws RocksDBException {

        db.put(key, value);
        return db.get(key) != null;


    }

    public synchronized byte[] find(byte[] key) throws RocksDBException {
        return db.get(key);
    }

    public synchronized boolean containsKey(byte[] key) throws RocksDBException {
        return db.keyExists(key);
    }

    public synchronized boolean delete(byte[] key) throws RocksDBException {
        db.delete(key);
        return !db.keyExists(key);
    }

    public synchronized String find(String key) throws RocksDBException {
        Optional<Object> result = Optional.ofNullable(SerializationUtils.deserialize(db.get(key.getBytes(StandardCharsets.UTF_8))));
        return result.map(Object::toString).orElse(null);
    }

    public synchronized String findIterator(String key, String seekFor) throws RocksDBException {
        try (RocksIterator it = this.db.newIterator()) {
            if (SEEK_PREV.equals(seekFor)) {
                it.seek(key.getBytes(StandardCharsets.UTF_8));
                if (!it.isValid()) {
                    throw new RocksDBException("Error while searching for " + key + " value with " + seekFor + " method.");
                }
                it.prev();
                return getString(key, seekFor, it);
            } else if (SEEK_NEXT.equals(seekFor)) {
                it.seek(key.getBytes(StandardCharsets.UTF_8));
                if (!it.isValid()) {
                    throw new RocksDBException("Error while searching for " + key + " value with " + seekFor + " method.");
                }
                it.next();
                return getString(key, seekFor, it);
            } else if (SEEK_FOR_PREV.equals(seekFor)) {
                it.seekForPrev(key.getBytes(StandardCharsets.UTF_8));
                if (!it.isValid()) {
                    throw new RocksDBException("Error while searching for " + key + " value with " + seekFor + " method.");
                }
                return getString(key, seekFor, it);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private String getString(String key, String seekFor, RocksIterator it) throws RocksDBException {
        if (it.isValid()) {

            return
                    Objects.requireNonNull(SerializationUtils.deserialize(it.value())).toString()
                            .replace("\\\"", "\"");
        } else {
            throw new RocksDBException("Error while searching for " + key + " value with " + seekFor + " method.");
        }
    }

    public void resetDb() {
        db.close();
    }
}
