package it.org.nifi.rocksdbmanager.utils;

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


import it.org.nifi.rocksdbmanager.exception.LookupFailureException;
import org.apache.nifi.controller.ControllerService;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface LookupService<T> extends ControllerService {

    /**
     * Looks up a value that corresponds to the given map of information, referred to as lookup coordinates
     *
     * @param coordinates a Map of key/value pairs that indicate the information that should be looked up
     * @return a value that corresponds to the given coordinates
     *
     * @throws LookupFailureException if unable to lookup a value for the given coordinates
     */
    Optional<T> lookup(Map<String, Object> coordinates) throws LookupFailureException;

}