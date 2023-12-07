package it.org.nifi.rocksdbmanager.utils;


import it.org.nifi.rocksdbmanager.exception.LookupFailureException;

import java.util.Map;
import java.util.Optional;

public interface RocksDbRocksLookupService extends RocksLookupService<String> {

    Optional<String> search(Map<String, Object> var1) throws LookupFailureException;

    void write(Map<String, Object> var1) throws LookupFailureException;


}
