package it.org.nifi.rocksdbmanager.utils;


import it.org.nifi.rocksdbmanager.exception.LookupFailureException;
import org.apache.nifi.util.Tuple;

import java.util.Map;
import java.util.Optional;

public interface RocksDbLookupService extends LookupService<Tuple<String, String>> {

    Optional<Tuple<String, String>> lookup(Map<String, Object> var1) throws LookupFailureException;


}
