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

    public static final String FIND = "Find";
    public static final String ITERATOR = "RocksIterator";
    public static final String ACTION_SEARCH = "search";
    public static final String ACTION_WRITE = "write";
    public static final String SEEK_PREV = "Seek Prev";
    public static final String SEEK_NEXT = "Seek Next";
    public static final String SEEK_FOR_PREV = "Seek For Prev";
}


