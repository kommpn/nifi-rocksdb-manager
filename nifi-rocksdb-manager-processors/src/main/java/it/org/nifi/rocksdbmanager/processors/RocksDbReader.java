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

import java.nio.charset.StandardCharsets;
import java.util.*;

import static it.org.nifi.rocksdbmanager.utils.PropertyDescriptorUtils.*;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"rocksdb", "reader"})
@CapabilityDescription("Processor that allows user to read key/values inside a RocksDb using a RocksDbService.")
@SeeAlso({RocksDbReader.class, RocksDbServiceRocks.class})
@WritesAttributes({@WritesAttribute(attribute = "rocksdb.search.key",
        description = "Key found inside the RocksDb with given key."),
        @WritesAttribute(attribute = "rocksdb.search.value", description = "Value found inside the RocksDb with given key.")})
public class RocksDbReader extends AbstractProcessor {

    public static final PropertyDescriptor ROCKSDB_SERVICE = new PropertyDescriptor
            .Builder().name("rocksdb-service")
            .displayName("RocksDB Service")
            .description("Designed Rocksdb to utilize")
            .required(true)
            .identifiesControllerService(RocksDbRocksLookupService.class)
            .build();

    public static final PropertyDescriptor SEARCH_TYPE = new PropertyDescriptor
            .Builder().name("search-type")
            .displayName("Search Type")
            .description("The modality to search with.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(Set.of(FIND, ITERATOR))
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

    public static final PropertyDescriptor SEEK_FOR = new PropertyDescriptor
            .Builder().name("seek-for")
            .displayName("Seek For")
            .description("Necessary only if \"Iterator\" has been choosed as \"Search Type\".")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(Set.of(SEEK_PREV, SEEK_NEXT, SEEK_FOR_PREV))
            .dependsOn(SEARCH_TYPE, ITERATOR)
            .build();

    public static final PropertyDescriptor RESULT_DESTINATION = new PropertyDescriptor
            .Builder().name("result-destination")
            .displayName("Result Destination")
            .description("Destination where to write result of the rocksdb read.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(Set.of(FLOWFILE_ATTRIBUTE, FLOWFILE_CONTENT))
            .build();

    private final Set<Relationship> relationships = Set.of(REL_SUCCESS,
            REL_FAILURE, REL_UNMATCHED);

    protected volatile RocksDbRocksLookupService lookupService;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.lookupService = context.getProperty(ROCKSDB_SERVICE)
                .asControllerService(RocksDbRocksLookupService.class);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ROCKSDB_SERVICE);
        properties.add(RESULT_DESTINATION);
        properties.add(SEARCH_TYPE);
        properties.add(KEY);
        properties.add(SEEK_FOR);
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        String searchMethod = context.getProperty(SEARCH_TYPE).getValue();
        String resultDestination = context.getProperty(RESULT_DESTINATION).getValue();


        Optional<?> lookupResultOptional;
        try {
            HashMap<String, Object> coordinates = new HashMap<>() {{
                put("key", key);
                put("searchMethod", searchMethod);
            }};
            if (ITERATOR.equals(searchMethod)) {
                coordinates.put("seekFor", context.getProperty(SEEK_FOR).getValue());

            }
            lookupResultOptional = lookupService.search(coordinates);
        } catch (LookupFailureException e) {
            session.transfer(flowFile, REL_FAILURE);
            throw new RuntimeException(e);
        }
        if (lookupResultOptional.isEmpty()) {
            session.transfer(flowFile, REL_UNMATCHED);
            return;
        }


        String lookupResult = (String) lookupResultOptional.get();

        if (FLOWFILE_CONTENT.equals(resultDestination)) {

            session.putAttribute(flowFile, "rocksdb.search.key", key);
            session.write(flowFile, outputStream -> {
                outputStream.write(lookupResult.getBytes(StandardCharsets.UTF_8));
            });
            session.transfer(flowFile, REL_SUCCESS);
        } else if (FLOWFILE_ATTRIBUTE.equals(resultDestination)) {
            session.putAttribute(flowFile, "rocksdb.search.key", key);
            session.putAttribute(flowFile, "rocksdb.search.value", lookupResult);
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

}
