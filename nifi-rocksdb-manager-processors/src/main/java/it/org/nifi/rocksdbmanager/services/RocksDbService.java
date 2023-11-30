package it.org.nifi.rocksdbmanager.services;

import it.org.nifi.rocksdbmanager.exception.LookupFailureException;
import it.org.nifi.rocksdbmanager.processors.RocksDbReader;
import it.org.nifi.rocksdbmanager.processors.RocksDbWriter;
import it.org.nifi.rocksdbmanager.utils.RocksDbLookupService;
import it.org.nifi.rocksdbmanager.utils.RocksDbUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static it.org.nifi.rocksdbmanager.utils.PropertyDescriptorUtils.*;

@Tags({"rocksdb", "service"})
@CapabilityDescription("Service that opens a RocksDb and allows user to interact with it, reading and writing values.")
@SeeAlso({RocksDbReader.class, RocksDbWriter.class})
public class RocksDbService extends AbstractControllerService implements RocksDbLookupService {

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
            .description("Mode to open RocksDB. Choosing \"readOnly\" allows just reads on the Rocksdb." +
                    "\"everything\" mode allows read and writes inside the database.")
            .required(true)
            .defaultValue("everything")
            .allowableValues(Set.of("everything","readOnly"))
            .build();

    public RocksDbUtils rocksDbUtils;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DATABASE_PATH);
        properties.add(OPEN_MODE);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        String fileDirectory = context.getProperty(DATABASE_PATH).getValue();
        HashMap<String, String> dynamicProperties = new HashMap<>();
        for (PropertyDescriptor entry : context.getProperties().keySet()) {
            if(entry.isDynamic()) {
                dynamicProperties.put(entry.getName(), context.getProperty(entry).getValue());
            }
        }

        rocksDbUtils = new RocksDbUtils();

        try {
            if("everything".equals(context.getProperty(OPEN_MODE).getValue())) {
                rocksDbUtils.initDbWrite(fileDirectory, dynamicProperties);
            } else if("readOnly".equals(context.getProperty(OPEN_MODE).getValue())) {
                rocksDbUtils.initDbReadOnly(fileDirectory, dynamicProperties);
            } else {
                throw new RocksDBException("OpenMode not specified is not allowed.");
            }

        } catch (RocksDBException ex) {
            getLogger().error("RocksDB not initialized in {} mode, service cannot start.\n" +
                    "The error is:\n {}",context.getProperty(OPEN_MODE).getValue(), ex.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @OnDisabled
    public void onDisable() {
        rocksDbUtils.resetDb();
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

    @Override
    public Optional<Tuple<String, String>> lookup(Map<String, Object> coordinates) throws LookupFailureException {
       if(coordinates == null || coordinates.isEmpty()) {
           return Optional.empty();
       }

        try {

        if(!coordinates.containsKey("key")) {
            return Optional.empty();
        }
        String key = coordinates.get("key").toString();

        if(!coordinates.containsKey("action")) {
            return Optional.empty();
        }
        String action = coordinates.get("action").toString();
        if(action == null) {
            return Optional.empty();
        }
        if(ACTION_SEARCH.equals(action)) {

            if(!coordinates.containsKey("searchMethod")) {
                return Optional.empty();
            }

            String searchMethod = coordinates.get("searchMethod").toString();
            if(FIND.equals(searchMethod)) {

                  return Optional.ofNullable(rocksDbUtils.find(key));

            } else if (ITERATOR.equals(searchMethod)) {

                if(!coordinates.containsKey("seekFor")) {
                    return Optional.empty();
                }

                String seekFor = coordinates.get("seekFor").toString();
                return Optional.ofNullable(rocksDbUtils.findIterator(key, seekFor));
            }
        } else if(ACTION_WRITE.equals(action)) {

            if(!coordinates.containsKey("value")) {
                return Optional.empty();
            }

            String value = coordinates.get("value").toString();
            return Optional.ofNullable(rocksDbUtils.saveEntry(key.getBytes(StandardCharsets.UTF_8), value));

        }
        } catch (RocksDBException e) {
            throw new LookupFailureException(e);
        }
        return Optional.empty();
    }

}
