import diesel.storage.avro.AvroStatistics;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import java.util.ArrayList;
import java.util.List;

public class SimpleAvroTest {
    public static void main(String[] args) {
        // Create a simple schema
        Schema schema = Schema.createRecord("TestRecord", "Test record", "test", false,
                List.of(
                    new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
                    new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
                    new Schema.Field("value", Schema.create(Schema.Type.DOUBLE), null, null)
                )
        );
        
        // Create test data
        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            GenericRecord record = new org.apache.avro.generic.GenericData.Record(schema);
            record.put("id", (long) i);
            record.put("name", "name_" + i);
            record.put("value", i * 1.5);
            records.add(record);
        }
        
        // Test our optimized statistics collection
        long startTime = System.currentTimeMillis();
        var stats = AvroStatistics.collectColumnStatistics(records, schema);
        long endTime = System.currentTimeMillis();
        
        System.out.println("Statistics collected in " + (endTime - startTime) + "ms");
        System.out.println("Total columns processed: " + stats.size());
        System.out.println("Sample stats for 'id': " + stats.get("id"));
        System.out.println("Sample stats for 'name': " + stats.get("name"));
        System.out.println("Sample stats for 'value': " + stats.get("value"));
    }
}