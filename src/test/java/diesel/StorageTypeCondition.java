package diesel;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;

/**
 * JUnit 5 {@link ExecutionCondition} that disables a test class when the
 * active {@code diesel.storage.type} does not match any of the values
 * declared in the {@link StorageType} annotation.
 *
 * <p>Resolution order for the current storage type:
 * <ol>
 *   <li>{@code System.getProperty("diesel.storage.type")}</li>
 *   <li>{@code ConfigLoader.getString("storage.type", "in_memory")}</li>
 * </ol>
 */
public class StorageTypeCondition implements ExecutionCondition {

    private static final String PROPERTY = "diesel.storage.type";

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        StorageType annotation = context.getRequiredTestClass().getAnnotation(StorageType.class);
        if (annotation == null) {
            return ConditionEvaluationResult.enabled("No @StorageType annotation");
        }

        String[] accepted = annotation.value();
        String current = System.getProperty(PROPERTY);
        if (current == null || current.isBlank()) {
            try {
                current = ConfigLoader.getString("storage.type", "in_memory");
            } catch (Exception e) {
                current = "in_memory";
            }
        }
        current = current.trim().toLowerCase();

        for (String allowed : accepted) {
            if (allowed.trim().toLowerCase().equals(current)) {
                return ConditionEvaluationResult.enabled(
                        "storage.type=" + current + " matches @StorageType" + Arrays.toString(accepted));
            }
        }
        return ConditionEvaluationResult.disabled(
                "storage.type=" + current + " does not match @StorageType" + Arrays.toString(accepted));
    }
}
