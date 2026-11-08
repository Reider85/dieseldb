package diesel.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.classic.Level;
import org.slf4j.Marker;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * TurboFilter for redacting sensitive information from log messages.
 * 
 * <p>Prevents secret leakage by detecting and redacting patterns like:
 * - tls.keystore.password, tls.truststore.password
 * - *.secret, *.token, *.password
 * - privateKey, passwd
 * 
 * <p>When redaction is enabled, this filter intercepts log messages and
 * replaces sensitive values with "<redacted>" or blocks the message entirely.
 */
public class DieselRedactionTurboFilter extends TurboFilter {
    
    private static final Pattern SECRET_KEY_PATTERN = Pattern.compile(
        ".*(password|passwd|secret|token|privateKey|keystore\\.password|truststore\\.password).*"
    );
    
    private static final Pattern CONFIG_VALUE_PATTERN = Pattern.compile(
        "Config: (.+) = (.+)"
    );
    
    private static final Pattern TLS_PASSWORD_PATTERN = Pattern.compile(
        "tls\\..*password|audit\\.archive\\.dir"
    );
    
    private boolean enabled = true;
    
    @Override
    public FilterReply decide(Marker marker, ch.qos.logback.classic.Logger logger, Level level, String format, Object[] params, Throwable t) {
        if (!enabled) {
            return FilterReply.NEUTRAL;
        }
        
        String message = format != null ? format : "";
        
        // Check for config values that might contain secrets
        if (message.contains("Config:")) {
            return handleConfigMessage(message, params);
        }
        
        // Check for general secret patterns
        if (SECRET_KEY_PATTERN.matcher(message).matches()) {
            return FilterReply.DENY;
        }
        
        // Check for specific TLS secrets
        if (TLS_PASSWORD_PATTERN.matcher(message).matches()) {
            return FilterReply.DENY;
        }
        
        return FilterReply.NEUTRAL;
    }
    
    /**
     * Handles config messages by redacting sensitive values.
     * For messages like "Config: key = value", redacts the value if it's sensitive.
     */
    private FilterReply handleConfigMessage(String message, Object[] params) {
        Matcher matcher = CONFIG_VALUE_PATTERN.matcher(message);
        if (matcher.matches() && params != null && params.length > 0) {
            String key = matcher.group(1);
            Object valueObj = params[0];
            
            if (valueObj != null) {
                String value = valueObj.toString();
                
                // Check if the key indicates a sensitive value
                if (isSensitiveKey(key)) {
                    // Replace the parameter with redacted value
                    if (params.length > 0) {
                        params[0] = "<redacted>";
                    }
                    return FilterReply.NEUTRAL; // Allow message but with redacted value
                }
            }
        }
        
        return FilterReply.NEUTRAL;
    }
    
    /**
     * Checks if a config key indicates sensitive information.
     */
    private boolean isSensitiveKey(String key) {
        String lowerKey = key.toLowerCase();
        return lowerKey.contains("password") ||
               lowerKey.contains("passwd") ||
               lowerKey.contains("secret") ||
               lowerKey.contains("token") ||
               lowerKey.contains("privatekey") ||
               lowerKey.contains("keystore.password") ||
               lowerKey.contains("truststore.password") ||
               lowerKey.contains("archive.dir");
    }
    
    @Override
    public void start() {
        super.start();
        this.enabled = true;
    }
    
    @Override
    public void stop() {
        super.stop();
        this.enabled = false;
    }
    
    /**
     * Enables or disables redaction.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}