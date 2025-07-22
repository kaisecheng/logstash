
package org.logstash;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

public class OTelUtil {
    public static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();

    public static final Tracer tracer = openTelemetry.getTracer("Logstash");

    public static Span newSpan(String name) {
        return tracer.spanBuilder(name).startSpan();
    }
}