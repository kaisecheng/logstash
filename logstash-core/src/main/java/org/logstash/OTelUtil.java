package org.logstash;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.logging.log4j.ThreadContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("try")
public class OTelUtil {
    // Get the global OpenTelemetry instance configured by the agent
    public static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();

    public static final Tracer tracer = openTelemetry.getTracer("Logstash");

    private static final W3CTraceContextPropagator PROPAGATOR = W3CTraceContextPropagator.getInstance();
    private static final MapTextMapSetter CONTEXT_SETTER = new MapTextMapSetter();
    private static final MapTextMapGetter CONTEXT_GETTER = new MapTextMapGetter();

    @FunctionalInterface
    public interface SpanRunnable {
        void run(Span span);
    }

    @FunctionalInterface
    public interface SpanSupplier<T,E extends Throwable> {
        T get() throws E;
    }

    public static Span newSpan(String name) {
        return tracer.spanBuilder(name).startSpan();
    }

    public static Map<String, String> getCurrentContextAsMap() {
        Map<String, String> kv = new HashMap<>();
        PROPAGATOR.inject(Context.current(), kv, CONTEXT_SETTER);
        return kv;
    }

    public static Context extractTraceContext(Map<String, String> headers) {
        return PROPAGATOR.extract(Context.current(), headers, CONTEXT_GETTER);
    }

    public static void withSpan(String spanName, SpanRunnable operation) {
        Span span = tracer.spanBuilder(spanName).startSpan();
        try (Scope scope = span.makeCurrent()) {
            operation.run(span);
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    public static void withParentSpan(final String spanName, final Event event, SpanRunnable operation) {
        @SuppressWarnings("unchecked")
        Span span = Optional.ofNullable(event.getField(Event.TRACE))
                .filter(Map.class::isInstance)
                .map(raw -> (Map<String, String>) raw)
                .map(OTelUtil::extractTraceContext)
                .map(parentContext -> tracer.spanBuilder(spanName)
                        .setParent(parentContext)
                        .startSpan())
                .orElseGet(() -> tracer.spanBuilder(spanName)
                        .setSpanKind(SpanKind.PRODUCER)
                        .startSpan());

        try (Scope scope = span.makeCurrent()) {
            operation.run(span);
        } finally {
            span.end();
        }
    }

    public static <V, E extends Exception> V withParentSpan(final String spanName, final Event event,
                                                            final SpanSupplier<V, E> supplier) throws E {
        @SuppressWarnings("unchecked")
        Span span = Optional.ofNullable(event.getField(Event.TRACE))
                .filter(Map.class::isInstance)
                .map(raw -> (Map<String, String>) raw)
                .map(OTelUtil::extractTraceContext)
                .map(parentContext -> tracer.spanBuilder(spanName)
                        .setParent(parentContext)
                        .startSpan())
                .orElseGet(() -> newSpan(spanName));

        try (Scope scope = span.makeCurrent()) {
            return supplier.get();
        } finally {
            span.end();
        }
    }

    public static String rawEventSpanName() {
        return String.format("%s.%s", ThreadContext.get("pipeline.id"),
                ThreadContext.get("plugin.shortname") == null ? "new.event" : ThreadContext.get("plugin.shortname"));
    }

    public static String eventSpanName() {
        return String.format("%s.%s", ThreadContext.get("pipeline.id"),
                ThreadContext.get("plugin.shortname") == null ? "queue.recreate.event" : ThreadContext.get("plugin.shortname"));
    }

    private static class MapTextMapSetter implements TextMapSetter<Map<String, String>> {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            if (carrier != null && key != null && value != null) {
                carrier.put(key, value);
            }
        }
    }

    private static class MapTextMapGetter implements TextMapGetter<Map<String, String>> {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier != null ? carrier.keySet() : java.util.Collections.emptyList();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier != null ? carrier.get(key) : null;
        }
    }
}
