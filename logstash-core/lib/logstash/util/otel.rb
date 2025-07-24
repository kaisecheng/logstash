# frozen_string_literal: true


module LogStash; module Util; module OTel
  extend self

  # new span
  def with_span(name)
    begin
      span = Java::org.logstash.OTelUtil.newSpan(name)
      scope = span.makeCurrent
      yield
    ensure
      scope.close
      span.end
    end
  end

  # new span with parent context from event if available
  def with_parent_span(name, event, &block)
    begin
      trace_map = event.get(Java::org.logstash.Event::TRACE)
      span = if trace_map
        trace_context = Java::org.logstash.OTelUtil.extractTraceContext(trace_map)
        Java::org.logstash.OTelUtil.tracer
                        .spanBuilder(name)
                        .setParent(trace_context)
                        .startSpan
      else
        Java::org.logstash.OTelUtil.newSpan(name)
      end
      scope = span.makeCurrent
      block.call
    ensure
      scope.close
      span.end
    end
  end

  # take the context from the first event and use it as parent span for all events
  def events_with_parent_span(name, events, &block)
    return [] if events.nil? || (events.is_a?(Array) && events.empty?)
    with_parent_span(name, events.first, &block)
  end
end;end;end