package brave.internal.recorder;

import brave.Clock;
import brave.Span.Kind;
import java.util.ArrayList;
import java.util.List;
import zipkin2.Endpoint;
import zipkin2.Span;

/**
 * One of these objects is allocated for each in-flight span, so we try to be parsimonious on things
 * like array allocation and object reference size.
 */
final class MutableSpan {
  final Clock clock;
  int kindIndex = -1;
  boolean shared;
  long timestamp, duration;
  String name;
  Endpoint remoteEndpoint;
  /**
   * To reduce the amount of allocation, collocate annotations with tags in a pair-indexed list.
   * This will be (timestamp, value) for annotations and (key, value) for tags.
   */
  List<Object> pairs = new ArrayList<>(4); // assume 2 tags and no annotations

  MutableSpan(Clock clock) {
    this.clock = clock;
  }

  void start() {
    start(clock.currentTimeMicroseconds());
  }

  synchronized void start(long timestamp) {
    this.timestamp = timestamp;
  }

  synchronized void name(String name) {
    this.name = name;
  }

  synchronized void kind(Kind kind) {
    this.kindIndex = kind.ordinal();
  }

  void annotate(String value) {
    annotate(clock.currentTimeMicroseconds(), value);
  }

  synchronized void annotate(long timestamp, String value) {
    // Modern instrumentation should not send annotations such as this, but we leniently
    // accept them rather than fail.
    if ("cs".equals(value)) {
      kind(Kind.CLIENT);
      this.timestamp = timestamp;
    } else if ("sr".equals(value)) {
      kind(Kind.SERVER);
      this.timestamp = timestamp;
    } else if ("cr".equals(value)) {
      kind(Kind.CLIENT);
      finish(timestamp);
    } else if ("ss".equals(value)) {
      kind(Kind.SERVER);
      finish(timestamp);
    } else {
      pairs.add(timestamp);
      pairs.add(value);
    }
  }

  synchronized void tag(String key, String value) {
    pairs.add(key);
    pairs.add(value);
  }

  synchronized void remoteEndpoint(Endpoint remoteEndpoint) {
    this.remoteEndpoint = remoteEndpoint;
  }

  synchronized void setShared() {
    shared = true;
  }

  /** Completes the span */
  synchronized void finish(long finishTimestamp) {
    if (timestamp != 0 && finishTimestamp != 0L) {
      duration = Math.max(finishTimestamp - timestamp, 1);
    }
  }

  // Since this is not exposed, this class could be refactored later as needed to act in a pool
  // to reduce GC churn. This would involve calling span.clear and resetting the fields below.
  synchronized void writeTo(zipkin2.Span.Builder result) {
    result.remoteEndpoint(remoteEndpoint);
    result.name(name);
    result.timestamp(timestamp);
    result.duration(duration);
    if (kindIndex != -1 && kindIndex < Span.Kind.values().length) { // defend against version skew
      result.kind(zipkin2.Span.Kind.values()[kindIndex]);
    }
    for (int i = 0, length = pairs.size(); i < length; i += 2) {
      Object first = pairs.get(i);
      String second = pairs.get(i + 1).toString();
      if (first instanceof Long) {
        result.addAnnotation((Long) first, second);
      } else {
        result.putTag(first.toString(), second);
      }
    }
    if (shared) result.shared(true);
  }
}
