package brave.propagation;

import brave.internal.Nullable;

//@Immutable
public abstract class SamplingFlags {
  public static final SamplingFlags EMPTY = new SamplingFlagsImpl(null, false);
  public static final SamplingFlags SAMPLED = new SamplingFlagsImpl(true, false);
  public static final SamplingFlags NOT_SAMPLED = new SamplingFlagsImpl(false, false);
  public static final SamplingFlags DEBUG = new SamplingFlagsImpl(true, true);

  /**
   * Sampled means send span data to Zipkin (or something else compatible with its data). It is a
   * consistent decision for an entire request (trace-scoped). For example, the value should not
   * move from true to false, even if the decision itself can be deferred.
   *
   * <p>Here are the valid options:
   * <pre><ul>
   *   <li>True means the trace is reported, starting with the first span to set the value true</li>
   *   <li>False means the trace should not be reported</li>
   *   <li>Null means the decision should be deferred to another span</li>
   * </ul></pre>
   *
   * <p>Once set to true or false, it is expected that this decision is propagated and honored
   * downstream.
   *
   * <p>Note: sampling does not imply the trace is invisible to others. For example, a common
   * practice is to generate and propagate identifiers always. This allows other systems, such as
   * logging, to correlate even when the tracing system has no data.
   */
  @Nullable public abstract Boolean sampled();

  /**
   * True is a request to store this span even if it overrides sampling policy. Defaults to false.
   */
  public abstract boolean debug();

  public static final class Builder {
    Boolean sampled;
    boolean debug = false;

    public Builder() {
      // public constructor instead of static newBuilder which would clash with TraceContext's
    }

    public Builder sampled(@Nullable Boolean sampled) {
      this.sampled = sampled;
      return this;
    }

    public Builder debug(boolean debug) {
      this.debug = debug;
      if (debug) sampled(true);
      return this;
    }

    /** Allows you to create flags from a boolean value without allocating a builder instance */
    public static SamplingFlags build(@Nullable Boolean sampled) {
      if (sampled != null) return sampled ? SAMPLED : NOT_SAMPLED;
      return EMPTY;
    }

    public SamplingFlags build() {
      if (debug) return DEBUG;
      return build(sampled);
    }
  }

  static final class SamplingFlagsImpl extends SamplingFlags {
    final Boolean sampled;
    final boolean debug;

    SamplingFlagsImpl(Boolean sampled, boolean debug) {
      this.sampled = sampled;
      this.debug = debug;
    }

    @Override public Boolean sampled() {
      return sampled;
    }

    @Override public boolean debug() {
      return debug;
    }

    @Override public String toString() {
      return "SamplingFlags(sampled=" + sampled + ", debug=" + debug + ")";
    }
  }

  SamplingFlags() {
  }

  static final int FLAG_SAMPLED = 1 << 1;
  static final int FLAG_SAMPLED_SET = 1 << 2;
  static final int FLAG_DEBUG = 1 << 3;

  static Boolean sampled(int flags) {
    return (flags & FLAG_SAMPLED_SET) == FLAG_SAMPLED_SET
        ? (flags & FLAG_SAMPLED) == FLAG_SAMPLED
        : null;
  }

  static boolean debug(int flags) {
    return (flags & FLAG_DEBUG) == FLAG_DEBUG;
  }
}
