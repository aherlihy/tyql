package tyql

/** Some dialect operations are defined by polyfills to provide consistent semantics across dialects. This is tracked
  * when the documentation is generated, and for this purpose it should be enabled, but since it also has a small
  * performance impact, for normal releases it should be disabled.
  */
object PolyfillTracking {
  inline val shouldTrackPolyfillUsage = false
  var wasPolyfillUsed: Boolean = false

  inline def polyfillWasUsed(): Unit =
    inline if shouldTrackPolyfillUsage then
      wasPolyfillUsed = true
}
