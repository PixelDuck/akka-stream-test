package pixelduck.akkastreamtest

import java.util.concurrent.atomic.AtomicInteger

class Counters {
  val parsed = new AtomicInteger(0)
  val enriched = new AtomicInteger(0)
  val written = new AtomicInteger(0)
}
