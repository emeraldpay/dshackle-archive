package io.emeraldpay.dshackle.archive.runner

import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class ProgressIndicator(
    private val n: Int = 5,
    private val opLabel: String = "op",
) {

    companion object {
        private val log = LoggerFactory.getLogger(ProgressIndicator::class.java)
    }

    private val counter = AtomicInteger(0)
    private val history = AtomicReference<List<HistoryItem>>(emptyList())
    private val timer = AtomicLong(System.currentTimeMillis())

    private val period = Duration.ofSeconds(60)

    fun start() {
        val time = timer.updateAndGet { System.currentTimeMillis() }
        history.set(listOf(HistoryItem(0, time)))
    }

    fun onNext() {
        val value = counter.incrementAndGet()
        // no reason to check for progress very often, so skip 95% updates
        if (value % 20 != 0) {
            return
        }
        val time = System.currentTimeMillis()
        val timeUpdated = timer.updateAndGet {
            if (it < time - period.toMillis()) {
                time
            } else {
                it
            }
        }
        if (timeUpdated == time) {
            mark(time)
        }
    }

    private fun mark(moment: Long) {
        val count = counter.get()
        val history = history.updateAndGet {
            val head = if (it.size < n) {
                it
            } else {
                it.drop(1)
            }
            val next = HistoryItem(count, moment)
            if (head.contains(next)) {
                head
            } else {
                head + listOf<HistoryItem>(next)
            }
        }
        if (history.size <= 1) {
            return
        }
        val countDelta = history.last().count - history.first().count
        val timeDelta = history.last().time - history.first().time
        val minutes = timeDelta / 60_000L
        if (minutes == 0L) {
            return
        }
        val opsPerMin = countDelta / minutes
        log.info("Processing: $opsPerMin $opLabel/min")
    }

    data class HistoryItem(
        val count: Int,
        val time: Long,
    )
}
