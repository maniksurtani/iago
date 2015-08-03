/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.parrot.server

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{TimeUnit, CountDownLatch, LinkedBlockingQueue}

import com.twitter.finagle.Service
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.Stats
import com.twitter.parrot.util.{PrettyDuration, RequestDistribution}
import com.twitter.util.{Promise, Stopwatch}

class RequestConsumer[Req <: ParrotRequest, Rep](
    distributionFactory: Int => RequestDistribution,
    transport: Service[Req, Rep]
)
  extends Thread
{
  private[this] val log = Logger.get(getClass)
  private[this] val queue = new LinkedBlockingQueue[Req]()
  private[this] var rate: Int = 1
  private[this] val done = new CountDownLatch(1)
  @volatile private[this] var running = true

  val started = Promise[Unit]

  private[this] val process =
    new AtomicReference[RequestDistribution](distributionFactory(rate))

  private[server] var totalClockError = 0L

  var totalProcessed = 0

  private[this] val suspended = new AtomicBoolean(false)
  private[this] var statsName = ""

  def offer(request: Req) {
    if (running) {
      queue.offer(request)
    }
  }

  override def start() {
    super.start()
  }

  /* The feeder goes at full speed until the queue is full, so queue.take() takes a long time the
     first time, then takes no time after that. If we were starting the timer BEFORE starting the
     process of sending requests, we clock a "huge delay" (aka, time from when we start the clock to
     when we do queue.take()). The rate then tries to compensate for that, "rapid-firing" requests
     until the "clock error" is back to 0. By starting the clock AFTER we take our first request, we
     are getting rid of the false initial delay.
  */

  override def run() {
    log.trace("RequestConsumer: beginning run")
    try {
      val request = queue.take()
      started.setValue(())
      send(request, System.nanoTime())
      while (running) takeOne
    } catch {
      case e: InterruptedException =>
    }
    while (queue.size() > 0 && running) takeOne
    done.countDown()
  }

  private def send(request: Req, start: Long) {
    send(request)
    sleep(request.weight, start)
  }

  private def send(request: Req) {
    try {
      val future = transport(request)
      Stats.get(statsName).incr("requests_sent")
      future.respond { _ =>
        totalProcessed += 1
      }
    } catch {
      case t: Throwable =>
        log.error(t, "Exception sending request: %s", t)
    }
  }

  private def sleep(weight: Int, start: Long) {

    if (rate > 0) {
      val sum = (1 to weight).map { _ =>
        process.get.timeToNextArrival().inNanoseconds
      }.sum

      val waitTime = sum - totalClockError
      totalClockError = 0L
      waitForNextRequest(waitTime)


      totalClockError += System.nanoTime() - start - waitTime
    }
  }

  private def takeOne {

    synchronized {
      while (suspended.get()) {
        try {
          wait()
        } catch {
          case ie: InterruptedException =>
        }

        if (!running) return
      }
    }

    val start = System.nanoTime()
    val request = queue.poll(1, TimeUnit.SECONDS)
    if (request != null) {
      send(request, start)
    }
  }

  def pause() {
    suspended.set(true)
  }

  def continue() {
    synchronized {
      if (suspended.compareAndSet(true, false)) {
        notify()
      }
    }
  }

  def size = {
    queue.size
  }

  def clockError = {
    totalClockError
  }

  def setRate(newRate: Int) {
    rate = newRate
    process.set(distributionFactory(rate))
  }

  def setStatsName(name: String): Unit = {
    statsName = name
  }

  def shutdown: Unit = {
    val elapsed = Stopwatch.start()
    running = false
    continue()
    queue.clear()
    done.await()
    log.trace("RequestConsumer shut down in %s", PrettyDuration(elapsed()))
  }

  private[this] def waitForNextRequest(waitTime: Long) {
    val millis = waitTime / 1000000L
    val remainder = waitTime % 1000000L

    if (millis > 0) {
      Thread.sleep(millis)
      busyWait(remainder)
    } else if (waitTime < 0) {
      ()
    } else {
      busyWait(waitTime)
    }
  }

  /**
   * You can't Thread.sleep for less than a millisecond in Java
   */
  private[this] def busyWait(wait: Long) {
    val endAt = System.nanoTime + wait
    var counter = 0L
    var done = false
    while (!done) {
      counter += 1
      if (counter % 1000 == 0 && System.nanoTime > endAt) done = true
    }
  }
}
