package com.geo.learn

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.internal.matchers.Null
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment
import java.lang.NullPointerException
import java.util.ArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.LongStream
import java.util.stream.LongStream.range
import kotlin.streams.toList

internal class ArrayPublisherTest {

    @Test
    fun everyMethodInSubscriberIsExecutedInParticularOrder() {
        val countDownLatch: CountDownLatch = CountDownLatch(1)
        val observedSignals: ArrayList<String> = arrayListOf<String>()
        val arrayPublisher: ArrayPublisher<Long> = ArrayPublisher<Long>(longArray(5))
        val subscriber = object : Subscriber<Long> {
            override fun onSubscribe(s: Subscription?) {
                observedSignals.add("onSubscribe")
                s?.request(10)
            }

            override fun onNext(t: Long?) {
                observedSignals.add("onNext $t")
            }

            override fun onError(t: Throwable?) {
                observedSignals.add("onError")
            }

            override fun onComplete() {
                observedSignals.add("onComplete")
                countDownLatch.countDown()
            }

        }
        arrayPublisher.subscribe(subscriber)
        assertThat(countDownLatch.await(1000, TimeUnit.MILLISECONDS)).isTrue
        assertThat(observedSignals).containsExactly(
            "onSubscribe",
            "onNext 0",
            "onNext 1",
            "onNext 2",
            "onNext 3",
            "onNext 4",
            "onComplete"
        )

    }

    @Test
    fun mustSupportBackPressure(){
        val countDownLatch=CountDownLatch(1)
        val collected:ArrayList<Long?> = ArrayList()
        val array:Array<Long> = longArray(5)
        val publisher:ArrayPublisher<Long> = ArrayPublisher(array)
        val subscriptionArray: Array<Subscription?> = arrayOfNulls<Subscription>(1)
        publisher.subscribe(object : Subscriber<Long>{
            override fun onSubscribe(s: Subscription?) {
               subscriptionArray[0]=s
            }

            override fun onNext(t: Long?) {
                collected.add(t)
            }

            override fun onError(t: Throwable?) {
                TODO("Not yet implemented")
            }

            override fun onComplete() {
                //In this example we can see the complete method getting called twice. But
                //as per reative stream specification ,it is not allowed to call any method on subscriber
                // once error or complete happens
                println("oncomplete")
                countDownLatch.countDown()
            }

        })
        assertThat(collected).isEmpty()
        subscriptionArray[0]?.request(1)
        assertThat(collected).containsExactly(0L)
        subscriptionArray[0]?.request(1)
        assertThat(collected).containsExactly(0L,1L)
        subscriptionArray[0]?.request(2)
        assertThat(collected).containsExactly(0L,1L,2L,3L)
        subscriptionArray[0]?.request(20)
        assertThat(countDownLatch.await(1000, TimeUnit.MILLISECONDS)).isTrue
        subscriptionArray[0]?.request(20)
    }
    @Test
    fun nulIsNotAllowedToBeSendInOnNext(){
        val countDownLatch=CountDownLatch(1)
        var err:AtomicReference<Throwable> = AtomicReference()
        val array: Array<Long?> = arrayOfNulls<Long>(1)
        val publisher: ArrayPublisher<Long?> = ArrayPublisher(array)
        val subscriptionArray: Array<Subscription?> = arrayOfNulls<Subscription>(1)
        publisher.subscribe(object : Subscriber<Long?>{
            override fun onSubscribe(s: Subscription?) {
                subscriptionArray[0]=s
            }

            override fun onNext(t: Long?) {
            }

            /**
             * Say in another thread publisher is sending and encountering exception,
             * and subscription is happening in another thread.So the communication can
             * happen on error properly through below method only.
             */
            override fun onError(t: Throwable?) {
                err.set(t)
               countDownLatch.countDown()
            }

            override fun onComplete() {
            }

        })
        subscriptionArray[0]?.request(1)
        countDownLatch.await(1000,TimeUnit.MILLISECONDS)
        assertThat(err.get()).isInstanceOf(NullPointerException::class.java)
    }

    @Test
    fun shouldNotThrowStackOverflowError() {
        val countDownLatch = CountDownLatch(1)
        val collected: ArrayList<Long?> = ArrayList()
        val array: Array<Long> = longArray(10000000L)
        val publisher: ArrayPublisher<Long> = ArrayPublisher(array)
        var sub:Subscription?=null
        publisher.subscribe(object : Subscriber<Long> {
            override fun onSubscribe(s: Subscription?) {
                sub = s
                sub?.request(1)
            }

            override fun onNext(t: Long?) {
                collected.add(t)
                sub?.request(1)
            }

            override fun onError(t: Throwable?) {
                TODO("Not yet implemented")
            }

            override fun onComplete() {
                println("oncomplete")
                countDownLatch.countDown()
            }

        })
        countDownLatch.await(1000,TimeUnit.MILLISECONDS)
        assertEquals(collected,array.asList())
    }

    @Test
    fun shouldBePossibleToCancelSubscription() {
        val countDownLatch = CountDownLatch(1)
        val collected: ArrayList<Long?> = ArrayList()
        val array: Array<Long> = longArray(10000000L)
        val publisher: ArrayPublisher<Long> = ArrayPublisher(array)
        var sub:Subscription?=null
        publisher.subscribe(object : Subscriber<Long> {
            override fun onSubscribe(s: Subscription?) {
                s?.cancel()
                s?.request(1)
            }

            override fun onNext(t: Long?) {
                collected.add(t)
            }

            override fun onError(t: Throwable?) {
                TODO("Not yet implemented")
            }

            override fun onComplete() {
                countDownLatch.countDown()
            }

        })
        assertThat(countDownLatch.await(1000,TimeUnit.MILLISECONDS)).isFalse
        assertThat(collected).isEmpty()
    }
    @Test
    fun shouldNotAllowNegativeRequestValue() {
        val countDownLatch = CountDownLatch(1)
        val collected: ArrayList<Long?> = ArrayList()
        val array: Array<Long> = longArray(10000000L)
        val publisher: ArrayPublisher<Long> = ArrayPublisher(array)
        var err:AtomicReference<Throwable> = AtomicReference()
        var sub:Subscription?=null
        publisher.subscribe(object : Subscriber<Long> {
            override fun onSubscribe(s: Subscription?) {
                sub = s
                sub?.request(-1)
            }

            override fun onNext(t: Long?) {
                collected.add(t)
                sub?.request(-1)
            }

            override fun onError(t: Throwable?) {
                err.set(t)
                countDownLatch.countDown()

            }

            override fun onComplete() {
                println("oncomplete")
                countDownLatch.countDown()
            }

        })
        assertThat(countDownLatch.await(1000,TimeUnit.MILLISECONDS)).isTrue
        assertThat(collected).isEmpty()
        assertThat(err.get()).isInstanceOf(IllegalArgumentException::class.java)
    }
    companion object {
        fun longArray(num: Long): Array<Long> = range(0, setLimit(num)).toList().toTypedArray()

        private fun setLimit(num: Long): Long {
            return if (num >= Int.MAX_VALUE) 100000 else num
        }
    }

}