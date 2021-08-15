package com.geo.learn

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription


class ArrayPublisher<T : Any?> : Publisher<T> {
    private val longArray: Array<T>

    constructor(longArray: Array<T>) {
        this.longArray = longArray
    }

    override fun subscribe(s: Subscriber<in T>?) {
        s?.onSubscribe(object : Subscription {
            var index: Int = 0
            var requested:Long=0L
            var cancelled:Boolean=false
            override fun request(n: Long) {
                if(n<=0 && !cancelled){
                    cancel()
                    s.onError(IllegalArgumentException("Error: negative argument not allowed"))
                }
                if(cancelled){
                    return
                }
                var initialRequested:Long=requested
                requested+=n
                var sent:Long=0L
                if(initialRequested != 0L){
                    return
                }

                while (sent<requested) {
                    if (index > longArray.size - 1) {
                        break
                    }
                    val t = longArray[index]
                    if(t==null){
                        s?.onError(NullPointerException())
                        return
                    }
                    index++
                    sent++
                    s?.onNext(t)
                }
                requested-=sent
                if (index == longArray.size) {
                    s?.onComplete()
                    return
                }
            }

            override fun cancel() {
                cancelled=true
            }

        })
    }
}