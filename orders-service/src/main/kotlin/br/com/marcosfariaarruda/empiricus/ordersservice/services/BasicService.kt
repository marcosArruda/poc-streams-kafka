package br.com.marcosfariaarruda.empiricus.ordersservice.services

import org.apache.kafka.streams.KafkaStreams
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

open class BasicService {
    open fun boilerplateStart(kafkaStreams: KafkaStreams, startLatch: CountDownLatch=CountDownLatch(1)): KafkaStreams {
        kafkaStreams.cleanUp()
        kafkaStreams.setStateListener { newState, oldState -> if(newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) startLatch.countDown()}
        kafkaStreams.start()
        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw RuntimeException("Streams never finished rebalancing on startup")
            }
        }catch (e:InterruptedException){
            Thread.currentThread().interrupt()
        }
        return kafkaStreams
    }
}