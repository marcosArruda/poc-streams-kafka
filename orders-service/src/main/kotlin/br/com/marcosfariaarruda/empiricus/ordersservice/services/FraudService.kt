package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysisSerde
import br.com.marcosfariaarruda.empiricus.model.UserBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.stereotype.Service
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Service
class FraudService {

    private val users: UserBox = UserBox()

    private lateinit var createdStream: KStream<String, Order>
    private lateinit var validatedStream: KStream<String, Order>

    fun init(): KafkaStreams {
        val builder = StreamsBuilder()
        val orderStream = builder.stream<String, Order>(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType)
        this.createdStream = orderStream.filter{_, order -> order.state == "CREATED"}
        //this.validatedStream = orderStream.filter{ _, order -> order.state == "VALIDATED"}
        preStream()
        val props = GlobalService.newConfigProperties("FRAUD")
        val topology = builder.build(props)
        println("======================================================================================================")
        println(topology.describe().toString())
        println("======================================================================================================")

        val startLatch = CountDownLatch(1)
        val kafkaStreams = KafkaStreams(topology, props)

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

    fun preStream() {
        createdStream
                .mapValues { _, order ->
                    println(">>[FraudService-PRE] validating")
                    if(canProceed(order)) approvePass(order) else blockPass(order)
                }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, Produced.with(Serdes.String(), OrderAnalysisSerde()))
    }

    fun getRamdomUser() = users.getRandomUser()

    private fun canProceed(order: Order): Boolean = users.isFraud(order)

    private fun approvePass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, true)

    private fun blockPass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, false)
}