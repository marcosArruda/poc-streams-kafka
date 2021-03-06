package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.*
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.stereotype.Service
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Service
class InventoryService : BasicService(){

    private val products = ProductBox()
    private lateinit var createdStream: KStream<String, Order>
    private lateinit var validatedStream: KStream<String, Order>

    fun init():KafkaStreams{
        val builder = StreamsBuilder()
        val orderStream = builder.stream<String, Order>(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType)
        this.createdStream = orderStream.filter{_, order -> order.state == "CREATED"}
        this.validatedStream = orderStream.filter{ _, order -> order.state == "VALIDATED"}
        preStreams()
        finalStreams()
        val props = GlobalService.newConfigProperties("INVENTORY")
        val topology = builder.build(props)
        println("======================================================================================================")
        println(topology.describe().toString())
        println("======================================================================================================")
        val kafkaStreams = KafkaStreams(topology, props)

        return boilerplateStart(kafkaStreams)
    }

    fun preStreams() {
        createdStream
                .mapValues { _, order ->
                    println(">>[InventoryService-PRE] validating")
                    if(canProceed(order)) approvePass(order) else blockPass(order)
                }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, Produced.with(Serdes.String(), OrderAnalysisSerde()))
    }

    fun finalStreams() {
        //validatedStream
        //        .peek{ key, order -> println("[InventoryService-FINAL] decreasing ${order.quantity} of product'${order.product.name}' from order '$key';")}
        //        .foreach{_, order -> products.buyIn(order.product, order.quantity)}
        /*
        validatedStream
                .filter { _, order -> canProceed(order) }
                .peek {_, order -> products.buyIn(order.product, order.quantity)}
        */
    }

    fun getRandomProduct(): Product = products.getRandomProduct()

    private fun canProceed(order: Order): Boolean = products.stockAvailable(order.product, order.quantity)

    private fun approvePass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), "InventoryService", order.id, true)

    private fun blockPass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), "InventoryService", order.id, false)
}