package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.*
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.stereotype.Service

@Service
class InventoryService {

    private val products = ProductBox()
    private lateinit var createdStream: KStream<String, Order>
    private lateinit var validatedStream: KStream<String, Order>

    fun init(createdStream: KStream<String, Order>, validatedStream: KStream<String, Order>){
        this.createdStream = createdStream
        this.validatedStream = validatedStream
        preStreams()
        finalStreams()
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

    fun getRamdomProduct(): Product = products.getRandomProduct()

    private fun canProceed(order: Order): Boolean = products.stockAvailable(order.product, order.quantity)

    private fun approvePass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, true)

    private fun blockPass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, false)
}