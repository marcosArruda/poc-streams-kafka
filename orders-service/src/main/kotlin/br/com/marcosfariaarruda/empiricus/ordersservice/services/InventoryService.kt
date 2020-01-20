package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.Product
import br.com.marcosfariaarruda.empiricus.model.ProductBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.springframework.stereotype.Service

@Service
class InventoryService {

    private val products = ProductBox()
    private lateinit var createdStream: KStream<String, Order>
    private lateinit var validatedStream: KStream<String, Order>

    fun init(stream: KStream<String, Order>){
        this.createdStream = stream.filter { _, order -> order.state == "CREATED"}
        this.validatedStream = stream.filter { _, order -> order.state == "VALIDATED"}
        preStreams()
        finalStreams()
    }

    fun preStreams() {
        val approvedStream = createdStream.filter { _, order -> canProceed(order) }
        val unapproovedStream = createdStream.filter{ _, order -> !canProceed(order) }

        approvedStream
                .map { key, order -> KeyValue(key, approvePass(order))  }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, OrdersValidationService.producedStringOrderAnalysis)

        unapproovedStream
                .peek {key, _ -> println("[InventoryService-PRE] unapproved $key") }
                .map { key, order -> KeyValue(key, blockPass(order))  }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, OrdersValidationService.producedStringOrderAnalysis)


    }

    fun finalStreams() {
        validatedStream
                .peek{ key, order -> print("[InventoryService-FINAL] decreasing ${order.quantity} of product'${order.product.name}' from order '$key';")}
                .foreach{_, order -> products.buyIn(order.product, order.quantity)}
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