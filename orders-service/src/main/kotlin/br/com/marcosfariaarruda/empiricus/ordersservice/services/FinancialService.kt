package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.WalletsBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.springframework.stereotype.Service

@Service
class FinancialService {

    private val wallets: WalletsBox = WalletsBox()

    private lateinit var createdStream: KStream<String, Order>
    private lateinit var validatedStream: KStream<String, Order>

    fun init(stream: KStream<String, Order>) {
        this.createdStream = stream.filter { _, order -> order.state == "CREATED"}
        this.validatedStream = stream.filter { _, order -> order.state == "VALIDATED"}
        preStreams()
        finalStreams()
    }

    fun preStreams() {
        val approvedStream = createdStream.filter { _, order -> canProceed(order) }
        val unapproovedStream = createdStream.filter { _, order -> !canProceed(order) }

        approvedStream
                .map { key, order -> KeyValue(key, approvePass(order))  }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, OrdersValidationService.producedStringOrderAnalysis)

        unapproovedStream
                .peek {key, _ -> println("[FinancialService-PRE] unapproved $key") }
                .map { key, order -> KeyValue(key, blockPass(order))  }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, OrdersValidationService.producedStringOrderAnalysis)
    }

    fun finalStreams() {
        validatedStream
                .peek{_, order -> println("[FinancialService-FINAL] transacting payment{totalPrice=${order.product.price * order.quantity}} for product=${order.product} ")}
                .foreach { _, order -> wallets.transactDebit(order.user, order.product.price * order.quantity) }
        /*
        validatedStream
                .filter { _, order -> canProceed(order) }
                .peek{_, order -> wallets.transactDebit(order.user, order.product.price.toLong())}
                .map { key, order ->  KeyValue("${key}_FINISHED_FinancialService", order.copy(state = "FINISHED"))}
                .to(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.producedStringOrder)
        */

        //TODO: Continuar a partir daqui.
    }

    private fun canProceed(order: Order): Boolean = wallets.hasCredit(order.user, order.product.price.toLong())

    private fun approvePass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, true)

    private fun blockPass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, false)
}