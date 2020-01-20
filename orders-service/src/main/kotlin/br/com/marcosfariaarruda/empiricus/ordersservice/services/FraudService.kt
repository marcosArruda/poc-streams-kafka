package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.UserBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.springframework.stereotype.Service

@Service
class FraudService {

    private val users: UserBox = UserBox()

    private lateinit var stream: KStream<String, Order>

    fun init(stream: KStream<String, Order>): KStream<String, Order> {
        this.stream = stream.filter { _, order -> order.state == "CREATED"}
        preStream()
        return this.stream
    }

    fun preStream() {

        val approvedStream = stream.filter { _, order -> canProceed(order) }
        val unapproovedStream = stream.filter { _, order -> !canProceed(order) }

        approvedStream
                .map { key, order -> KeyValue(key, approvePass(order))  }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, OrdersValidationService.producedStringOrderAnalysis)

        unapproovedStream
                .peek {key, _ -> println("[FraudService-PRE] unapproved $key") }
                .map { key, order -> KeyValue(key, blockPass(order))  }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, OrdersValidationService.producedStringOrderAnalysis)
    }

    fun getRamdomUser() = users.getRandomUser()

    private fun canProceed(order: Order): Boolean = users.isFraud(order)

    private fun approvePass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, true)

    private fun blockPass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, false)
}