package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysisSerde
import br.com.marcosfariaarruda.empiricus.model.UserBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.stereotype.Service

@Service
class FraudService {

    private val users: UserBox = UserBox()

    private lateinit var createdStream: KStream<String, Order>
    private lateinit var validatedStream: KStream<String, Order>

    fun init(createdStream: KStream<String, Order>, validatedStream: KStream<String, Order>): KStream<String, Order> {
        this.createdStream = createdStream
        this.validatedStream = validatedStream
        preStream()
        return this.createdStream
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