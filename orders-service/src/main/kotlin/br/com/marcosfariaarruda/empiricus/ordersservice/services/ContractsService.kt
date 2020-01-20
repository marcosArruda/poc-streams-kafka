package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.ContractsBox
import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.WalletsBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.streams.kstream.KStream
import org.springframework.stereotype.Service

@Service
class ContractsService {

    //TODO: Finalizar o Contracts
    private val contracts: ContractsBox = ContractsBox()

    private lateinit var stream: KStream<String, Order>

    fun init(stream: KStream<String, Order>): KStream<String, Order> {
        this.stream = stream
        //prepareStream()
        return this.stream
    }

    /*
    fun prepareStream() {
        val approvedStream = stream.filter { _, order -> canProceed(order) }
        val unapproovedStream = stream.filterNot { _, order -> canProceed(order) }

        approvedStream
                .mapValues { order -> approvePass(order) }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS)

        unapproovedStream
                .mapValues { order -> blockPass(order) }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS)
    }

    private fun canProceed(order: Order): Boolean = wallets.hasCredit(order.user, order.product.price.toLong())

    private fun approvePass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, true)

    private fun blockPass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), this.javaClass.simpleName, order.id, false)
    */
}