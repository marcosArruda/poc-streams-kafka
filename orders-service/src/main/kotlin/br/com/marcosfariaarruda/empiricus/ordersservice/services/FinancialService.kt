package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysisSerde
import br.com.marcosfariaarruda.empiricus.model.WalletsBox
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
class FinancialService : BasicService(){

    private val wallets: WalletsBox = WalletsBox()

    private lateinit var createdStream: KStream<String, Order>
    private lateinit var validatedStream: KStream<String, Order>

    fun init():KafkaStreams {
        val builder = StreamsBuilder()
        val orderStream = builder.stream<String, Order>(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType)
        this.createdStream = orderStream.filter{_, order -> order.state == "CREATED"}
        this.validatedStream = orderStream.filter{ _, order -> order.state == "VALIDATED"}
        preStreams()
        finalStreams()
        val props = GlobalService.newConfigProperties("FINANCIAL")
        val topology = builder.build(props)
        println("======================================================================================================")
        println(topology.describe().toString())
        println("======================================================================================================")
        val kafkaStreams = KafkaStreams(topology, props)
        return boilerplateStart(kafkaStreams)
    }

    fun preStreams() {
        createdStream
                .mapValues{ _, order ->
                    println(">>[FinancialService-PRE] validating")
                    if(canProceed(order)) approvePass(order) else blockPass(order)
                }
                .to(GlobalFuckingTopology.ORDER_VALIDATIONS, Produced.with(Serdes.String(), OrderAnalysisSerde()))
    }

    fun finalStreams() {
        validatedStream
                //.peek{_, order -> println("[FinancialService-FINAL] transacting payment{totalPrice=${order.product.price * order.quantity}} for product=${order.product} ")}
                .mapValues { _, order ->
                    wallets.transactDebit(order.user, order.product.price * order.quantity)
                    return@mapValues order.copy(state = "FINISHED")
                }
                .to(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.producedStringOrder)
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

    private fun approvePass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), "FinancialService", order.id, true)

    private fun blockPass(order: Order): OrderAnalysis = OrderAnalysis(OrderProducer.randLong(), "FinancialService", order.id, false)
}