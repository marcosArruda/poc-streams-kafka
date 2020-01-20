package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysisSerde
import br.com.marcosfariaarruda.empiricus.model.OrderSerde
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class OrdersValidationService {

    private val numberOfRules: Int = 3
    private lateinit var streamOrders: KStream<String, Order>

    //TODO: RECEBER REQUESTS DE NOVAS ORDERS E RESPONDER DE MANEIRA SINCRONA
    //TODO: RECEBER REQUESTS DE NOVAS ORDERS E RESPONDER DE MANEIRA ASYNC
    //TODO: LIGAR E DESLIGAR O COMPRADOR AUTOMATICO
    //TODO:

    companion object {
        val stringSerde: Serde<String> = Serdes.String()
        val groupedStringOrderAnalysis:Grouped<String, OrderAnalysis> = Grouped.with(stringSerde, OrderAnalysisSerde())
        val groupedStringOrder:Grouped<String, Order> = Grouped.with(stringSerde, OrderSerde())
        val sessionWindow5min:SessionWindows = SessionWindows.with(Duration.ofMinutes(5))
        val sessionWindow1min:SessionWindows = SessionWindows.with(Duration.ofMinutes(1))
        val joinWindow5min:JoinWindows = JoinWindows.of(Duration.ofMinutes(5))
        val joinWindow1min:JoinWindows = JoinWindows.of(Duration.ofMinutes(1))
        val joinedStringLongOrder:Joined<String, Long, Order> = Joined.with(stringSerde, Serdes.Long(), OrderSerde())
        val producedStringOrder:Produced<String, Order> = Produced.with(stringSerde, OrderSerde())
        val producedStringOrderAnalysis:Produced<String, OrderAnalysis> = Produced.with(stringSerde, OrderAnalysisSerde())
        val joinedStringAnalysisOrder:Joined<String, OrderAnalysis, Order> = Joined.with(stringSerde, OrderAnalysisSerde(), OrderSerde())
        val consumedOrdersType:Consumed<String, Order> = Consumed.with(stringSerde, OrderSerde())
        val consumedOrderAnalysisType:Consumed<String, OrderAnalysis> = Consumed.with(stringSerde, OrderAnalysisSerde())
    }

    fun init(orders: KStream<String, Order>, analysis: KStream<String, OrderAnalysis>): KStream<String, OrderAnalysis> {

        this.streamOrders = orders.filter { _, order -> order.state == "CREATED" }
        //success results
        analysis
                .groupByKey(groupedStringOrderAnalysis)
                .windowedBy(sessionWindow1min)
                .aggregate({ 0 }, { _, value, total -> if (value.pass) total.inc() else total }, { _, a, b -> b ?: a }, Materialized.with(null, Serdes.Long()))
                .toStream {windowedKey, _ -> windowedKey.key()}
                .filter {_, v -> v != null}
                .filter { _, total -> total >= numberOfRules}
                .join(streamOrders, { _, order -> order.copy(state = "VALIDATED" )}, joinWindow1min, joinedStringLongOrder)
                .to(GlobalFuckingTopology.ORDERS_TOPIC, producedStringOrder)

        //failed results
        analysis
                .filter { _, rule -> !rule.pass}
                .join(streamOrders, {analysed, order -> order.copy(state = "FAILED: ${analysed.analysedFrom}").also { shit -> shit.analysis.add(analysed) }}, joinWindow1min, joinedStringAnalysisOrder)
                .groupByKey(groupedStringOrder)
                .reduce { order, _ -> order}
                .toStream().to(GlobalFuckingTopology.ORDERS_TOPIC, producedStringOrder)
        return analysis
    }

    /*
    fun myAnalysisStream(): Order? {
        streamAnalysis
                .join(streamOrders, ValueJoiner<OrderAnalysis, Order, Order> { analysisDone, originalOrder ->
                    originalOrder.analysis.add(analysisDone)
                    return@ValueJoiner originalOrder
                }, JoinWindows.of(Duration.ofMinutes(5)), Joined.with(Serdes.StringSerde(), OrderAnalysisSerde(), OrderSerde()))
    }
    */
}