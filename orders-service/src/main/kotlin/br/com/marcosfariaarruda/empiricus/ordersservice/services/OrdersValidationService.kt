package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysisSerde
import br.com.marcosfariaarruda.empiricus.model.OrderSerde
import br.com.marcosfariaarruda.empiricus.ordersservice.api.util.Contador
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Service
class OrdersValidationService : BasicService(){

    private val numberOfRules: Int = 3
    private lateinit var createdStream: KStream<String, Order>
    private lateinit var analysisStream: KStream<String, OrderAnalysis>

    //TODO: LIGAR E DESLIGAR O COMPRADOR AUTOMATICO
    //TODO:

    companion object {
        val stringSerde: Serde<String> = Serdes.String()
        val groupedStringOrderAnalysis:Grouped<String, OrderAnalysis> = Grouped.with(stringSerde, OrderAnalysisSerde())
        val groupedStringOrder:Grouped<String, Order> = Grouped.with(stringSerde, OrderSerde())
        val materializedStringOrder:Materialized<String, Order, KeyValueStore<Bytes, ByteArray>> = Materialized.with(stringSerde, OrderSerde())
        val sessionWindow5min:SessionWindows = SessionWindows.with(Duration.ofMinutes(5))
        val sessionWindow1min:SessionWindows = SessionWindows.with(Duration.ofMinutes(1))
        val sessionWindow15s:SessionWindows = SessionWindows.with(Duration.ofSeconds(15))
        val sessionWindow3s:SessionWindows = SessionWindows.with(Duration.ofSeconds(3))
        val sessionWindow5s:SessionWindows = SessionWindows.with(Duration.ofSeconds(5))
        val joinWindow5min:JoinWindows = JoinWindows.of(Duration.ofMinutes(5))
        val joinWindow1min:JoinWindows = JoinWindows.of(Duration.ofMinutes(1))
        val joinWindow15s:JoinWindows = JoinWindows.of(Duration.ofSeconds(15))
        val joinWindow3s:JoinWindows = JoinWindows.of(Duration.ofSeconds(3))
        val joinWindow5s:JoinWindows = JoinWindows.of(Duration.ofSeconds(5))
        val joinedStringLongOrder:Joined<String, Long, Order> = Joined.with(stringSerde, Serdes.Long(), OrderSerde())
        val producedStringOrder:Produced<String, Order> = Produced.with(stringSerde, OrderSerde())
        val producedStringOrderAnalysis:Produced<String, OrderAnalysis> = Produced.with(stringSerde, OrderAnalysisSerde())
        val joinedStringAnalysisOrder:Joined<String, OrderAnalysis, Order> = Joined.with(stringSerde, OrderAnalysisSerde(), OrderSerde())
        val consumedOrdersType:Consumed<String, Order> = Consumed.with(stringSerde, OrderSerde())
        val consumedOrderAnalysisType:Consumed<String, OrderAnalysis> = Consumed.with(stringSerde, OrderAnalysisSerde())
        val validationMapMemory:MutableMap<String, Contador> = mutableMapOf()
    }

    fun init(): KafkaStreams {
        val builder = StreamsBuilder()
        this.createdStream = builder.stream(GlobalFuckingTopology.ORDERS_TOPIC, consumedOrdersType).filter{_, order -> order.state == "CREATED"}
        this.analysisStream = builder.stream(GlobalFuckingTopology.ORDER_VALIDATIONS, consumedOrderAnalysisType)
        finalStreams()
        val props = GlobalService.newConfigProperties("ANALYSIS-VALIDATIONS")
        val topology = builder.build(props)
        println("======================================================================================================")
        println(topology.describe().toString())
        println("======================================================================================================")
        val kafkaStreams = KafkaStreams(topology, props)

        return boilerplateStart(kafkaStreams)
    }

    private fun finalStreams(){
        analysisStream
                /*
                .peek{key, _ ->
                    if(validationMapMemory.containsKey(key)){
                        validationMapMemory[key]!!.qtd.inc()
                        if(validationMapMemory[key]!!.qtd == 3){
                            validationMapMemory[key] = validationMapMemory[key]!!.copy(finished = true)
                        }
                    }else{
                        validationMapMemory[key] = Contador()
                    }}
                .filter{ key, _ -> validationMapMemory[key]!!.finished}
                .join(streamOrders, { _, order -> order.copy(state = "VALIDATED" )}, joinWindow3s, joinedStringAnalysisOrder)
                //.peek{key, _ -> println("[OrdersValidationService] VALIDATED $key")}
                .to(GlobalFuckingTopology.ORDERS_TOPIC, producedStringOrder)
                */

                //.peek { key, value ->  println("111111111>> $key -> $value")}
                .groupByKey(groupedStringOrderAnalysis)
                .windowedBy(sessionWindow5min)
                .aggregate({ 0 }, { _, value, total -> if (value.pass) total.inc() else total }, { _, a, b -> b ?: a }, Materialized.with(null, Serdes.Long()))
                .toStream {windowedKey, _ -> windowedKey.key()}
                //.peek { key, value ->  println("222222222>> $key -> $value")}
                .filter {_, v -> v != null}
                //.peek{key, total -> println(">>>>>>>>>> $key has $total validations complete")}
                .filter { _, total -> total >= numberOfRules}
                .join(this.createdStream, { _, order -> order.copy(state = "VALIDATED" )}, joinWindow5min, joinedStringLongOrder)
                //.peek{key, _ -> println("[OrdersValidationService] VALIDATED $key")}
                .to(GlobalFuckingTopology.ORDERS_TOPIC, producedStringOrder)



        //failed results
        //analysis
        /*
        .filter { _, rule -> !rule.pass}
        .join(this.createdStream, {analysed, order -> order.copy(state = "FAILED: ${analysed.analysedFrom}").also { shit -> shit.analysis.add(analysed) }}, joinWindow15s, joinedStringAnalysisOrder)
        .groupByKey(groupedStringOrder)
        .reduce { order, _ -> order}
        .toStream().peek{key, _ -> println("[OrdersValidationService] FAILED $key")}
        .to(GlobalFuckingTopology.ORDERS_TOPIC, producedStringOrder)
        */
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