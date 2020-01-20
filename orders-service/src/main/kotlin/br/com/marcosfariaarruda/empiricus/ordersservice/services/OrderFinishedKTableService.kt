package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.WalletsBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.Stores
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.stereotype.Service
import reactor.core.publisher.toMono
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Service
class OrderFinishedKTableService {

    private lateinit var ordersTable: ReadOnlyKeyValueStore<String, Order>

    fun init():KafkaStreams {
        val builder = StreamsBuilder()
        builder.table(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType, Materialized.`as`(Stores.inMemoryKeyValueStore(GlobalFuckingTopology.MATERIALIZED_ORDERS)))
        val props = GlobalService.newConfigProperties("ORDERSTABLE")
        val topology = builder.build(props)
        println("======================================================================================================")
        println(topology.describe().toString())
        println("======================================================================================================")
        val kafkaStreams = KafkaStreams(topology, props)
        val startLatch = CountDownLatch(1)
        kafkaStreams.cleanUp()
        kafkaStreams.setStateListener { newState, oldState -> if(newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) startLatch.countDown()}
        kafkaStreams.start()

        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw RuntimeException("Streams never finished rebalancing on startup")
            }
        }catch (e:InterruptedException){
            Thread.currentThread().interrupt()
        }
        this.ordersTable = kafkaStreams.store(GlobalFuckingTopology.MATERIALIZED_ORDERS, QueryableStoreTypes.keyValueStore())
        return kafkaStreams
    }

    fun hasOrderFinished(order:Order):Boolean{
        val storedOrder: Order = ordersTable.get(deduceKey(order)) ?: return false
        return storedOrder.state == "FINISHED" || storedOrder.state.startsWith("FAILED", true)
    }

    fun getFinishedOrder(order:Order):Order? = if(hasOrderFinished(order)) ordersTable.get(deduceKey(order)) else null

    companion object {
        fun deduceKey(order:Order):String = "${order.id}_${order.user.name}_${order.product.name}_${order.isFraud}"
    }
}