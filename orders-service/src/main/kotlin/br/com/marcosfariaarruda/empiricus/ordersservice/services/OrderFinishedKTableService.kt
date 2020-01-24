package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderSerde
import br.com.marcosfariaarruda.empiricus.model.WalletsBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.Stores
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Flow
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

@Service
class OrderFinishedKTableService {

    private lateinit var ordersTable: ReadOnlyKeyValueStore<String, Order>
    private lateinit var ordersTableKeyLong: ReadOnlyKeyValueStore<Long, Order>

    fun init():KafkaStreams {
        val builder = StreamsBuilder()
        val mat1 = Materialized.`as`<String, Order>(Stores.inMemoryKeyValueStore(GlobalFuckingTopology.MATERIALIZED_ORDERS)).withKeySerde(Serdes.StringSerde())
        val mat2 = Materialized.`as`<Long, Order>(Stores.inMemoryKeyValueStore(GlobalFuckingTopology.MATERIALIZED_ORDERS_KEYLONG)).withKeySerde(Serdes.LongSerde())

        //builder.table(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType, mat1)
        val stream = builder.stream(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType)
        stream
                .groupByKey()
                .reduce({_, v2 -> v2}, mat1)
        stream
                .selectKey { _, value ->  value.id}
                .through(GlobalFuckingTopology.ORDERS_LONG_TOPIC, Produced.with(Serdes.LongSerde(), OrderSerde()))
                .groupByKey()
                .reduce({_, v2 -> v2}, mat2)

        /*
        val ordersStream = builder
                .stream(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType)
        ordersStream
                .groupByKey()
                .reduce({_, v2 -> v2}, mat1)
        ordersStream
                .map { _, value ->  KeyValue(value.id, value)}

                .groupByKey()
                .reduce({_, v2 -> v2}, mat2)
        */

                //.groupBy{ _, value -> KeyValue(value.id, value) }
                //.reduce({_, v2 -> v2}, {_, v2 -> v2})
                //.queryableStoreName()
                //.table(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType, mat1)
        //builder.table(GlobalFuckingTopology.)

        val props = GlobalService.noDefaultsConfigProperties("ORDERSTABLE")
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
        this.ordersTableKeyLong = kafkaStreams.store(GlobalFuckingTopology.MATERIALIZED_ORDERS_KEYLONG, QueryableStoreTypes.keyValueStore())
        return kafkaStreams
    }

    fun hasOrderFinished(order:Order):Boolean{
        val storedOrder: Order = ordersTable[deduceKey(order)] ?: return false
        return storedOrder.state == "FINISHED" || storedOrder.state.startsWith("FAILED", true)
    }

    fun hasOrderFinished(orderId:Long):Boolean{
        val storedOrder: Order = ordersTableKeyLong[orderId] ?: return false
        return storedOrder.state == "FINISHED" || storedOrder.state.startsWith("FAILED", true)
    }

    fun getFinishedOrder(order:Order):Order? = if(hasOrderFinished(order)) ordersTable[deduceKey(order)] else null
    fun getFinishedOrder(orderId:Long):Order? = if(hasOrderFinished(orderId)) ordersTableKeyLong[orderId] else null

    fun get(id:Long): Order? = ordersTableKeyLong[id]

    fun getAll(): Mono<MutableSet<Order>> = Flux.just(ordersTable).map {it.all()}.reduceWith({ mutableSetOf<Order>()}, { set, iter ->
            iter.forEachRemaining{ keyValue -> set.add(keyValue.value)}
            return@reduceWith set
        } )

    companion object {
        fun deduceKey(order:Order):String = "${order.id}_${order.user.name}_${order.product.name}_${order.isFraud}"
    }
}