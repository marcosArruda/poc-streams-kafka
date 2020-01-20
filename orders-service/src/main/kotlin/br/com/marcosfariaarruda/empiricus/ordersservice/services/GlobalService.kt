package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderSerde
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.CustomRocksDBConfig
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct

@Service
class GlobalService @Autowired constructor(private val inventoryService: InventoryService,
                                           private val fraudService: FraudService,
                                           private val financialService: FinancialService,
                                           private val ordersValidationService: OrdersValidationService,
                                           private val orderFinishedKTableService: OrderFinishedKTableService) : ApplicationContextAware  {
    private lateinit var applicationContext: ApplicationContext

    private lateinit var kafkaStreams: KafkaStreams

    @Scheduled(initialDelay = 1000 * 7, fixedDelay = Long.MAX_VALUE)
    fun init(){
        val builder = StreamsBuilder()
        val ordersStream = builder.stream<String, Order>(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType)
        val ordersCreatedStream = ordersStream.filter{_, order -> order.state == "CREATED"}
        val ordersValidatedStream = ordersStream.filter{ _, order -> order.state == "VALIDATED"}

        val ordersTable:KTable<String, Order> = ordersStream
                .groupByKey()
                .reduce({ v1, v2 -> returnLast(v1, v2)}, Materialized.`as`(GlobalFuckingTopology.MATERIALIZED_ORDERS))

        inventoryService.init(ordersCreatedStream, ordersValidatedStream)
        fraudService.init(ordersCreatedStream, ordersValidatedStream)
        financialService.init(ordersCreatedStream, ordersValidatedStream)

        ordersValidationService.init(ordersCreatedStream, builder.stream<String, OrderAnalysis>(GlobalFuckingTopology.ORDER_VALIDATIONS, OrdersValidationService.consumedOrderAnalysisType))

        //builder.table(ORDER_VALIDATIONS, Consumed.with(Serdes.String(), OrderAnalysisSerde()), Materialized.`as`(MATERIALIZED_ANALYSIS_NAME))
        val configProps = newConfigProperties("FUCKING-STREAM-MARCOS")

        kafkaStreams = startShit(builder, configProps, ordersTable)
    }

    private fun startShit(builder: StreamsBuilder, configProps: Properties, ordersTable: KTable<String,Order>): KafkaStreams {
        val topology = builder.build(configProps)
        println("-------------------------------------------------------------------------------------------------------")
        println(topology.describe().toString())
        println("-------------------------------------------------------------------------------------------------------")
        val startLatch = CountDownLatch(1)
        val streams = KafkaStreams(topology, configProps)

        streams.cleanUp()
        streams.setStateListener { newState, oldState -> if(newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) startLatch.countDown()}
        streams.start()

        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw RuntimeException("Streams never finished rebalancing on startup")
            }
        }catch (e:InterruptedException){
            (this.getApplicationContext() as ConfigurableApplicationContext).close()
            Thread.currentThread().interrupt()
        }

        println(">>>>>>>>>>>>>>>>>> queryableStoreName: ${ordersTable.queryableStoreName()}")
        val readOnlyOrdersTable: ReadOnlyKeyValueStore<String, Order> = streams.store(ordersTable.queryableStoreName(), QueryableStoreTypes.keyValueStore<String,Order>())
        orderFinishedKTableService.init(readOnlyOrdersTable)
        return streams
    }

    override fun setApplicationContext(applicationContext: ApplicationContext) {
        this.applicationContext = applicationContext
    }

    fun getApplicationContext():ApplicationContext{
        return this.applicationContext
    }

    companion object {
        private val stateOrder:Map<String, Int> = mapOf(
                Pair("CREATED", 0),
                Pair("VALIDATED", 1),
                Pair("FAILED", 2),
                Pair("FINISHED", 2)
        )
        fun returnLast(v1:Order?, v2:Order?):Order?{
            if(v1 == null) return v2
            if(v2 == null) return v1
            val v1State:Int = if(v1.state.startsWith("FAILED", true)) stateOrder.getOrDefault("FAILED", 2) else stateOrder.getOrDefault(v1.state, -1)
            val v2State:Int = if(v2.state.startsWith("FAILED", true)) stateOrder.getOrDefault("FAILED", 2) else stateOrder.getOrDefault(v2.state, -1)
            return if(v1State <= v2State) v2 else v1
        }

        fun newConfigProperties(applicationId:String):Properties{
            val configProps = Properties()
            configProps[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "broker:9092"
            configProps[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
            configProps[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java.name
            configProps[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = OrderSerde::class.java.name
            configProps[StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG] = CustomRocksDBConfig::class.java.name
            configProps[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = 3
            configProps[StreamsConfig.PRODUCER_PREFIX+ProducerConfig.BATCH_SIZE_CONFIG] = 0
            configProps[StreamsConfig.TOPOLOGY_OPTIMIZATION] = StreamsConfig.OPTIMIZE
            //configProps[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
            return configProps
        }
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