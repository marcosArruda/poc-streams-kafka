package br.com.marcosfariaarruda.empiricus.ordersservice.api.http

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderSerde
import br.com.marcosfariaarruda.empiricus.model.ResponseOrders
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.CustomRocksDBConfig
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.services.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@RestController
class OrdersController @Autowired constructor(private val kafkaTemplate: KafkaTemplate<String, Order>,
                                              private val inventoryService: InventoryService,
                                              private val fraudService: FraudService,
                                              private val financialService: FinancialService,
                                              private val ordersValidationService: OrdersValidationService,
                                              private val globalService: GlobalService) {



    @GetMapping("/u/{userName}/order/product/{productId}")
    fun getOrdersForUserAndProduct(@PathVariable userName:String, @PathVariable productId:Int):ResponseEntity<ResponseOrders>{
        return ResponseEntity.ok(ResponseOrders(listOf()))
    }

    @GetMapping("/shit/start")
    fun startShit():ResponseEntity<ResponseOrders>{
        doShit()
        return ResponseEntity.ok(ResponseOrders(listOf()))
    }

    fun doShit(): KafkaStreams {
        val builder = StreamsBuilder()

        val ordersStream = builder.stream<String, Order>(GlobalFuckingTopology.ORDERS_TOPIC, OrdersValidationService.consumedOrdersType)

        inventoryService.init(ordersStream)
        fraudService.init(ordersStream)
        financialService.init(ordersStream)

        ordersValidationService.init(ordersStream, builder.stream<String, OrderAnalysis>(GlobalFuckingTopology.ORDER_VALIDATIONS, OrdersValidationService.consumedOrderAnalysisType)
        )

        globalService.init()

        //builder.table(ORDER_VALIDATIONS, Consumed.with(Serdes.String(), OrderAnalysisSerde()), Materialized.`as`(MATERIALIZED_ANALYSIS_NAME))
        val configProps = Properties()
        configProps[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "broker:9092"
        configProps[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java.name
        configProps[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = OrderSerde::class.java.name
        configProps[StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG] = CustomRocksDBConfig::class.java.name

        val configPropsGlobal = Properties()
        configPropsGlobal[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "broker:9092"
        configPropsGlobal[StreamsConfig.APPLICATION_ID_CONFIG] = "myfucking-stream"
        configPropsGlobal[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java.name
        configPropsGlobal[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = OrderSerde::class.java.name
        configPropsGlobal[StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG] = CustomRocksDBConfig::class.java.name
        return startShit(builder, configProps, configPropsGlobal)
    }

    private fun startShit(builder: StreamsBuilder, configProps: Properties, configPropsGlobal: Properties): KafkaStreams {
        val startLatch = CountDownLatch(1)
        val streams = KafkaStreams(builder.build(configProps), configPropsGlobal)
        streams.cleanUp()
        streams.setStateListener { newState, oldState -> if(newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) startLatch.countDown()}
        streams.start()
        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw RuntimeException("Streams never finished rebalancing on startup")
            }
        }catch (e:InterruptedException){
            (globalService.getApplicationContext() as ConfigurableApplicationContext).close()
            Thread.currentThread().interrupt()
        }
        return streams
    }
}