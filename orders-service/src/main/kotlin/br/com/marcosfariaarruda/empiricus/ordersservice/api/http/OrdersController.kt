package br.com.marcosfariaarruda.empiricus.ordersservice.api.http

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.OrderSerde
import br.com.marcosfariaarruda.empiricus.model.ResponseOrders
import br.com.marcosfariaarruda.empiricus.ordersservice.api.util.OrderCreatedSupplier
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.CustomRocksDBConfig
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import br.com.marcosfariaarruda.empiricus.ordersservice.services.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@RestController
class OrdersController @Autowired constructor(private val kafkaTemplate: KafkaTemplate<String, Order>,
                                              private val orderFinishedKTableService: OrderFinishedKTableService,
                                              private val fraudService: FraudService,
                                              private val inventoryService: InventoryService) {



    @GetMapping("/u/{userName}/order/product/{productId}")
    fun getOrdersForUserAndProduct(@PathVariable userName:String, @PathVariable productId:Int):ResponseEntity<ResponseOrders>{
        return ResponseEntity.ok(ResponseOrders(listOf()))
    }

    @GetMapping("/example-order")
    fun getExampleOrder():ResponseEntity<Order>{
        return ResponseEntity.ok(Order(
                id = OrderProducer.randLong(),
                product = inventoryService.getRamdomProduct(),
                user = fraudService.getRamdomUser(),
                quantity = 1,
                state = "CREATED",
                isFraud = Random.nextBoolean()
        ))
    }

    @PostMapping("/order")
    fun newOrder(@RequestBody newOrder:Order):Mono<Order>{
        return Mono.fromCallable(OrderCreatedSupplier(newOrder, kafkaTemplate, orderFinishedKTableService))
    }
}