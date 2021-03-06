package br.com.marcosfariaarruda.empiricus.ordersservice.api.http

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.ResponseOrders
import br.com.marcosfariaarruda.empiricus.ordersservice.api.util.OrderCreatedSupplier
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import br.com.marcosfariaarruda.empiricus.ordersservice.services.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
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
                product = inventoryService.getRandomProduct(),
                user = fraudService.getRandomUser(),
                quantity = 1,
                state = "CREATED",
                isFraud = Random.nextBoolean()
        ))
    }

    @PostMapping("/order")
    fun newOrder(@RequestBody newOrder:Order):Mono<Order>{
        return Mono.fromCallable(OrderCreatedSupplier(newOrder, kafkaTemplate, orderFinishedKTableService))
    }

    @GetMapping("/order/{id}")
    fun oneOrder(@PathVariable id:Long):Mono<Order?> = Mono.just(id).map { x -> orderFinishedKTableService.get(x) }

    @GetMapping("/order/multi/{multipleOrders}")
    fun multipleOrders(@PathVariable multipleOrders:String): Flux<Order?> = Flux.fromStream(multipleOrders.split("-").stream()).map{ id -> orderFinishedKTableService.get(id.toLong())}

    @GetMapping("/order")
    fun allOrders(): Mono<MutableSet<Order>> = orderFinishedKTableService.getAll()
}