package br.com.marcosfariaarruda.empiricus.ordersservice.producers

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.services.FraudService
import br.com.marcosfariaarruda.empiricus.ordersservice.services.InventoryService
import br.com.marcosfariaarruda.empiricus.ordersservice.services.OrderFinishedKTableService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import kotlin.random.Random
import kotlin.random.Random.Default.nextBoolean


@Service
class OrderProducer @Autowired constructor(private val kafkaTemplate: KafkaTemplate<String, Order>,
                                           private val inventoryService: InventoryService,
                                           private val fraudService: FraudService,
                                           private val kafkaAdmin: KafkaAdmin) {

    @Scheduled(initialDelay = 1000 * 3, fixedDelay = Long.MAX_VALUE)
    fun createFuckingTopics(){
        kafkaAdmin.initialize()
    }

    //@Scheduled(initialDelay = 1000 * 15, fixedRate = 3000)
    fun sendMessage() {
        val user = fraudService.getRamdomUser()

        val order = Order(
                id = randLong(),
                product = inventoryService.getRamdomProduct(),
                user = user,
                quantity = 1,
                state = "CREATED",
                isFraud = nextBoolean()
        )
        val key = OrderFinishedKTableService.deduceKey(order)
        println("[CREATING A NEW ORDER] key: $key, value: $order")
        kafkaTemplate.send(GlobalFuckingTopology.ORDERS_TOPIC, key, order)
        /*
        val future: ListenableFuture<SendResult<String, String>> = kafkaTemplate.send("orders", msg)

        future.addCallback(object : ListenableFutureCallback<SendResult<String?, String?>?> {
            override fun onSuccess(result: SendResult<String?, String?>?) {
                System.out.println("Sent message=[" + msg.toString() +
                        "] with offset=[" + result.getRecordMetadata().offset().toString() + "]")
            }

            override fun onFailure(ex: Throwable) {
                System.out.println(("Unable to send message=["
                        + msg) + "] due to : " + ex.message)
            }
        })
        */
    }

    companion object {
        fun randInt(): Int = Random.nextInt(1, 100000)
        fun randLong(): Long = Random.nextLong(1, 10000000)
        fun randString(): String {
            val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
            return (1..10).map { Random.nextInt(0, charPool.size) }.map(charPool::get).joinToString("")
        }
    }
}