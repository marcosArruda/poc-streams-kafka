package br.com.marcosfariaarruda.empiricus.ordersservice.api.util

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.services.OrderFinishedKTableService
import org.springframework.kafka.core.KafkaTemplate
import java.io.Serializable
import java.util.concurrent.Callable

class OrderCreatedSupplier(private val newOrder:Order, private val kafkaTemplate: KafkaTemplate<String, Order>, private val orderFinishedKTableService:OrderFinishedKTableService) : Callable<Order>{
    override fun call(): Order? {
        kafkaTemplate.send(GlobalFuckingTopology.ORDERS_TOPIC, OrderFinishedKTableService.deduceKey(newOrder), newOrder)
        while (true){
            Thread.sleep(50)
            if(orderFinishedKTableService.hasOrderFinished(newOrder)){
                val x = orderFinishedKTableService.getFinishedOrder(newOrder)
                println(">>>> ACHOU: $x")
                return x
            }
            println(">>>>>> PROCURANDO_POR: $newOrder")
        }
    }
}


data class Contador(val qtd:Int=1, val finished:Boolean=false) :Serializable