package br.com.marcosfariaarruda.empiricus.ordersservice.services

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderAnalysis
import br.com.marcosfariaarruda.empiricus.model.WalletsBox
import br.com.marcosfariaarruda.empiricus.ordersservice.configs.GlobalFuckingTopology
import br.com.marcosfariaarruda.empiricus.ordersservice.producers.OrderProducer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.stereotype.Service
import reactor.core.publisher.toMono

@Service
class OrderFinishedKTableService {

    private lateinit var ordersTable: ReadOnlyKeyValueStore<String, Order>

    fun init(ordersTable:ReadOnlyKeyValueStore<String, Order>) {
        this.ordersTable = ordersTable


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