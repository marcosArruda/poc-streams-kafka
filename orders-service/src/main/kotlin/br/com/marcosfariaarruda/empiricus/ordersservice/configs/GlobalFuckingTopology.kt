package br.com.marcosfariaarruda.empiricus.ordersservice.configs

import br.com.marcosfariaarruda.empiricus.model.Order
import br.com.marcosfariaarruda.empiricus.model.OrderSerde
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory


@Configuration
class GlobalFuckingTopology {

    companion object {
        const val ORDERS_TOPIC: String = "orders"
        const val ORDER_VALIDATIONS: String = "order-validations"
        const val MATERIALIZED_ORDERS:String = "materialized-orders"
    }

    private val bootstrapAddress: String = "broker:9092"


    @Bean
    fun orders(): NewTopic {
        return NewTopic(ORDERS_TOPIC, 3, 1.toShort())
    }



    @Bean
    fun orderValidations(): NewTopic {
        return NewTopic(ORDER_VALIDATIONS, 3, 1.toShort())
    }


    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val configs: MutableMap<String, Any> = HashMap()
        configs[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapAddress
        val theAdmin = KafkaAdmin(configs)
        theAdmin.setAutoCreate(true)
        return theAdmin
    }


    @Bean
    fun producerFactory(): ProducerFactory<String, Order> {
        val configProps: MutableMap<String, Any> = HashMap()
        configProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapAddress
        configProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = OrderSerde().serializer()::class.java

        return DefaultKafkaProducerFactory(configProps)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, Order> {
        return KafkaTemplate(producerFactory())
    }

    /*
    @Bean
    fun processOrders(): Consumer<KStream<String, String>> = Consumer { input: KStream<String, String> ->
        println("veio coisa! $input")
        input.foreach { key: String, value: String ->
            try {
                println("Key: $key Value: $value")
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }
    */

}