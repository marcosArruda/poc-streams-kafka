package br.com.marcosfariaarruda.empiricus.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.io.Serializable


data class ResponseOrders(val orders: List<Order>) : Serializable

/*
order state types:
CREATED
VALIDATED
FAIL_VALIDATION_FRAUD
FAIL_VALIDATION_INVENTORY
FAIL_VALIDATION_MONEY
 */

data class User(val id: Long = 1, val name: String = "Marcos", val age: Int = 34, val paymentMethods: MutableList<PaymentMethod> = mutableListOf(PaymentMethod())) : Serializable
data class PaymentMethod(val id: Long = 1, val name: String = "cartao") : Serializable
data class Product(val id: Long = 1, val name: String = "vacas leiteiras", val price: Int = 22) : Serializable
data class Order(val id: Long = 1, val user: User = User(), val product: Product = Product(), val quantity: Long = 1, val state: String = "CREATED", val analysis: MutableSet<OrderAnalysis> = mutableSetOf(), val isFraud:Boolean=false) : Serializable
data class OrderAnalysis(val id: Long = 1, val analysedFrom: String, val orderId: Long, val pass: Boolean = false) : Serializable
data class Contract(val id: Long = 1, val user: User = User(), val order: Order = Order(), val products: MutableMap<Product, Long> = mutableMapOf<Product, Long>(Pair(Product(), 1))) : Serializable

class UserSerde : Serde<User> {
    constructor()
    override fun deserializer(): Deserializer<User> = UserDeserializer()
    override fun serializer(): Serializer<User> = UserSerializer()
}

class UserDeserializer : Deserializer<User> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun deserialize(topic: String?, data: ByteArray?): User = mapper.readValue(data, User::class.java)
}

class UserSerializer : Serializer<User> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun serialize(topic: String?, data: User?): ByteArray = mapper.writeValueAsBytes(data)
}

class PaymentMethodSerde : Serde<PaymentMethod> {
    constructor()
    override fun deserializer(): Deserializer<PaymentMethod> = PaymentMethodDeserializer()
    override fun serializer(): Serializer<PaymentMethod> = PaymentMethodSerializer()
}

class PaymentMethodDeserializer : Deserializer<PaymentMethod> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun deserialize(topic: String?, data: ByteArray?): PaymentMethod = mapper.readValue(data, PaymentMethod::class.java)
}

class PaymentMethodSerializer : Serializer<PaymentMethod> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun serialize(topic: String?, data: PaymentMethod?): ByteArray = mapper.writeValueAsBytes(data)
}


class ProductSerde : Serde<Product> {
    constructor()
    override fun deserializer(): Deserializer<Product> = ProductDeserializer()
    override fun serializer(): Serializer<Product> = ProductSerializer()
}

class ProductDeserializer : Deserializer<Product> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun deserialize(topic: String?, data: ByteArray?): Product = mapper.readValue(data, Product::class.java)
}

class ProductSerializer : Serializer<Product> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun serialize(topic: String?, data: Product?): ByteArray = mapper.writeValueAsBytes(data)
}

class OrderSerde : Serde<Order> {
    constructor()
    override fun deserializer(): Deserializer<Order> = OrderDeserializer()
    override fun serializer(): Serializer<Order> = OrderSerializer()
}

class OrderDeserializer : Deserializer<Order> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun deserialize(topic: String?, data: ByteArray?): Order = mapper.readValue(data, Order::class.java)
}

class OrderSerializer : Serializer<Order> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun serialize(topic: String?, data: Order?): ByteArray = mapper.writeValueAsBytes(data)
}

class ContractSerde : Serde<Contract> {
    constructor()
    override fun deserializer(): Deserializer<Contract> = ContractDeserializer()
    override fun serializer(): Serializer<Contract> = ContractSerializer()
}

class ContractDeserializer : Deserializer<Contract> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun deserialize(topic: String?, data: ByteArray?): Contract = mapper.readValue(data, Contract::class.java)
}

class ContractSerializer : Serializer<Contract> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun serialize(topic: String?, data: Contract?): ByteArray = mapper.writeValueAsBytes(data)
}

class OrderAnalysisSerde : Serde<OrderAnalysis> {
    constructor()
    override fun deserializer(): Deserializer<OrderAnalysis> = OrderAnalysisDeserializer()
    override fun serializer(): Serializer<OrderAnalysis> = OrderAnalysisSerializer()
}

class OrderAnalysisDeserializer : Deserializer<OrderAnalysis> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun deserialize(topic: String?, data: ByteArray?): OrderAnalysis = mapper.readValue(data, OrderAnalysis::class.java)
}

class OrderAnalysisSerializer : Serializer<OrderAnalysis> {
    constructor()
    private val mapper: ObjectMapper = jacksonObjectMapper()
    override fun serialize(topic: String?, data: OrderAnalysis?): ByteArray = mapper.writeValueAsBytes(data)
}
