CREATE STREAM orders_stream ( \
  id BIGINT, \
  user STRUCT<\
    id BIGINT, \
    name VARCHAR, \
    age INTEGER \
  >, \
  product STRUCT<\
    id BIGINT, \
    name VARCHAR, \
    price INTEGER \
  >, \
  quantity BIGINT, \
  state VARCHAR, \
  analysis STRUCT<\
    id BIGINT, \
    analysedFrom VARCHAR, \
    orderId BIGINT, \
    pass BOOLEAN \
  >) \
 WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON');


CREATE STREAM orders_stream (
id BIGINT,
user STRUCT<
 id BIGINT,
 name VARCHAR,
 age INT>,
product STRUCT<
 id BIGINT,
 name VARCHAR,
 price INT>,
quantity BIGINT,
state VARCHAR) WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON');

SET 'auto.offset.reset' = 'earliest';

select * from orders_stream emit changes;


--data class User(val id: Long = 1, val name: String = "Marcos", val age: Int = 34, val paymentMethods: MutableList<PaymentMethod> = mutableListOf(PaymentMethod())) : Serializable
--data class PaymentMethod(val id: Long = 1, val name: String = "cartao") : Serializable
--data class Product(val id: Long = 1, val name: String = "vacas leiteiras", val price: Int = 22) : Serializable
--data class Order(val id: Long = 1, val user: User = User(), val product: Product = Product(), val quantity: Long = 1, val state: String = "CREATED", val analysis: MutableSet<OrderAnalysis> = mutableSetOf()) : Serializable
--data class Contract(val id: Long = 1, val user: User = User(), val order: Order = Order(), val products: MutableMap<Product, Long> = mutableMapOf<Product, Long>(Pair(Product(), 1))) : Serializable

--data class OrderAnalysis(val id: Long = 1, val analysedFrom: String, val orderId: Long, val pass: Boolean = false) : Serializable
