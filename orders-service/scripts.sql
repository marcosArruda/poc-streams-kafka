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

CREATE STREAM order_validations_stream (
id BIGINT,
analysedfrom VARCHAR,
orderid BIGINT,
pass BOOLEAN) WITH (KAFKA_TOPIC='order-validations', VALUE_FORMAT='JSON');

select * from order_validations_stream emit changes;