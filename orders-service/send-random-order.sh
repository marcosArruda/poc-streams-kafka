#!/usr/bin/env zsh
order_id=$(python -c "import random; print random.randint(100000,1000000)")
echo $order_id

curl -w "\n\n%{time_connect} + %{time_starttransfer} = %{time_total}\n" -d "{\"id\":$order_id,\"user\":{\"id\":8,\"name\":\"Stag\",\"age\":30,\"paymentMethods\":[]},\"product\":{\"id\":0,\"name\":\"sapato\",\"price\":11},\"quantity\":1,\"state\":\"CREATED\",\"analysis\":[],\"isFraud\":true}" \
 -H "Content-Type: application/json" \
 -X POST http://localhost:8080/order
