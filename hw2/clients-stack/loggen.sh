#!/bin/sh
mkdir -p /var/log/app
counter=0
LEVELS="INFO INFO INFO INFO WARN ERROR"
EVENTS="order.placed payment.processed cart.updated login.success login.failed"
while true; do
  counter=$((counter + 1))
  AMOUNT=$((RANDOM % 1000))
  USER_ID=$((counter % 50))
  LEVEL=$(echo $LEVELS | tr ' ' '\n' | shuf -n 1)
  EVENT=$(echo $EVENTS | tr ' ' '\n' | shuf -n 1)
  echo "{\"timestamp\":\"$(date -Iseconds)\",\"level\":\"$LEVEL\",\"service\":\"checkout\",\"event\":\"$EVENT\",\"order_id\":$counter,\"amount\":$AMOUNT,\"user\":\"user-$USER_ID\"}" >> /var/log/app/app.log
  sleep 2
done
