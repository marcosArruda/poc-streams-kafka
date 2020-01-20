#!/usr/bin/env zsh
cd modelo-de-dados
./gradlew clean build publishDefaultPublicationToMavenLocal
cd ..

cd orders-service
./gradlew clean build -x test && docker-compose stop && docker system prune -f && docker volume prune -f && docker-compose up -d --build zookeeper broker ksqldb-server ksqldb-cli
echo 'sleeping 20 seconds waiting for the base service to launch'
sleep 10
./gradlew clean build -x test && docker-compose stop orders-service && docker system prune -f && docker volume prune -f && docker-compose up -d --build

echo 'OK, now you can use the stuff..'
