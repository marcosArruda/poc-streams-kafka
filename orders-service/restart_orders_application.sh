#!/usr/bin/env zsh
cd ../modelo-de-dados
./gradlew clean build publishDefaultPublicationToMavenLocal
cd ../orders-service
./gradlew clean build -x test && docker-compose stop orders-service && docker system prune -f && docker volume prune -f && docker-compose up -d --build
