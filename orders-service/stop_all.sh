#!/usr/bin/env zsh
docker-compose stop && docker system prune -f && docker volume prune -f
