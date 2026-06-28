#!/usr/bin/env bash
# Cleanup — bash version of cleanup.ps1
# Останавливает стенд и удаляет volumes (полная очистка состояния)

set -euo pipefail

cyan()  { printf '\033[36m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }

cyan "Останавливаем demo stack и удаляем контейнеры + volumes"
docker-compose down -v --remove-orphans

green "Готово"
