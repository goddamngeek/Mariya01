#!/bin/bash
set -e

ENV_FILE="mac_sync/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "Не найден $ENV_FILE — запускай скрипт из папки odysseus-queue-bot"
  exit 1
fi

echo "Текущий BOT_URL:"
grep "^BOT_URL=" "$ENV_FILE" || echo "(не найден)"
echo ""
read -p "Вставь публичный URL бота с Northflank (например https://xxxxx--8000.code.run): " NEW_URL

if [ -z "$NEW_URL" ]; then
  echo "Пустой URL, отмена."
  exit 1
fi

# Убираем слэш в конце, если есть — чтобы не было двойных // в путях
NEW_URL="${NEW_URL%/}"

# Заменяем строку BOT_URL= в .env (создаём бэкап на всякий случай)
cp "$ENV_FILE" "${ENV_FILE}.bak"
if grep -q "^BOT_URL=" "$ENV_FILE"; then
  # macOS sed требует пустой '' после -i
  sed -i '' "s|^BOT_URL=.*|BOT_URL=${NEW_URL}|" "$ENV_FILE"
else
  echo "BOT_URL=${NEW_URL}" >> "$ENV_FILE"
fi

echo ""
echo "Обновлено. Новая строка:"
grep "^BOT_URL=" "$ENV_FILE"

echo ""
echo "Проверяю соединение с ботом..."
BOT_TOKEN=$(grep "^BOT_TOKEN=" "$ENV_FILE" | cut -d '=' -f2- || true)

if [ -n "$BOT_TOKEN" ]; then
  curl -s -o /dev/null -w "HTTP статус: %{http_code}\n" "${NEW_URL}/sync/pull" -H "Authorization: Bearer ${BOT_TOKEN}"
else
  curl -s -o /dev/null -w "HTTP статус: %{http_code}\n" "${NEW_URL}/sync/pull"
fi

echo ""
echo "Если статус НЕ 000 и не connection refused — можно гонять sync.py:"
echo "  python3 mac_sync/sync.py"
