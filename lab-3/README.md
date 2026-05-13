```
cp .env.example .env
docker compose up --build
```

Ждём, пока Flink сделает несколько чекпоинтов...

Нормализованные данные должны быть доступны в pgAdmin:
- URL: http://localhost:5050
- Username: `flink@example.com`
- Password: `flink`