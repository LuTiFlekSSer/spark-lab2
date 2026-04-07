<h1 align="center">
Spark Lab 2
</h1>

## Сравнение времени

![example](md/all_times.png)

- Отображаются все распознанные люди + уверенность.
- В левом верхнем углу отображается задержка времени поступления сырого кадра от распознанного
- kafka ui будет доступна по адресу `http://localhost:8080/`

## Запуск

1. Распаковать датасет в папке `data`
2. Замер с 1 нодой `docker compose -f .\docker-compose-1-node.yaml up --build`
3. Замер с 3 нодами `docker compose -f .\docker-compose-3-nodes.yaml up --build`