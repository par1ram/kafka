# Хороший шаблон (producer-consumer) для работы с кафкой.

## Используется библиотека - надстройка над сишной библиотекой.

`go get github.com/confluentinc/confluent-kafka-go/v2/kafka`

### consumer lag = последнее сообщние в партиции - offset консумера.

### Количество consumers должно быть <= количество partitions
