# Samsa

One morning, when Gregor Samsa woke from troubled dreams, he found himself transformed in his bed into a horrible message broker.

## Features

- topic-based pub/sub
- asynchronous publishing
- realtime subscriptions
- graceful shutdown

## Run

go run ./cmd/server

Server starts on localhost:8080

## API

Publish message:

POST /publish

Example:

curl -X POST localhost:8080/publish \
-H "Content-Type: application/json" \
-d '{"topic":"logs","value":"hello"}'

Consume messages:

GET /consume?topic=logs&offset=0

Subscribe to realtime messages:

GET /subscribe?topic=logs

## Docker

Build image:

docker build -t samsa .

Run container:

docker run -p 8080:8080 samsa