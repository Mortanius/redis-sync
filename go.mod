module github.com/Mortanius/redis-sync

go 1.17

require (
	github.com/go-redis/redis/v8 v8.8.0
	github.com/go-redis/redismock/v8 v8.0.5
	github.com/stretchr/testify v1.8.0
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/otel v0.19.0 // indirect
	go.opentelemetry.io/otel/metric v0.19.0 // indirect
	go.opentelemetry.io/otel/trace v0.19.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/go-redis/redismock/v8 v8.0.5 => github.com/joesonw/redismock/v8 v8.0.7-0.20211108075720-dde91811bf58
