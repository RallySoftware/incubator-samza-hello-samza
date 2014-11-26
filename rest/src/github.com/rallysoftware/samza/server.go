package main

import (
  "net/http"
  "github.com/gorilla/mux"
)

const (
  ServiceField = "service"
  LogField = "log"
  HostField = "host"
  NameField = "name"
  TypeField = "type"
  TimestampField = "timestamp"

  GaugeType = "gauge"
  CounterType = "counter"
  HistogramType = "histogram"
  TimerType = "timer"
  MeterType = "meter"

  CountField = "count"

  ValueField = "value"

  MaxField = "max"
  MeanField = "mean"
  MinField = "min"
  StdDevField = "stddev"
  P50Field = "p50"
  P75Field = "p75"
  P95Field = "p95"
  P98Field = "p98"
  P99Field = "p99"
  P999Field = "p999"

  OneMinuteRateField = "one-minute-rate"
  FiveMinuteRateField = "five-minute-rate"
  FifteenMinuteRateField = "fifteen-minute-rate"
  MeanRateField = "mean-rate"
)

func Server()  {
  r := mux.NewRouter()
  r.HandleFunc("/{service}/log4j", Log4JHandler).Methods("POST")
  r.HandleFunc("/{service}/yammer", YammerHandler).Methods("POST")
  http.ListenAndServe(":8080", r)
}