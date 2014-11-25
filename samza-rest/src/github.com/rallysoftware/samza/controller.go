package main

import (
  "net/http"
  "io/ioutil"
  "github.com/Shopify/sarama"
  "github.com/gorilla/mux"
  "encoding/json"
)

var log4jProducer *sarama.SimpleProducer
var yammerProducer *sarama.SimpleProducer

func publishLog4J(producer *sarama.SimpleProducer, service string, log string, host string, timestamp string, message string) {
  err := producer.SendMessage(Log4JKey{Service: service, Log: log, Host: host, Timestamp: timestamp}, sarama.StringEncoder(message))
  if err != nil {
    panic(err)
  }
}

func publishYammerGauge(producer *sarama.SimpleProducer, service string, name string, timestamp string, counter YammerGauge) {
  err := producer.SendMessage(YammerKey{Name: name, Type: GaugeType, Timestamp: timestamp}, counter)
  if err != nil {
    panic(err)
  }
}

func publishYammerCounter(producer *sarama.SimpleProducer, service string, name string, timestamp string, counter YammerCounter) {
  err := producer.SendMessage(YammerKey{Name: name, Type: CounterType, Timestamp: timestamp}, counter)
  if err != nil {
    panic(err)
  }
}

func publishYammerHistogram(producer *sarama.SimpleProducer, service string, name string, timestamp string, histogram YammerHistogram) {
  err := producer.SendMessage(YammerKey{Name: name, Type: HistogramType, Timestamp: timestamp}, histogram)
  if err != nil {
    panic(err)
  }
}

func publishYammerTimer(producer *sarama.SimpleProducer, service string, name string, timestamp string, timer YammerTimer) {
  err := producer.SendMessage(YammerKey{Name: name, Type: TimerType, Timestamp: timestamp}, timer)
  if err != nil {
    panic(err)
  }
}

func publishYammerMeter(producer *sarama.SimpleProducer, service string, name string, timestamp string, meter YammerMeter) {
  err := producer.SendMessage(YammerKey{Name: name, Type: MeterType, Timestamp: timestamp}, meter)
  if err != nil {
    panic(err)
  }
}

func client() *sarama.Client {
  client, err := sarama.NewClient("web-front-end", []string{"hall.f4tech.com:9092"}, sarama.NewClientConfig())
  if err != nil {
      panic(err)
  }
  return client
}

func producer(client *sarama.Client, topic string) *sarama.SimpleProducer {
  producer, err := sarama.NewSimpleProducer(client, topic, nil)
  if err != nil {
      panic(err)
  }
  return producer
}

func service(req *http.Request) string {
  vars := mux.Vars(req)
  return vars["service"]
}

func log(req *http.Request) string {
  return req.Header.Get("z-log")
}

func host(req *http.Request) string {
  return req.Header.Get("z-host")
}

func timestamp(req *http.Request) string {
  return req.Header.Get("z-timestamp")
}

func name(req *http.Request) string {
  return req.Header.Get("z-name")
}

func typ(req *http.Request) string {
  return req.Header.Get("z-type")
}

func Log4JHandler(res http.ResponseWriter, req *http.Request) {
  client := client()
  defer client.Close()

  producer := producer(client, "go-log4j")
  defer producer.Close()

  body, err := ioutil.ReadAll(req.Body);
  if err != nil {
    panic(err)
  }

  service := service(req)
  log := log(req)
  host := host(req)
  timestamp := timestamp(req)

  if log == "" {
    res.WriteHeader(422)
    res.Write([]byte("z-log header not set"))
  }

  if host == "" {
    res.WriteHeader(422)
    res.Write([]byte("z-host header not set"))
  }

  if timestamp == "" {
    res.WriteHeader(422)
    res.Write([]byte("z-timestamp header not set"))
  }

  publishLog4J(producer, service, log, host, timestamp, string(body))
}

func YammerHandler(res http.ResponseWriter, req *http.Request) {
  client := client()
  defer client.Close()

  producer := producer(client, "go-yammer")
  defer producer.Close()

  body, err := ioutil.ReadAll(req.Body);
  if err != nil {
    panic(err)
  }

  service := service(req)
  name := name(req)
  t := typ(req)
  timestamp := timestamp(req)

  if name == "" {
    res.WriteHeader(422)
    res.Write([]byte("z-name header not set"))
  }

  if t == "" {
    res.WriteHeader(422)
    res.Write([]byte("z-type header not set"))
  }

  if timestamp == "" {
    res.WriteHeader(422)
    res.Write([]byte("z-timestamp header not set"))
  }

  switch(t) {
  case "counter":
    var counter YammerCounter
    json.Unmarshal(body, &counter)
    publishYammerCounter(producer, service, name, timestamp, counter)
  case "timer":
    var timer YammerTimer
    json.Unmarshal(body, &timer)
    publishYammerTimer(producer, service, name, timestamp, timer)
  case "histogram":
    var histogram YammerHistogram
    json.Unmarshal(body, &histogram)
    publishYammerHistogram(producer, service, name, timestamp, histogram)
  case "meter":
    var meter YammerMeter
    json.Unmarshal(body, &meter)
    publishYammerMeter(producer, service, name, timestamp, meter)
  }
}
