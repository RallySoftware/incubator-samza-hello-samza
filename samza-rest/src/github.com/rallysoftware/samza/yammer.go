package main

import (
  "encoding/json"
)

type YammerKey struct {
  Name string;
  Type string;
  Timestamp string;
}

func (k YammerKey) Encode() (bytes []byte, err error) {
  key := map[string]interface{}{
    NameField: k.Name,
    TypeField: k.Type,
    TimestampField: k.Timestamp,
  }
  bytes, err = json.Marshal(key)
  return
}

func (k YammerKey) Length() (length int) {
  bytes, err := k.Encode()
  if err != nil {
    length = len(bytes)
  }
  return
}

type YammerGauge struct {
  Value interface{}
}

func (g YammerGauge) Encode() (bytes []byte, err error) {
  key := map[string]interface{}{
    ValueField: g.Value,
  }
  bytes, err = json.Marshal(key)
  return
}

func (g YammerGauge) Length() (length int) {
  bytes, err := g.Encode()
  if err != nil {
    length = len(bytes)
  }
  return
}

type YammerCounter struct {
  Count int
}

func (c YammerCounter) Encode() (bytes []byte, err error) {
  value := map[string]interface{}{
    CountField: c.Count,
  }
  bytes, err = json.Marshal(value)
  return
}

func (c YammerCounter) Length() (length int) {
  bytes, err := c.Encode()
  if err != nil {
    length = len(bytes)
  }
  return
}

type YammerHistogram struct {
  Count int
  Max int
  Mean int
  Min int
  StdDev float64
  P50 float64
  P75 float64
  P95 float64
  P98 float64
  P99 float64
  P999 float64
}

func (h YammerHistogram) Encode() (bytes []byte, err error) {
  value := map[string]interface{}{
    CountField: h.Count,
    MaxField: h.Max,
    MeanField: h.Mean,
    MinField: h.Min,
    StdDevField: h.StdDev,
    P50Field: h.P50,
    P75Field: h.P75,
    P95Field: h.P95,
    P98Field: h.P98,
    P99Field: h.P99,
    P999Field: h.P999,
  }
  bytes, err = json.Marshal(value)
  return
}

func (h YammerHistogram) Length() (length int) {
  bytes, err := h.Encode()
  if err != nil {
    length = len(bytes)
  }
  return
}

type YammerTimer struct {
  Count int
  Max int
  Mean int
  Min int
  StdDev float64
  P50 float64
  P75 float64
  P95 float64
  P98 float64
  P99 float64
  P999 float64
}

func (t YammerTimer) Encode() (bytes []byte, err error) {
  value := map[string]interface{}{
    CountField: t.Count,
    MaxField: t.Max,
    MeanField: t.Mean,
    MinField: t.Min,
    StdDevField: t.StdDev,
    P50Field: t.P50,
    P75Field: t.P75,
    P95Field: t.P95,
    P98Field: t.P98,
    P99Field: t.P99,
    P999Field: t.P999,
  }
  bytes, err = json.Marshal(value)
  return
}

func (t YammerTimer) Length() (length int) {
  bytes, err := t.Encode()
  if err != nil {
    length = len(bytes)
  }
  return
}

type YammerMeter struct {
  Count int
  OneMinuteRate float64
  FiveMinuteRate float64
  FifteenMinuteRate float64
  MeanRate float64
}

func (m YammerMeter) Encode() (bytes []byte, err error) {
  value := map[string]float64{
    CountField: float64(m.Count),
    OneMinuteRateField: m.OneMinuteRate,
    FiveMinuteRateField: m.FiveMinuteRate,
    FifteenMinuteRateField: m.FifteenMinuteRate,
    MeanRateField: m.MeanRate,
  }
  bytes, err = json.Marshal(value)
  return
}

func (m YammerMeter) Length() (length int) {
  bytes, err := m.Encode()
  if err != nil {
    length = len(bytes)
  }
  return
}
