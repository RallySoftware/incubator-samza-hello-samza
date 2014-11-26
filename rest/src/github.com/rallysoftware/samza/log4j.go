package main

import (
  "encoding/json"
)

type Log4JKey struct {
  Service string
  Log string
  Host string
  Timestamp string
}

func (k Log4JKey) Encode() (bytes []byte, err error) {
  key := map[string]interface{}{
    ServiceField: k.Service,
    LogField: k.Log,
    HostField: k.Host,
    TimestampField: k.Timestamp,
  }
  bytes, err = json.Marshal(key)
  return
}

func (k Log4JKey) Length() (length int) {
  bytes, err := k.Encode()
  if err != nil {
    length = len(bytes)
  }
  return
}
