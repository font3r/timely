package main

import (
	"bytes"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/api/v1/jobs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(bytes.NewBufferString("Hello world!").Bytes())
	})

	log.Printf("Listening on port %v", 5000)
	log.Fatal(http.ListenAndServe(":5000", nil))
}
