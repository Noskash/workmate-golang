package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func connectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	return sarama.NewConsumer(brokers, config)
}

func main() {
	topic := "orders"
	msgcount := 0
	worker, err := connectConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	doneChan := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println("Error:", err)
			case msg := <-consumer.Messages():
				msgcount++
				fmt.Printf("Выполняется обработка долгой задачи %d: %s\n", msgcount, string(msg.Value))
				order := string(msg.Value)
				time.Sleep(time.Minute * 3)
				fmt.Printf("Обработка заказа для : %s\n", order)
			case <-sigchan:
				fmt.Println("Получен сигнал завершения, закрытие...")
				doneChan <- struct{}{}
			}
		}
	}()

	<-doneChan
	fmt.Println("Было обработано", msgcount, "сообщений")
	if err := worker.Close(); err != nil {
		panic(err)
	}

}
