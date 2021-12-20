package main

import (
	"log"
	"math"
	"strconv"
	"sync"

	"github.com/gofiber/fiber/v2"
)

var nodes []*node

func main() {

	app := fiber.New()

	//Config endpoint
	app.Post("/config/:n", func(c *fiber.Ctx) error {
		if n := c.Params("n"); n != "" {
			numberOfNodes, err := strconv.Atoi(n)
			if err != nil {
				return c.SendStatus(400)
			}

			nodes = make([]*node, 0)
			for i := 0; i < numberOfNodes; i++ {
				nodes = append(nodes, &node{Id: i})
			}

			log.Printf("[INFO] created %d nodes", numberOfNodes)
			return c.SendStatus(200)
		}
		return c.SendStatus(400)
	})

	//Process endpoint
	app.Post("/process", func(c *fiber.Ctx) error {

		rqt := new(request)
		if err := c.BodyParser(&rqt); err != nil {
			c.SendStatus(400)
			return c.JSON(newErrorResponse("Campo inválido "))
		}

		if len(nodes) < 1 {
			c.Status(400)
			return c.JSON(newErrorResponse("Não foi configurado o número de nós"))
		}

		var wg sync.WaitGroup
		sumChan := make(chan int)
		doneChan := make(chan bool)
		var sumResult int

		interval := float64(len(rqt.List)) / float64(len(nodes))
		if interval > math.Trunc(interval) {
			interval = math.Trunc(interval) + 1
		}
		if interval < 1 {
			interval = float64(len(rqt.List))
		}

		chunks := chunkSlice(rqt.List, int(interval))

		wg.Add(len(chunks))

		//Percorre os nós para processar
		//Caso o numero de divisões for menor que o numero de nós, os nós não seram utilizados.
		for k, node := range nodes {
			if k >= len(chunks) {
				continue
			}
			go node.sum(&wg, sumChan, chunks[k]...)

		}

		go func() {
			wg.Wait()
			doneChan <- true
		}()

		for {
			select {
			case <-doneChan:
				log.Printf("[INFO] done")
				doneChan = nil
			case sum := <-sumChan:
				log.Printf("[INFO] sum = %d", sum)
				sumResult += sum
			}
			if doneChan == nil {
				break
			}
		}

		return c.JSON(&response{
			Result: sumResult,
			Err:    nil,
		})
	})

	app.Listen(":3000")
}

type request struct {
	List []int
}

type response struct {
	Result int   `json:"result"`
	Err    error `json:"error"`
}

type errorResponse struct {
	Error Message `json:"error"`
}

type Message struct {
	Message string `json:"message"`
}
type node struct {
	Id int
}

func (*node) sum(wg *sync.WaitGroup, sumChan chan int, list ...int) {
	defer wg.Done()
	var result int
	for _, v := range list {
		result += v
	}

	sumChan <- result
}

func newErrorResponse(msg string) *errorResponse {
	return &errorResponse{
		Error: Message{
			Message: msg,
		},
	}
}

func chunkSlice(slice []int, chunkSize int) [][]int {
	var chunks [][]int
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}

		chunks = append(chunks, slice[i:end])
	}
	return chunks
}
