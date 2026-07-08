package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Job — задание на обработку URL
type Job struct {
	ID  int
	URL string
}

// Result — результат обработки задания
type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
}

// worker — функция-воркер, обрабатывает задания из канала jobs и пишет результаты в results
func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		res := processJob(job)
		results <- res
	}
}

// processJob — имитирует HTTP-запрос и замеряет время
func processJob(job Job) Result {
	start := time.Now()

	// Имитация HTTP-запроса случайной задержкой от 100 до 500 мс
	delay := time.Duration(rand.Intn(400)+100) * time.Millisecond
	time.Sleep(delay)

	return Result{
		Job:      job,
		Status:   "обработано",
		Duration: time.Since(start),
	}
}

// report — выводит агрегированный отчёт
func report(results []Result) {
	fmt.Println("--- Итоговый отчёт ---")

	totalDuration := 0 * time.Millisecond

	for _, r := range results {
		ms := r.Duration.Milliseconds()
		fmt.Printf("URL: %s | Статус: %s | Время: %d мс\n", r.Job.URL, r.Status, ms)
		totalDuration += r.Duration
	}

	avgDuration := totalDuration / time.Duration(len(results))

	fmt.Println("================================")
	fmt.Printf("Всего  URL: %d\n", len(results))
	fmt.Printf("Общее время: %v\n", totalDuration)
	fmt.Printf("Среднее время на URL: %v\n", avgDuration)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	urls := []string{
		"https://example.com",
		"https://google.com",
		"https://github.com",
		"https://stackoverflow.com",
		"https://wikipedia.org",
		"https://yandex.ru",
		"https://mail.ru",
		"https://vk.com",
		"https://ok.ru",
		"https://ria.ru",
		"https://tass.ru",
		"https://lenta.ru",
		"https://rbc.ru",
	}

	const workerCount = 3 // размер пула воркеров

	jobsCh := make(chan Job, len(urls))       // канал заданий (буферизованный)
	resultsCh := make(chan Result, len(urls)) // канал результатов (буферизованный)

	var wg sync.WaitGroup

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker(jobsCh, resultsCh, &wg)
	}

	for i, url := range urls {
		jobsCh <- Job{ID: i, URL: url}
	}
	close(jobsCh)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var results []Result
	for res := range resultsCh {
		results = append(results, res)
	}

	report(results)
}
