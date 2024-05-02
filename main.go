package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const queryCreateCashBalance = `%s %s.%s_%s (Account String, Subconto String, Bank String, BankAccount String, Currency String,RemainingAmount Float64, CurrencyAmount  Float64, Organization    String) engine=Memory`
const queryInsertCashBalance = `INSERT INTO %s_%s (Account, Subconto, Bank, BankAccount, Currency, RemainingAmount, CurrencyAmount, Organization) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

var (
	usr          = "a.malakhov@orbita.center"
	psswd        = "rfkb,thlf12"
	URLs         []string
	listServices = "./listServices.txt"
)

type CashBalance []CashBalanceStruct

type CashBalanceStruct struct {
	Account         string  `json:"Счет"`
	Subconto        string  `json:"Субконто1"`
	Bank            string  `json:"Банк"`
	BankAccount     string  `json:"БанковскийСчет"`
	Currency        string  `json:"Валюта"`
	RemainingAmount float64 `json:"СуммаОстаток"`
	CurrencyAmount  float64 `json:"ВалютнаяСуммаОстаток"`
	Organization    string  `json:"Организация"`
}

func main() {

	getServiceURLs(listServices)

	var waitGroup sync.WaitGroup
	for _, url := range URLs {

		waitGroup.Add(1)
		go func(url string) {
			defer waitGroup.Done()
			projectName := get1CProjectName(url)
			serviceName := get1CServiceName(url)

			fmt.Printf("1C Base: %s \n1C Service: %s \n", projectName, serviceName)

			connect, err := connect()
			if err != nil {
				panic((err))
			}

			ctx := context.Background()
			err = connect.Exec(ctx, fmt.Sprintf(queryCreateCashBalance, "CREATE TABLE IF NOT EXISTS", "default", projectName, serviceName))
			if err != nil {
				log.Fatal(err)
			}

			rows := get1CResponse(url)

			err = insert(connect, rows, projectName, serviceName)
			if err != nil {
				log.Fatal(err)
			}
		}(url)
	}
	waitGroup.Wait()
}

func errControl(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getServiceURLs(filename string) {

	file, err := os.Open(filename)
	errControl(err)

	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
			break
		}
		URLs = append(URLs, line)
	}
}
func get1CResponse(url string) CashBalance {

	request, err := http.NewRequest("GET", url, nil)
	errControl(err)
	request.SetBasicAuth(usr, psswd)

	response, err := http.DefaultClient.Do(request)
	errControl(err)
	targetStruct := new(CashBalance)
	defer response.Body.Close()

	json.NewDecoder(response.Body).Decode(targetStruct)

	return *targetStruct
}

func get1CProjectName(url string) string {

	matchL := "(https:\\/\\/[0-9A-Za-z\\.\\-]*\\/)"
	matchR := "(\\/([a-z]*)\\/([0-9A-Za-z\\-\\_]*)\\/([0-9A-Za-z\\-\\_]*))"
	cutLeft := regexp.MustCompile(matchL)
	cutRight := regexp.MustCompile(matchR)

	fs := strings.ReplaceAll(url, cutLeft.FindString(url), "")

	projectName := strings.ReplaceAll(fs, cutRight.FindString(fs), "")

	return projectName
}

func get1CServiceName(url string) string {

	matchL := "(https:\\/\\/[0-9A-Za-z\\.\\-]*\\/[0-9A-Za-z\\.\\-\\_]*\\/hs\\/[0-9A-Za-z\\.\\-\\_]*\\/)"
	cutLeft := regexp.MustCompile(matchL)

	serviceName := strings.ReplaceAll(url, cutLeft.FindString(url), "")

	return serviceName
}

func connect() (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"localhost:9000"},
			Auth: clickhouse.Auth{
				Database: "Ones_test",
				Username: "default",
				Password: "",
			},
			Debug:           true,
			DialTimeout:     time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func insert(conn driver.Conn, rows CashBalance, projectName, serviceName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf(queryInsertCashBalance, projectName, serviceName))

	if err != nil {
		return err
	}
	for _, info := range rows {
		err := batch.Append(
			info.Account,
			info.Subconto,
			info.Bank,
			info.BankAccount,
			info.Currency,
			info.RemainingAmount,
			info.CurrencyAmount,
			info.Organization,
		)

		if err != nil {
			log.Fatal(err)
		}
	}
	return batch.Send()
}
