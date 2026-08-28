package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config map[string]string

var config Config

func read_config(file_path string) error {
	file, err := open_file(file_path)
	if err != nil {
		fmt.Println("Could not read config")
		return err
	}
	defer file.Close()
	config = make(Config)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		config_pair := strings.Split(scanner.Text(), "=")
		config[config_pair[0]] = config_pair[1]
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading lines ", err)
		return err
	}
	return nil
}

func read_amount_clients() (int, error) {
	amount_clients, err := strconv.Atoi(config["default_amount_clients"])
	if err != nil {
		fmt.Println("Error converting string:", err)
		return 0, err
	}
	return amount_clients, nil
}

func reset_yaml() error {
	file_path := config["yaml_path"]
	file, err := open_file(file_path)
	if err != nil {
		fmt.Println("Could not reset yaml")
		return err
	}
	base_text, err := detect_base_text_yaml(file)
	if err != nil {
		fmt.Println("Could not reset yaml")
		return err
	}
	return overwrite_yaml(file_path, base_text)
}

func open_file(file_path string) (*os.File, error) {
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Println("Could not read file ", file_path)
		return nil, err
	}
	return file, nil
}

func detect_base_text_yaml(file *os.File) (string, error) {
	defer file.Close()
	var base_text strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "client") {
			break
		}
		base_text.WriteString(scanner.Text())
		base_text.WriteString("\n")
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading lines ", err)
		return "", err
	}
	result := strings.TrimRight(base_text.String(), "\n") + "\n"
	return result, nil
}

func overwrite_yaml(file_path string, base_text string) error {
	err := os.WriteFile(file_path, []byte(base_text), 0644)
	if err != nil {
		fmt.Println("Could not write file", file_path)
		return err
	}
	return nil
}

func add_clients(amount_clients int) {
	for index_client := 0; index_client < amount_clients; index_client++ {
		client_config := fmt.Sprintf(`
  client_%d:
    build:
      context: ./services/client
      dockerfile: Dockerfile
    container_name: client_%d
    depends_on:
      - server
    environment:
      - AGENCY_ID=%d
      - SERVER_HOST=server
      - SERVER_PORT=%s \n`, index_client, index_client, index_client, config["server_port"])
		save_client(client_config)
	}
}

func save_client(client_config string) {
	file, err := os.OpenFile(config["yaml_path"], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	_, _ = file.WriteString(client_config)
}

func main() {
	path_config := flag.String("config", "./create_clients.config", "a string")
	flag.Parse()
	read_config(*path_config)
	_ = reset_yaml()
	amount_clients, err := read_amount_clients()
	if err != nil {
		return
	}
	fmt.Println("amount clients", amount_clients)
	add_clients(amount_clients)

}
