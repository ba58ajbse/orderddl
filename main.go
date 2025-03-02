package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
)

const (
	TABLE_PATTERN      = `(?i)CREATE TABLE ` + "`?" + `(\w+)` + "`?"
	REFERENCES_PATTERN = `(?i)REFERENCES ` + "`?" + `(\w+)` + "`?"
)

var (
	input  = flag.String("i", "", "")
	output = flag.String("o", "output.sql", "")
)

// テーブルの依存関係を解析する関数
func parseDDL(ddlFile string) (map[string][]string, map[string]int, []string) {
	file, err := os.Open(ddlFile)
	if err != nil {
		fmt.Println("ファイルを開けませんでした:", err)
		os.Exit(1)
	}
	defer file.Close()

	// 正規表現: CREATE TABLE と FOREIGN KEY を抽出
	reCreateTable := regexp.MustCompile(TABLE_PATTERN)
	reReferences := regexp.MustCompile(REFERENCES_PATTERN)

	// データ構造
	graph := make(map[string][]string) // 外部キーの依存関係（親 → 子）
	inDegree := make(map[string]int)   // 入次数
	tableOrder := []string{}           // テーブル作成順序
	currentTable := ""

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// CREATE TABLE の検出
		if matches := reCreateTable.FindStringSubmatch(line); len(matches) > 1 {
			// fmt.Printf("matches: %v\n", matches[1])
			currentTable = matches[1]
			tableOrder = append(tableOrder, currentTable)
			if _, exists := graph[currentTable]; !exists {
				graph[currentTable] = []string{}
			}
			if _, exists := inDegree[currentTable]; !exists {
				inDegree[currentTable] = 0
			}
		}

		// FOREIGN KEY の検出
		if strings.Contains(strings.ToLower(line), "foreign key") {
			if matches := reReferences.FindStringSubmatch(line); len(matches) > 1 {
				// fmt.Printf("matches: %v\n", matches[1])
				parentTable := matches[1]
				if currentTable != "" {
					graph[parentTable] = append(graph[parentTable], currentTable)
					inDegree[currentTable]++
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("ファイル読み込みエラー:", err)
		os.Exit(1)
	}

	return graph, inDegree, tableOrder
}

// Kahn's Algorithm を使ったトポロジカルソート
func topologicalSort(graph map[string][]string, inDegree map[string]int) []string {
	var sortedTables []string
	var queue []string

	// 入次数が0のノードをキューに追加
	for table, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, table)
		}
	}

	// トポロジカルソート処理
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		sortedTables = append(sortedTables, current)

		for _, dependent := range graph[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	// 閉路チェック（DAGでない場合）
	if len(sortedTables) != len(graph) {
		fmt.Println("エラー: 外部キーの循環依存が発生しています")
		os.Exit(1)
	}

	return sortedTables
}

// DDLを正しい順序で並び替えて出力
func reorderDDL(inputDDL, outputDDL string, sortedTables []string) {
	file, err := os.Open(inputDDL)
	if err != nil {
		fmt.Println("ファイルを開けませんでした:", err)
		os.Exit(1)
	}
	defer file.Close()

	ddlContent := make(map[string]string)
	scanner := bufio.NewScanner(file)
	var currentTable string
	var currentDDL strings.Builder

	// DDLをテーブルごとに分割
	for scanner.Scan() {
		line := scanner.Text()

		if matches := regexp.MustCompile(TABLE_PATTERN).FindStringSubmatch(line); len(matches) > 1 {
			if currentTable != "" {
				ddlContent[currentTable] = currentDDL.String()
				currentDDL.Reset()
			}
			currentTable = matches[1]
		}

		if currentTable != "" {
			currentDDL.WriteString(line + "\n")
		}
	}

	// 最後のテーブルを追加
	if currentTable != "" {
		ddlContent[currentTable] = currentDDL.String()
	}

	// 新しいDDLファイルに正しい順序で書き出す
	outputFile, err := os.Create(outputDDL)
	if err != nil {
		fmt.Println("出力ファイルを作成できませんでした:", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	for _, table := range sortedTables {
		if ddl, exists := ddlContent[table]; exists {
			_, err := writer.WriteString(ddl)
			if err != nil {
				fmt.Println("書き込みに失敗しました:", err)
				os.Exit(1)
			}
		}
	}
	writer.Flush()

	fmt.Println("✅ 正しい順序でDDLを出力しました:", outputDDL)
}

func processSQL(input, output string) {
	graph, inDegree, _ := parseDDL(input)

	sortedTables := topologicalSort(graph, inDegree)

	reorderDDL(input, output, sortedTables)
}

func main() {
	flag.Parse()
	// 必須項目のチェック
	if *input == "" {
		fmt.Println("❌ エラー: `-input` オプションで入力 SQL ファイルのパスを指定してください。")
		flag.Usage()
		os.Exit(1)
	}

	processSQL(*input, *output)
}
