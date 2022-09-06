package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)
}

const dataDir = "./data"

type Type int32

const (
	HUOBI   Type = iota + 1 //火币
	BINANCE                 //币安
)

func main() {
	var (
		crawler = Type(HUOBI)
	)
	switch crawler {
	case HUOBI:
		var huobiURL = "https://futures.huobi.com/data/klines/spot/daily/"
		err := crawlerHuobiKline("FILUSDT", "60min", huobiURL)
		if err != nil {
			log.Error(err)
			return
		}
		//解析CSV
		ReadHuoBiCsv()
	case BINANCE:
		var binanceURL = "https://data.binance.vision/data/spot/daily/klines/"
		err := crawlerBinanceKline("FILUSDT", "1h", binanceURL)
		if err != nil {
			log.Error(err)
			return
		}
	}
}

func ReadHuoBiCsv() error {
	//https://futures.huobi.com/data/klines/spot/daily/FILUSDT/60min/FILUSDT-60min-2022-02-24.zip
	file, err := os.Create("./data/huobi_1h_FILUSDT.csv")
	if err != nil {
		log.Error(err)
		return err
	}
	//windows bom
	_, err = file.Write([]byte{0xEF, 0xBB, 0xBF})
	if err != nil {
		log.Error(err)
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	err = writer.Write([]string{
		"开盘时间戳",
		"开盘时间",
		"年",
		"月",
		"日",
		"小时",
		"开盘价",
		"收盘价",
		"最高价",
		"最低价",
		"以基础币种计量的交易量",
		"以报价币种计量的交易量",
		"交易次数",
	})
	if err != nil {
		log.Error(err)
		return err
	}
	originFile, err := os.Open("./data/huobi_FILUSDT.csv")
	if err != nil {
		log.Error(err)
		return err
	}
	scanner := bufio.NewScanner(originFile)

	scanner.Split(bufio.ScanLines)
	var ss []string
	for scanner.Scan() {
		ss = append(ss, scanner.Text())
	}
	originFile.Close()
	// 读取文件数据
	sz := len(ss)
	// 循环取数据
	for i := 1; i < sz; i++ {
		log.Infof("line %+v , val: %+v", i, ss[i])
		tmp := strings.Split(ss[i], ",")
		// 1602774000,10.0,137.0001,220.0001,10.0,1.0231803003396902E7,84333.49782092
		id := tmp[0] //时间戳
		open := tmp[1]
		clos := tmp[2]
		high := tmp[3]
		low := tmp[4]
		amount := tmp[5]
		vol := tmp[6]
		count := ""
		if len(tmp) > 7 {
			count = tmp[7]
		}
		timeTemplate := "2006-01-02 15:04:05"
		ts, err := strconv.Atoi(id)
		if err != nil {
			log.Error(err)
			return err
		}
		timeStr := time.Unix(int64(ts), 0)
		year := timeStr.Year()
		month := int64(timeStr.Month())
		day := timeStr.Day()
		hour := timeStr.Hour()
		err = writer.Write([]string{
			id,
			timeStr.Format(timeTemplate),
			strconv.Itoa(year),
			strconv.Itoa(int(month)),
			strconv.Itoa(day),
			strconv.Itoa(hour),
			open,
			clos,
			high,
			low,
			amount,
			vol,
			count,
		})
		if err != nil {
			log.Error(err)
			return err
		}
	}
	writer.Flush()
	return nil
}
func crawlerHuobiKline(pair string, samplingPeriod string, url string) error {
	//抓取时间设置
	start := "2020-01-01 00:00:00"
	end := "2022-09-06 00:00:00"

	//https://futures.huobi.com/data/klines/spot/daily/FILUSDT/60min/FILUSDT-60min-2022-02-24.zip
	file, err := os.Create("./data/huobi_FILUSDT.csv")
	if err != nil {
		log.Error(err)
		return err
	}
	//windows bom
	_, err = file.Write([]byte{0xEF, 0xBB, 0xBF})
	if err != nil {
		log.Error(err)
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	/*
			      {
		            "id": 1629769200,
		            "open": 49056.37,
		            "close": 49025.51,
		            "low": 49022.86,
		            "high": 49056.38,
		            "amount": 3.946281917950917,
		            "vol": 193489.67275732,
		            "count": 196
		        },
				id	long	调整为新加坡时间的时间戳，单位秒，并以此作为此K线柱的id
				amount	float	以基础币种计量的交易量
				count	integer	交易次数
				open	float	本阶段开盘价
				close	float	本阶段收盘价
				low	float	本阶段最低价
				high	float	本阶段最高价
				vol	float	以报价币种计量的交易量
	*/
	writer.Write([]string{
		"开盘时间",
		"开盘价",
		"收盘价",
		"最高价",
		"最低价",
		"以基础币种计量的交易量",
		"以报价币种计量的交易量",
		"交易次数",
	})
	writer.Flush()
	dateNow := time.Now()
	startUnix, _ := time.ParseInLocation("2006-01-02 15:04:05", start, dateNow.Location())
	endUnix, _ := time.ParseInLocation("2006-01-02 15:04:05", end, dateNow.Location())

	for endUnix.Sub(startUnix).Hours() > 0 {
		url := url + pair + "/" + samplingPeriod + "/" + pair + "-" + samplingPeriod + "-" + startUnix.Format("2006-01-02") + ".zip"
		outFile := dataDir + "/" + pair + "-" + samplingPeriod + "-" + startUnix.Format("2006-01-02") + ".zip"
		startUnix = startUnix.AddDate(0, 0, 1)
		log.Infof("抓取火币K线数据: %s", url)
		// Download file
		err := DownloadFile(outFile, url)
		if err != nil {
			log.Error(err)
			continue
		}
		//log.Println("Downloaded: " + url)

		// Unzip file
		files, err := Unzip(outFile, dataDir)
		if err != nil {
			log.Errorf("%+v : %+v ", outFile, err.Error())
			continue
		}
		log.Infof("Unzipped 火币K线数据: %+v", files)
		for f := range files {
			csf, err := os.Open(files[f])
			defer csf.Close()
			if err != nil {
				log.Error(err)
				continue
			}
			lines, err := csv.NewReader(csf).ReadAll()
			if err != nil {
				log.Error(err)
				continue
			}
			writer.WriteAll(lines)
			writer.Flush()
		}
	}

	return nil
}

func crawlerBinanceKline(pair string, samplingPeriod string, url string) error {
	//抓取时间设置
	start := "2020-01-01 00:00:00"
	end := "2022-09-05 00:00:00"

	file, err := os.Create("./data/binance_FILUSDT.csv")
	if err != nil {
		log.Error(err)
		return err
	}
	//windows bom
	_, err = file.Write([]byte{0xEF, 0xBB, 0xBF})
	if err != nil {
		log.Error(err)
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	/*
			[
		  [
		    1499040000000,      // k线开盘时间
		    "0.01634790",       // 开盘价
		    "0.80000000",       // 最高价
		    "0.01575800",       // 最低价
		    "0.01577100",       // 收盘价(当前K线未结束的即为最新价)
		    "148976.11427815",  // 成交量
		    1499644799999,      // k线收盘时间
		    "2434.19055334",    // 成交额
		    308,                // 成交笔数
		    "1756.87402397",    // 主动买入成交量
		    "28.46694368",      // 主动买入成交额
		    "17928899.62484339" // 请忽略该参数
		  ]
		]
	*/
	writer.Write([]string{
		"k线开盘时间",
		"开盘价",
		"最高价",
		"最低价",
		"收盘价",
		"成交量",
		"k线收盘时间",
		"成交额",
		"成交笔数",
		"主动买入成交量",
		"主动买入成交额",
		"请忽略该参数",
	})
	writer.Flush()

	dateNow := time.Now()
	startUnix, _ := time.ParseInLocation("2006-01-02 15:04:05", start, dateNow.Location())
	endUnix, _ := time.ParseInLocation("2006-01-02 15:04:05", end, dateNow.Location())

	for endUnix.Sub(startUnix).Hours() > 0 {
		url := url + pair + "/" + samplingPeriod + "/" + pair + "-" + samplingPeriod + "-" + startUnix.Format("2006-01-02") + ".zip"
		outFile := dataDir + "/" + pair + "-" + samplingPeriod + "-" + startUnix.Format("2006-01-02") + ".zip"
		startUnix = startUnix.AddDate(0, 0, 1)
		log.Infof("抓取币安K线数据: %s", url)
		// Download file
		err := DownloadFile(outFile, url)
		if err != nil {
			log.Error(err)
			continue
		}
		// Unzip file
		files, err := Unzip(outFile, dataDir)
		if err != nil {
			log.Error(err)
			continue
		}
		log.Infof("Unzipped币安K线数据: %+v", files)
		for f := range files {
			csf, err := os.Open(files[f])
			defer csf.Close()
			if err != nil {
				log.Error(err)
				continue
			}
			lines, err := csv.NewReader(csf).ReadAll()
			if err != nil {
				log.Error(err)
				continue
			}
			writer.WriteAll(lines)
			writer.Flush()
		}
	}

	return nil
}

// DownloadFile will download a url to a local file. It's efficient because it will
// write as it downloads and not load the whole file into memory.
func DownloadFile(filepath string, url string) error {

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		log.Error(err)
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		log.Error(err)
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		log.Error(err)
	}
	return err
}

// Unzip will decompress a zip archive, moving all files and folders
// within the zip file (parameter 1) to an output directory (parameter 2).
func Unzip(src string, dest string) ([]string, error) {

	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, err
	}
	defer r.Close()

	for _, f := range r.File {

		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)

		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return filenames, fmt.Errorf("%s: illegal file path", fpath)
		}

		filenames = append(filenames, fpath)

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			log.Error(err)
			return filenames, err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			log.Error(err)
			return filenames, err
		}

		rc, err := f.Open()
		if err != nil {
			log.Error(err)
			return filenames, err
		}

		_, err = io.Copy(outFile, rc)

		// Close the file without defer to close before next iteration of loop
		outFile.Close()
		rc.Close()

		if err != nil {
			log.Error(err)
			return filenames, err
		}
	}
	return filenames, nil
}
