package main

import (
	"flag"
	"fmt"
	"go.guoyk.net/common"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	indexDateLayout = "2006-01-02"
)

var (
	optDry bool

	optConf     string
	optTemplate bool
	optDelete   bool
	optWarm     bool
	optMove     bool
	optCold     bool
	optReopen   bool

	conf Conf
)

func exit(err *error) {
	if *err != nil {
		log.Printf("esmaint: exited with error: %s", (*err).Error())
		if optDry {
			sendAlert(conf.AlertDispatcher, fmt.Sprintf("esmain 错误\n(dry run)\n%s", (*err).Error()))
		} else {
			sendAlert(conf.AlertDispatcher, fmt.Sprintf("esmain 错误\n%s", (*err).Error()))
		}
		os.Exit(1)
	}
}

func main() {
	var err error
	defer exit(&err)

	flag.BoolVar(&optDry, "dry", false, "干跑，不进行实际的调用")
	flag.StringVar(&optConf, "conf", "/etc/esmaint.yml", "配置文件")
	flag.BoolVar(&optTemplate, "template", false, "执行 索引模板载入 步骤")
	flag.BoolVar(&optWarm, "warm", false, "执行 温区 步骤")
	flag.BoolVar(&optMove, "move", false, "执行 移动 步骤")
	flag.BoolVar(&optCold, "cold", false, "执行 冷区 步骤")
	flag.BoolVar(&optDelete, "delete", false, "执行 删除 步骤")
	flag.BoolVar(&optReopen, "reopen", false, "执行重新打开索引步骤，并等待调度恢复")
	flag.Parse()

	// load conf
	if err = common.LoadYAMLConfigFile(optConf, &conf); err != nil {
		return
	}
	if err = conf.Validate(); err != nil {
		return
	}

	log.Printf("esmaint: conf: %+v", conf)

	// es client
	var es *ES
	if es, err = NewES(conf.URL); err != nil {
		return
	}

	// template stage
	if optTemplate {
		var fis []os.FileInfo
		if fis, err = ioutil.ReadDir(conf.TemplateDir); err != nil {
			return
		}
		for _, f := range fis {
			fName := f.Name()
			if strings.ToLower(filepath.Ext(fName)) != ".json" {
				continue
			}
			name := fName[0 : len(fName)-5]
			log.Printf("esmaint: found index template: %s", name)
			var buf []byte
			if buf, err = ioutil.ReadFile(filepath.Join(conf.TemplateDir, fName)); err != nil {
				return
			}
			if !optDry {
				if err = es.PutIndexTemplate(name, buf); err != nil {
					return
				}
			}
		}
	}

	if !(optWarm || optMove || optCold || optDelete || optReopen) {
		return
	}

	// list indices
	var indices []ESIndex
	if indices, err = es.GetIndices(); err != nil {
		return
	}

	// determine now
	now := time.Now()
	_ = now

	// counters
	var countWarm, countMove, countCold, countDelete int

	// iterate indices
	for _, index := range indices {
		// skip system indices and blacklist
		if conf.ShouldSkip(index.Index) {
			continue
		}

		var ok bool

		// extract date
		var date time.Time
		if date, ok = dateFromIndex(index.Index); !ok {
			log.Printf("esmaint: missing date suffix: %s", index.Index)
			continue
		}
		// find rule
		var rule Rule
		if rule, ok = conf.FindRule(index.Index); !ok {
			log.Printf("esmaint: no rule matched: %s", index.Index)
			continue
		}

		// calculate day count
		day := int64(now.UTC().Sub(date) / (time.Hour * 24))
		// warm
		if optWarm {
			// open warm index if closed and not yet cold
			if day > rule.Warm && day <= rule.Cold && !index.Open {
				log.Printf("esmaint: change cold to warm: %s", index.Index)
				if err = es.OpenIndex(index.Index); err != nil {
					return
				}
				index.Open = true
			}
			if day > rule.Warm && index.Open {
				log.Printf("esmaint: check best compression: %s", index.Index)
				// check if codec is best_compression
				var isBC bool
				if isBC, err = es.IsIndexCodecBestCompression(index.Index); err != nil {
					return
				}
				// set best compression if needed
				if !isBC || !index.FullMerged {
					countWarm++
					log.Printf("esmaint: set best compression: %s", index.Index)
					if !optDry {
						if err = es.SetIndexCodecBestCompression(index.Index); err != nil {
							return
						}
					}
					log.Printf("esmaint: full merge: %s", index.Index)
					if !optDry {
						if err = es.FullMergeIndex(index.Index); err != nil {
							return
						}
					}
				}
			}
		}
		// move
		if optMove {
			if day > rule.Move {
				countMove++
				if !optDry {
					var isHDD bool
					if isHDD, err = es.IsIndexRoutingToHDD(index.Index); err != nil {
						return
					}
					if !isHDD {
						log.Printf("esmiant: move index to hdd: %s", index.Index)
						if err = es.SetIndexRoutingToHDD(index.Index); err != nil {
							return
						}
					}
				}
			}
		}

		// reopen
		if optReopen && (!index.Open) {
			log.Printf("esmaint: reopen :%s", index.Index)
			if !optDry {
				if err = es.OpenIndex(index.Index); err != nil {
					return
				}
				if err = es.WaitClusterRecovery(); err != nil {
					return
				}
				if err = es.CloseIndex(index.Index); err != nil {
					return
				}
			}
		}

		// cold
		if optCold {
			if day > rule.Cold && index.Open {
				countCold++
				log.Printf("esmaint: close: %s", index.Index)
				if !optDry {
					if err = es.CloseIndex(index.Index); err != nil {
						return
					}
				}
			}
		}
		// delete
		if optDelete {
			if day > rule.Delete {
				countDelete++
				log.Printf("esmaint: delete: %s", index.Index)
				if !optDry {
					if err = es.DeleteIndex(index.Index); err != nil {
						return
					}
				}
			}
		}
	}

	// alert
	alert := "esmaint 报告\n"
	if optDry {
		alert += "(dry run)\n"
	}
	if countWarm > 0 {
		alert += fmt.Sprintf("%d 个索引移入温区\n", countWarm)
	}
	if countMove > 0 {
		alert += fmt.Sprintf("%d 个索引移入 HDD 节点\n", countMove)
	}
	if countCold > 0 {
		alert += fmt.Sprintf("%d 个索引移入冷区\n", countCold)
	}
	if countDelete > 0 {
		alert += fmt.Sprintf("%d 个索引已删除\n", countDelete)
	}
	alert += "报告结束"

	sendAlert(conf.AlertDispatcher, alert)
}
