package main

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Rule struct {
	Warm   int64 `yaml:"warm"`
	Move   int64 `yaml:"move"`
	Cold   int64 `yaml:"cold"`
	Delete int64 `yaml:"delete"`
}

func ParseRule(s string) (rule Rule, err error) {
	splits := strings.Split(s, ":")
	if len(splits) != 4 {
		err = errors.New("Rule 格式错误")
		return
	}
	if rule.Warm, err = strconv.ParseInt(strings.TrimSpace(splits[0]), 10, 64); err != nil {
		return
	}
	if rule.Move, err = strconv.ParseInt(strings.TrimSpace(splits[1]), 10, 64); err != nil {
		return
	}
	if rule.Cold, err = strconv.ParseInt(strings.TrimSpace(splits[2]), 10, 64); err != nil {
		return
	}
	if rule.Delete, err = strconv.ParseInt(strings.TrimSpace(splits[3]), 10, 64); err != nil {
		return
	}
	return
}

type Conf struct {
	URL             string            `yaml:"url" defaults:"http://127.0.0.1:9200"`
	AlertDispatcher string            `yaml:"alert_dispatcher"`
	TemplateDir     string            `yaml:"template_dir"`
	Rules           map[string]string `yaml:"rules"`
	Blacklist       []string          `yaml:"blacklist"`
}

func (c Conf) Validate() error {
	if len(c.TemplateDir) == 0 {
		return errors.New("no template_dir provided")
	}
	if len(c.URL) == 0 {
		return errors.New("no url provided")
	}
	if len(c.Rules) == 0 {
		return errors.New("no rules provided")
	}
	for pfx, rawRule := range c.Rules {
		if rule, err := ParseRule(rawRule); err != nil {
			return err
		} else {
			if rule.Warm <= 0 {
				return fmt.Errorf("missing 'warm' in rule '%s'", pfx)
			}
			if rule.Move <= 0 {
				return fmt.Errorf("missing 'move' in rule '%s'", pfx)
			}
			if rule.Cold <= 0 {
				return fmt.Errorf("missing 'cold' in rule '%s'", pfx)
			}
			if rule.Delete <= 0 {
				return fmt.Errorf("missing 'delete' in rule '%s'", pfx)
			}
			if rule.Warm > rule.Move {
				return fmt.Errorf("'warm' should be smaller than or equal to 'move' in rule '%s'", pfx)
			}
			if rule.Move >= rule.Cold {
				return fmt.Errorf("'move' should be smaller than 'cold' in rule '%s'", pfx)
			}
			if rule.Cold >= rule.Delete {
				return fmt.Errorf("'cold' should be smaller than 'delete' in rule '%s'", pfx)
			}
		}
	}
	return nil
}

func (c Conf) ShouldSkip(index string) bool {
	// skip blacklist
	for _, pfx := range c.Blacklist {
		if strings.HasPrefix(index, pfx) {
			return true
		}
	}
	return false
}

func (c Conf) FindRule(index string) (Rule, bool) {
	// 倒序排列前缀，让更长的前缀规则更早匹配
	var prefixes []string
	for prefix := range c.Rules {
		prefixes = append(prefixes, prefix)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(prefixes)))

	for _, prefix := range prefixes {
		if strings.HasPrefix(index, prefix) {
			rule, _ := ParseRule(c.Rules[prefix])
			return rule, true
		}
	}
	return Rule{}, false
}
