package main

// Options options for logtubed
type Options struct {
	Verbose  bool   `yaml:"verbose" default:"$LOGTUBED_VERBOSE|false"`
	Hostname string `yaml:"hostname"`
	PProf    struct {
		Bind  string `yaml:"bind" default:"$LOGTUBED_PPROF_BIND|0.0.0.0:6060"`
		Block int    `yaml:"block" default:"$LOGTUBED_PPROF_BLOCK|0"`
		Mutex int    `yaml:"mutex" default:"$LOGTUBED_PPROF_MUTEX|0"`
	} `yaml:"pprof"`
	InputRedis struct {
		Enabled    bool   `yaml:"enabled" default:"$LOGTUBED_REDIS_ENABLED|false"`
		Bind       string `yaml:"bind" default:"$LOGTUBED_REDIS_BIND|0.0.0.0:6379"`
		Multi      bool   `yaml:"multi" default:"$LOGTUBED_REDIS_MULTI|false"`
		TimeOffset int    `yaml:"time_offset" default:"$LOGTUBED_REDIS_TIME_OFFSET|0"`
	} `yaml:"input_redis"`
	InputSPTP struct {
		Enabled bool   `yaml:"enabled" default:"$LOGTUBED_SPTP_ENABLED|false"`
		Bind    string `yaml:"bind" default:"$LOGTUBED_SPTP_BIND|0.0.0.0:9921"`
	} `yaml:"input_sptp"`
	Topics struct {
		KeywordRequired []string `yaml:"keyword_required" default:"$LOGTUBED_TOPICS_KEYWORD_REQUIRED|[]"`
		Ignored         []string `yaml:"ignored" default:"$LOGTUBED_TOPICS_IGNORED|[]"`
		Priors          []string `yaml:"priors" default:"$LOGTUBED_TOPICS_PRIORS|[]"`
	} `yaml:"topics"`
	Queue struct {
		Dir       string `yaml:"dir" default:"$LOGTUBED_QUEUE_DIR|/var/lib/logtubed"`
		Name      string `yaml:"name" default:"$LOGTUBED_QUEUE_NAME|logtubed"`
		SyncEvery int    `yaml:"sync_every" default:"$LOGTUBED_QUEUE_SYNC_EVERY|100"`
	} `yaml:"queue"`
	OutputSlowSQL struct {
		Enabled   bool   `yaml:"enabled" default:"$OUTPUT_SLOW_SQL_ENABLED|false"`
		URL       string `yaml:"url" default:"$OUTPUT_SLOW_SQL_URL|"`
		Threshold int    `yaml:"threshold" default:"$OUTPUT_SLOW_SQL_THRESHOLD|3000"`
		Topic     string `yaml:"topic" default:"$OUTPUT_SLOW_SQL_TOPIC|x-mybatis-track"`
	} `yaml:"output_slow_sql"`
	OutputES struct {
		Enabled      bool     `yaml:"enabled" default:"$LOGTUBED_ES_ENABLED|false"`
		URLs         []string `yaml:"urls" default:"$LOGTUBED_ES_URLS|[\"http://127.0.0.1:9200\"]"`
		Concurrency  int      `yaml:"concurrency" default:"$LOGTUBED_ES_CONCURRENCY|3"`
		BatchSize    int      `yaml:"batch_size" default:"$LOGTUBED_ES_BATCH_SIZE|100"`
		BatchTimeout int      `yaml:"batch_timeout" default:"$LOGTUBED_ES_BATCH_TIMEOUT|3"`
	} `yaml:"output_es"`
	OutputLocal struct {
		Enabled bool   `yaml:"enabled" default:"$LOGTUBED_LOCAL_ENABLED|false"`
		Dir     string `yaml:"dir" default:"$LOGTUBED_LOCAL_DIR|/var/log/logtubed"`
	} `yaml:"output_local"`
}
