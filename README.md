# timescaledb_test

## Instructions
### 
1. Clone this repo and cd to src directory:
   1. git clone git@github.com:snwight/timescaledb_test.git
   2. cd ./timescaledb_test/cpu_usage
2. Pull the timescaledb docker image, initialize database, load sample data, and build the takehome src:
   1. docker compose build
3. Run the takehome executable: 
   1. docker compose run cpu_usage

## Rationale
###
1. Concurrent workers are modeled here using goroutines.
   1. Total query processing time is aggregated across each worker goroutine's invocation of pgx.Query(), resulting in a total duration longer than the time expended by the user back on Earth. 
2. Go channels are used to pass data as well as state management, to wit:
    ``` 
	completionChannel := make(chan (QueryStats))
	terminationChannel := make(chan bool)
	reportChan := make(chan QueryStatReport)
    ``` 
3. Anonymous go funcs are used to instantiate the workers, each of whom owns a unique receive channel which is mapped to query hostname using a [very simplistic] hashing function. 
    ```
	hostChannelMap := spawnWorkers(pool, numWorkers, completionChannel, terminationChannel)
    ```
4. The aggregation of worker statistics is done in the completionTasks function, run as a goroutine. It is basically a select statement with some summing functionality, as well as the synchronization point where we await input parameter CSV exhaustion by all of our worker goroutines.
    ```
	go completionTasks(time.Now(), completionChannel, terminationChannel, reportChan, numWorkers)
    ```
