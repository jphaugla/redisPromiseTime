# Related Tutorials

Code for stream process all came for here
* [Stream Processing with Redis and Spring Boot Data](https://howtodoinjava.com/spring-data/redis-streams-processing/)

This github has a producer and a consumer using a common record structure to produce events and write these events to redis.

The events are worker order events with a facility id, work order for the facility, work queue and two dates:  promise time and verification time.

Redis will hold all the work orders in raw fashion but additionally will use sorted sets and increments to provide real time metrics on the work orders including success or failure at work orders meeting the promise date.

| Logical Name    | Naming                                 | Example Name                        | Type                                          | Description                                                               | Used For                                                                |
|-----------------|----------------------------------------|-------------------------------------|-----------------------------------------------|---------------------------------------------------------------------------|-------------------------------------------------------------------------|
| Order Promise   | Order:<facility>:<workorder>:<promise> | Order:028219:1:2023-05-06 12:30:30	 | Hash                                          | Contains full information for each order event                            | Keep detail information from every promised work order                  |
| Unfilled Order  | Unfilled:<facility>:<queue>	           | Unfilled:028219:QV1	                | Sorted set	                                   | Value is worker with score of promise date	                               | For each store and queue can retrieve workorders sorted by promise date |
| Filled Order	   | Filled:<facility>:<queue>              | 	Filled:028219:QV1	                 | Sorted set	Value is key for the order promise | Score is promise date.	Index into the Order Promise table by promise date |                                                                         |
| Store Success 	 | Success:<facility>                     | 	Success:028219	                    | Hash                                          | 	Success and Failure Counter for each store                               | 	retrieve number of successes and failure by store                      |