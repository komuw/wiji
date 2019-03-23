## `wiji` changelog:
most recent version is listed first.

## **version:** v0.1.5
- fix examples broker which was broken by last PR: https://github.com/komuw/wiji/pull/35


## **version:** v0.1.4
- `wiji.broker.BaseBroker` interface should have a method that is called during shutdown
  and `wiji.Worker` calls this method when it receives a shutdown signal like `SIGTERM`: https://github.com/komuw/wiji/pull/34

## **version:** v0.1.3
- all task invocations should have unique task_id: https://github.com/komuw/wiji/pull/32

## **version:** v0.1.2
- first release