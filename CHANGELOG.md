## `wiji` changelog:
most recent version is listed first.


## **version:** v0.1.6-beta.5
- add better error messages : https://github.com/komuw/wiji/pull/54
- Stop capturing `SIGINT` signal : https://github.com/komuw/wiji/pull/55

## **version:** v0.1.6-beta.4
- rename CLI option, `--config` to `--app` : https://github.com/komuw/wiji/pull/52

## **version:** v0.1.6-beta.3
- bugfix, `task.task_options is stale`    
  We had a case where `broker.done` would get called with `task_id`==`''`(empty string) : https://github.com/komuw/wiji/pull/39
- add timestamp to log events: https://github.com/komuw/wiji/pull/45
- remove `wiji.task.WijiRetryError`.   
  we replace it with a `wiji.task.Task._RETRYING` boolean.: https://github.com/komuw/wiji/pull/46
- rename wiji conf file: https://github.com/komuw/wiji/pull/47
- `wiji` no longer requires task instances in order to start running : https://github.com/komuw/wiji/pull/49

## **version:** v0.1.5
- fix examples broker which was broken by last PR: https://github.com/komuw/wiji/pull/35

## **version:** v0.1.4
- `wiji.broker.BaseBroker` interface should have a method that is called during shutdown
  and `wiji.Worker` calls this method when it receives a shutdown signal like `SIGTERM`: https://github.com/komuw/wiji/pull/34

## **version:** v0.1.3
- all task invocations should have unique task_id: https://github.com/komuw/wiji/pull/32

## **version:** v0.1.2
- first release
