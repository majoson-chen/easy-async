# easy_async

请你忘记 `asyncio` 的存在。

这是一个对 `asyncio` 进行高度封装的库，让你可以像写同步代码一样写异步代码，支持多线程，多进程。

## 功能

| 功能     | 支援 |
|--------|----|
| 多线程    | ✅  |
| 多进程    | ✅  |
| 简单易用   | ✅  |
| 全局退出管理 | ✅  |

## 安装

```bash
pip install easy-async
```

## 使用

```python
import asyncio
import multiprocessing
import threading

from easy_async import EasyAsync, MISSION

# scheduler will initial when every process start,
# so different process will have different scheduler
#                                                Maximum 4k concurrent mission-task
scheduler = EasyAsync(task_default_type=MISSION, complicating_limit=4096)


# if you don't create scheduler before get_scheduler, it will create a scheduler with default params
# scheduler = get_scheduler()

# when you decorate a function with scheduler.as_mission,
# it will be use `scheduler.create_task` when you call it
@scheduler.as_mission
async def sleep(n):
    print(f'mission_task: sleep {n}')
    await asyncio.sleep(n)
    print(f'wake up: {n}')

    # 在此处不要触发循环，stop之后函数需要返回。

    if n == 5:
        print('sys stop')
        scheduler.stop()
        # scheduler.stop(force=True)


# assign it is a daemon task, the scheduler will stop when all daemon-task finish.
# when you call a non-args `scheduler.run` all registered daemon-task will be called.
@scheduler.daemon()
async def assist():
    print('assist1 start')
    print('new_pid: ', multiprocessing.current_process().pid)
    print('new_tid: ', threading.current_thread().native_id)
    await sleep(1)

    print('assist1 end')


@scheduler.daemon()
async def main():
    print('main start')
    print('mian scheduler', scheduler)

    # start_by_process
    scheduler.new(assist, method='process')

    # start mission-task manually
    await scheduler.add_mission(sleep(5))

    # start_by_thread
    # scheduler.new(assist, method='thread')

    # just use `scheduler.new` like `Process` or `Thread`

    await asyncio.sleep(3)
    print('main end')


if __name__ == '__main__':
    # run all daemon_task
    # scheduler.run()

    # just run main()
    scheduler.run(main())

```