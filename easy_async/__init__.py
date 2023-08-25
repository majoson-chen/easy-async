import asyncio
import contextvars
import multiprocessing as mprocess
import signal
import threading
import traceback
import typing as t
from atexit import register as atexit_register
from abc import abstractmethod, ABC
from asyncio.events import AbstractEventLoop
from functools import wraps, partial, cache
from multiprocessing import Process
from multiprocessing.process import BaseProcess
from threading import Thread
from uuid import uuid4

DAEMON = "daemon"
MISSION = "mission"

THREAD = "thread"
PROCESS = "process"


# 用法：
# 在主进程创建首个
# ｜ - 使用调度器创建子线程。
# ｜ - 使用调度器创建子进程。

# create_task: 传入一个 coro，添加到当前 loop 执行。
# create_thread: 传入一个 coro，创建新线程执行。
# create_process: 传入一个 coro，在新进程中执行。

# 每个进程都有一个 root 节点，主进程会创建锁，子进程会获取锁。
# 当主进程发出退出信号，广播至所有调度器。


# 退出
# 情况1: Ctrl+C 或者进程被 TERM(可能来自父进程)，被 signal 捕获，捕获器触发 调用 stop。
# 情况2: 被手动调用调度器的 stop，只会结束他的子进程或者子线程，不影响全局。
# 情况3: daemon-tasks 执行完毕。
# 1. 向所有子进程发送 terminate(进程接收到会去调用 stop)
# 2. 调用所有子线程的 stop
# 3. 所有 daemon-task cancel
# 之后流程第三步就会开始执行

# 注意点：
# daemon-task 中不能出现同步的阻塞任务。

# 创建新调度器：
# 1. 创建自身 force_quit_event(线程是线程 evnet， 进程是进程 event)
# 2. 继承父 force_quit_event，对于 root scheduler，是其本身的.
# 2. call _fork (在新进程、线程中)
# ======== in _fork ===========
# 参数： force_quit event (每个调度器两个，一个由父传递，一个由自身创建，会传递给子调度器)
# 参数： target_coro(to_run) 可选，传入到新实例的 run() 中。
# 参数： Semaphore(用于限制并发)(如果是 process-scope-limit), 否则创建新的 Semaphore
# 参数： 其他需要参数
# 调用 EasyAsync.__init__()
# 调用 EasyAsync.run()


class AbstractChildSchedulerRef(ABC):
    ident: int
    type: t.Union[MISSION, DAEMON]
    _filtered_args: t.Dict

    def __init__(self, type, **kwargs):  # 此处 **kwargs 用于防止接收到其他参数，报错
        self.type = type

        # Prepare arg for Thread | Process
        self._filtered_args = {
            'group': kwargs.get('group'),
            'name': kwargs.get('name'),
            'target': kwargs.get('target'),
            'args': kwargs.get('args', tuple()),
            'kwargs': kwargs.get('kwargs'),
            'daemon': kwargs.get('daemon')
        }

        if not self._filtered_args['name']:
            # default name
            kwargs['name'] = f"EasyAsync-scheduler: {uuid4()}"

    @property
    def id(self):
        return f'{self.type} - {self.ident}'

    @abstractmethod
    def wait(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def kill(self):
        pass


class ProcessSchedulerRef(Process, AbstractChildSchedulerRef):

    def __init__(self, **kwargs):
        # cannot pickle

        AbstractChildSchedulerRef.__init__(self, **kwargs)

        # _filtered_args: from AbstractChildScheduler
        Process.__init__(self, **self._filtered_args)

    def wait(self):
        self.join()

    def stop(self):
        # 发送 signal.SIG_TERM 信号。
        self.terminate()


class ThreadSchedulerRef(Thread, AbstractChildSchedulerRef):

    def __init__(self, *args, **kwargs):
        # for AbstractChildScheduler
        AbstractChildSchedulerRef.__init__(self, *args, **kwargs)

        # for Thread
        Thread.__init__(self, **self._filtered_args)

    def wait(self):
        self.join()

    def stop(self):
        for inst in EasyAsync._all:
            if inst._thread.native_id == self.native_id:
                inst.stop()
                break

    def kill(self):
        for inst in EasyAsync._all:
            if inst._thread.native_id == self.native_id:
                inst.stop(force=True)
                break


class EasyAsync:
    """
    可以在不同进程，线程之间管理异步

    退出由 SIG_INT 和 SIG_TERM 完成。
    当接收到这个信号，表示子线程被父线程调用
    """

    # 记录一个进程内的所有调度器
    _all: t.List["EasyAsync"] = []

    _children: t.List["AbstractChildSchedulerRef"]
    _waiting_daemons: t.List[t.Coroutine]
    _daemon_tasks: t.Set[asyncio.Task]
    _mission_tasks: t.Set[asyncio.Task]
    _stop_callbacks: t.List[t.Callable]
    running: bool

    _process: BaseProcess
    _loop: AbstractEventLoop
    _thread: threading.Thread

    # 用于限制并发
    _limit_sem: asyncio.Semaphore

    # ====== configs ======
    task_default_type: t.Union[MISSION, DAEMON]
    complicating_limit: int

    _remove_mission_task: t.Callable
    _remove_daemon_task: t.Callable

    @t.overload
    def __init__(
            self,
            *,
            task_default_type: t.Union[MISSION, DAEMON] = MISSION,
            complicating_limit: int = 512,
            redirect_exit: bool = True  # 仅在 root 有效
    ):
        pass

    def __init__(
            self,
            *args,
            **kwargs
            # 隐藏参数
            # force_quit_event: t.Optional[threading.Event] = None,
    ):
        """
        :param task_default_type: What type of task do you expect to create when you call `create_task`.
        :param complicating_limit: the max coro can run at the same time.
        :param redirect_exit:
        :param kwargs:
        """
        default_kwargs = {
            'task_default_type': MISSION,
            'complicating_limit': 512,
            'redirect_exit': True
        }
        default_kwargs.update(kwargs)
        kwargs = default_kwargs

        if len(args):
            raise ValueError("EasyAsync: Please do not pass any nameless parameters")

        # 被调用的情况
        # 手动调用(用于生成 root schedule)
        # 被 _fork 在新进程中被调用
        # 被 _fork 在新线程中被调用

        self._process = mprocess.current_process()
        self._thread = threading.current_thread()

        try:
            self._loop = asyncio.get_event_loop()
        except:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        # disable asyncio warning
        self._loop.set_debug(False)

        self._waiting_daemons = []
        self._children = []
        self.running = False
        self._daemon_tasks = set()
        self._mission_tasks = set()
        self._stop_callbacks = []
        self._remove_mission_task = partial(_remove_task, self._mission_tasks)
        self._remove_daemon_task = partial(_remove_task, self._daemon_tasks)

        # 初始化参数
        self.task_default_type = kwargs.get('task_default_type', MISSION)
        self.complicating_limit = kwargs.get('complicating_limit', 512)

        # 初始化 Semaphore
        self._limit_sem = asyncio.Semaphore(self.complicating_limit)

        # 为进程注册退出事件, 只有进程第一个调度器才能注册
        if len(EasyAsync._all) == 0 and kwargs.get('redirect_exit'):
            self._loop.add_signal_handler(signal.SIGINT, self.stop)
            self._loop.add_signal_handler(signal.SIGTERM, self.stop)
            self._loop.add_signal_handler(signal.SIGQUIT, self.stop)
            atexit_register(self.stop)


        # 注册自身到 Class
        EasyAsync._all.append(self)

    @property
    def is_root(self):
        return self._process.name == 'MainProcess' and self._thread.name == 'MainThread'

    @property
    def id(self):
        return f'{self._process.pid}-{self._thread.native_id}'

    @property
    def running_all_count(self):
        return len(self._daemon_tasks) + len(self._mission_tasks)

    @property
    def running_daemon_count(self):
        return len(self._daemon_tasks)

    @property
    def running_mission_count(self):
        return len(self._mission_tasks)

    def __repr__(self):
        return f"<EasyAsync pid:{self._process.pid}, tid:{self._thread.native_id}, running:{self.running}>"

    async def _add_task(
            self,
            coro: t.Coroutine,
            ls: t.Set,
            name: str = None,
            context: contextvars.Context = None,
    ):
        if not self.running:
            raise RuntimeError("EasyAsync: You can't add a mission task before run()")

        async with self._limit_sem:
            task = self._loop.create_task(coro, name=name, context=context)
            task.add_done_callback(
                self._remove_daemon_task if ls is self._daemon_tasks else self._remove_mission_task
            )
            ls.add(task)

        return task

    async def add_daemon(
            self,
            coro: t.Coroutine,
            # exit_when_crash: bool = False,
    ):
        """
        添加一个守护任务，当任务退出或者所有main-task守护进程时，自动退出。
        :return:
        """

        # TODO: exit when crash.

        await self._add_task(coro, self._daemon_tasks)

    async def add_mission(self, coro: t.Coroutine):
        """
        添加一个后台任务，当任务退出时，等待后台执行完毕再退出
        :return:
        """

        await self._add_task(coro, self._mission_tasks)

    def daemon(self, *args, **kwargs):
        """
        添加到待执行列表，run 时自动执行
        :return:
        """

        # TODO: exit when crash.

        def wrapper(func: t.Callable[..., t.Coroutine]):
            self._waiting_daemons.append(func(*args, **kwargs))
            return func

        return wrapper

    def as_mission(
            self,
            func
            # weight: int = 0
    ):
        """
        将一个函数标注为 mission task，替换目标函数，被调用时自动添加。
        :return:
        """

        print("as_mission", id(func), self)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.create_task(func(*args, **kwargs), method=MISSION)

        return wrapper

    async def create_task(
            self,
            coro: t.Coroutine,
            name: str = None,
            context: contextvars.Context = None,
            method: t.Union[MISSION, DAEMON] = MISSION
    ) -> asyncio.Task:
        """
        just use this like `asyncio.create_task`, but you can specify the type of task.
        """

        return await self._add_task(
            coro=coro,
            ls=self._mission_tasks if method == MISSION else self._daemon_tasks,
            name=name,
            context=context
        )

    async def _wait_daemon(self):
        # 将所有 daemon-task create
        for coro in self._waiting_daemons:
            await self.add_daemon(coro)

        # 等待所有 daemon-task 执行完毕
        # 如果 stop 被调用，所有 daemon 会被 cancel
        while self._daemon_tasks:
            await asyncio.wait(self._daemon_tasks, return_when=asyncio.ALL_COMPLETED)

    def run(
            self,
            target: t.Coroutine = None,
            # 如果指定了 Target，其他先前注册的 task 都会失效。
            # 如果不指定 Target，就会运行先前被注册的任务
    ):
        """
        wait for all daemon-task done.

        :return:
        """

        # 任务执行流程：
        # 1. 将所有 daemon-task create
        # 2. 等待所有 daemon-task 执行完毕 (如果发生了中断，daemon-task 会被取消)
        # 3. 检查是否 force-quit(来自父的)，如果是跳过等待 mission-task
        # 4. 等待所有 mission-task 执行完毕
        # 5. 等待所有子调度器退出(调用子调度器的 wait())

        if target:
            while self._waiting_daemons:
                self._waiting_daemons.pop().close()

            self._waiting_daemons.append(target)

        self.running = True
        self._loop.run_until_complete(self._wait_daemon())
        # 所有 daemon 退出

        # 等待所有 mission 退出
        # 由于在等待的过程中可能会有新的任务产生，所以使用 while 循环
        while self._mission_tasks:
            self._loop.run_until_complete(
                asyncio.wait(self._mission_tasks, return_when=asyncio.ALL_COMPLETED)
            )

        self.running = False
        self._loop.close()

        # 等待所有子调度器退出
        for child in self._children:
            child.wait()

        # exit.

    def stop(self, force: bool = False):
        """
        stop all the task, threading, process.

        :param force: stop immediately.
        :return:
        """

        # call callback
        for callback in self._stop_callbacks:
            try:
                callback()
            except Exception as e:
                print("EasyAsync: WARRING!! Error occurred when calling stop callback.")
                traceback.print_tb(e.__traceback__)

        for daemon_task in self._daemon_tasks:
            daemon_task.cancel()

        if not force:
            # 正常退出
            for child in self._children:
                if child.type == DAEMON:
                    child.stop()
        else:
            # 强制退出
            for mission_task in self._mission_tasks:
                mission_task.cancel()

            for child in self._children:
                child.kill()

        # 删除在 _all 中的记录
        try:
            EasyAsync._all.remove(self)
        except ValueError:
            pass

    def when_stop(self, callback: t.Callable):
        """
        注册一个函数，当退出时会被调用。

        :return:
        """

        self._stop_callbacks.append(callback)

        return callback

    def new(
            self,
            target: t.Callable[..., t.Coroutine],
            # ------------
            # to_target_func
            args: t.Tuple = tuple(),
            kwargs=None,
            # ------------
            method: t.Union[PROCESS, THREAD] = PROCESS,
            type: t.Union[MISSION, DAEMON] = DAEMON,
            name: str = None,
            **kwargs_to_new  # to the new EasyAsync inst.
    ):

        if kwargs is None:
            kwargs = dict()
        if method == PROCESS:
            factory = ProcessSchedulerRef
        elif method == THREAD:
            factory = ThreadSchedulerRef
        else:
            raise ValueError(f"EasyAsync: Unknown factory method {method}")

        kwargs_to_new['target'] = target
        kwargs_to_new['args'] = args
        kwargs_to_new['kwargs'] = kwargs

        # combine with default
        combine_with_default = {
            'complicating_limit': self.complicating_limit,
            'task_default_type': self.task_default_type
        }
        combine_with_default.update(kwargs_to_new)
        combine_with_default['redirect_exit'] = True  # it must be True for subprocess

        # when execute here, will create a new thread/process.
        # _fork is the wrapper for the new task.
        child: AbstractChildSchedulerRef = factory(
            # for process/thread inst.
            target=self._fork,  # 在新进程中调用自身
            kwargs=combine_with_default,
            name=name,  # process | thread name
            group=None,  # process | thread group
            daemon=True if type == DAEMON else False,
            # for scheduler inst
            type=method,
            parent=self,
        )
        child.start()
        self._children.append(child)

    @staticmethod
    def _fork(
            **kwargs,
    ):
        """
        will run in new `thread/process` as the main.
        """

        target = kwargs.pop('target')
        args_to_target = kwargs.pop('args', tuple())
        kwargs_to_target = kwargs.pop('kwargs', dict())

        inst = get_scheduler()
        # inst = EasyAsync(**kwargs)
        inst.run(target=target(*args_to_target, **kwargs_to_target))


def _remove_task(queue: t.Set, task: asyncio.Task):
    queue.discard(task)


def get_scheduler() -> EasyAsync:
    """
    get the scheduler for current thread, if it not exists, create it.

    :return:
    """

    @cache
    def get(thread_id):

        for scheduler in EasyAsync._all:
            if scheduler._thread.native_id == thread_id:
                return scheduler

        # not find
        return EasyAsync()

    return get(
        threading.current_thread().native_id
    )


async def create_task(
        coro: t.Coroutine,
        name: str = None,
        context: contextvars.Context = None,
):
    """
    you can use it just like `asyncio.create_task()`, but will resign to current scheduler.
    :return:
    """

    return await get_scheduler().create_task(coro, name=name, context=context)


def exit(force: bool = True):
    """exit the scheduler globally"""

    EasyAsync._all[0].stop(force=force)
