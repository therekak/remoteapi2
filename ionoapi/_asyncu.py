import time
import signal
import warnings
import contextlib
import asyncio
import threading

class LoopExit(SystemExit):
    code = 1


class Loop(object):

    @staticmethod
    def eventloop():
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return asyncio.get_event_loop()
        except RuntimeError as ex:
            if "There is no current event loop in thread" in str(ex):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return asyncio.get_event_loop()

    def onsig_loopexit(self, *args):
        self.astop.set()
        raise LoopExit()

    @property
    def started(self):
        return self._loop is not None

    @property
    def loop(self):
        from iapi import Logger
        if not self._loop:
            try:
                self._loop = self.eventloop()
                assert self._loop is not None
                asyncio.set_event_loop(self._loop)
                self.astop = asyncio.Event()
                self.alock = asyncio.Lock()
                self._shutdown = asyncio.Event()
                self.ismain = threading.current_thread() is threading.main_thread()
                if self.ismain:
                    for sig in (signal.SIGINT, signal.SIGTERM):
                        self._loop.add_signal_handler(sig, self.onsig_loopexit)
            except Exception as e:
                Logger.logger.error(f'Unable to create Event Loop: {e}')
                exit(0)

        return self._loop

    def clearStopEvent(self):
        _ = self.loop
        self.astop.clear()

    def setStopEvent(self):
        _ = self.loop
        self.astop.set()

    def __init__(self, loop=None):
        self._loop = loop
        self.astop = None
        self.alock = None
        self.ismain = None
        self.tasks = list()
        self._shutdown = None

    def close(self):
        from iapi import Logger
        if self._shutdown is None or self._shutdown.is_set() or self.loop.is_closed():
            return

        self._shutdown.set()

        try:
            all_tasks = asyncio.gather(*asyncio.all_tasks(self.loop), return_exceptions=True)
            all_tasks.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                self.loop.run_until_complete(all_tasks)
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        except Exception as e:
            Logger.logger.critical(f'Error {e} while shutting-down Event Loop')
            self._shutdown.reset()
        finally:
            try:
                self.loop.close()
                Logger.logger.debug(f'Event Loop Shutdown')
            except:
                self._shutdown.reset()

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            pass


class AsyncCTXClass:

    @property
    def loop(self):
        return self.loop_.loop

    def onsig_loopexit(self, *args):
        raise LoopExit()

    def startloop(self):
        self.loop_ = Loop() if self._loopowner else self._l
        if self._loopowner and self.loop_.ismain:
            for sig in (signal.SIGINT, signal.SIGTERM):
                self.loop.add_signal_handler(sig, self.onsig_loopexit)

        self.listener_cancellation_event = None

    def __init__(self, *args, loop: Loop=None, startloop=True, _logprefix=None, **kwargs):
        self._logprefix = _logprefix
        self._loopowner = bool(loop is None)
        self._l = None
        if loop is not None:
            self._l = loop if isinstance(loop, Loop) else Loop(loop=loop)
        if startloop:
            self.startloop()

    async def disconnect(self):
        pass

    def close(self):
        from iapi import Logger
        if (not self.loop_ or not self.loop_.started) or self.loop.is_closed():
            return

        time.sleep(0.1)
        Logger[self._logprefix].debug(f'Closing Asyncio <{self.__class__.__name__}>')
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                self.loop.run_until_complete(self.disconnect())
        except Exception as e:
            pass
        finally:
            if self._loopowner:
                self.loop_.close()
                Logger[self._logprefix].debug(f'Background Asyncio Loop Stopped')

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()