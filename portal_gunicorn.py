"""Keep Gunicorn's graceful socket draining outside its request event loop.

Compatible with the pinned Gunicorn 26.2.0 gthread worker. The normal parser,
request handling, keepalive management, and immediate-close paths stay upstream.
"""

from __future__ import annotations

from dataclasses import dataclass
import queue
import selectors
import socket
import threading
import time

from gunicorn.workers.gthread import ThreadWorker


@dataclass
class _ClosingSocket:
    sock: socket.socket
    deadline: float
    drained: int = 0


class GracefulCloseManager:
    """One selector thread, with a shared cap on queued and draining sockets.

    FIN is followed by at most two seconds / 64 KiB of nonblocking reads, as in
    Gunicorn's close_graceful. At admission exhaustion, send FIN and immediately
    close instead of blocking the request loop or allocating an unbounded queue.
    """

    def __init__(self, *, capacity=512, linger=2.0, max_drain=65536):
        if capacity < 1 or linger <= 0 or max_drain < 1:
            raise ValueError("positive close-manager limits required")
        self.capacity = capacity
        self.linger = linger
        self.max_drain = max_drain
        self._slots = threading.BoundedSemaphore(capacity)
        self._state_lock = threading.Lock()
        self._stopping = threading.Event()
        self._pending = queue.SimpleQueue()
        self._selector = selectors.DefaultSelector()
        self._wake_read, self._wake_write = socket.socketpair()
        self._wake_read.setblocking(False)
        self._wake_write.setblocking(False)
        self._selector.register(self._wake_read, selectors.EVENT_READ, None)
        self._thread = threading.Thread(target=self._run,
                                        name="portal-socket-close", daemon=True)
        self._thread.start()

    @staticmethod
    def _close(sock, *, send_fin=False):
        try:
            if send_fin:
                sock.setblocking(False)
                try:
                    sock.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
        finally:
            sock.close()

    def _wake(self):
        try:
            self._wake_write.send(b"\x00")
        except OSError:
            # A full wake socket is already readable; shutdown also lands here.
            pass

    def submit(self, sock):
        """Transfer ownership without waiting for a peer, queue slot, or lock."""
        admitted = False
        if self._state_lock.acquire(blocking=False):
            try:
                if not self._stopping.is_set() and self._slots.acquire(blocking=False):
                    self._pending.put(_ClosingSocket(sock, time.monotonic() + self.linger))
                    admitted = True
            finally:
                self._state_lock.release()
        if admitted:
            self._wake()
        else:
            self._close(sock, send_fin=True)
        return admitted

    def stop(self):
        """Called during worker shutdown, after its request loop has stopped."""
        with self._state_lock:
            self._stopping.set()
        self._wake()
        self._thread.join(timeout=0.5)

    def _run(self):
        active = {}

        def finish(state):
            active.pop(id(state), None)
            try:
                self._selector.unregister(state.sock)
            except (KeyError, ValueError, OSError):
                pass
            try:
                self._close(state.sock)
            finally:
                self._slots.release()

        try:
            while not self._stopping.is_set():
                # Reservations bound both this queue and active selector entries.
                while True:
                    try:
                        state = self._pending.get_nowait()
                    except queue.Empty:
                        break
                    active[id(state)] = state
                    try:
                        state.sock.setblocking(False)
                        state.sock.shutdown(socket.SHUT_WR)
                        if state.deadline <= time.monotonic():
                            finish(state)
                        else:
                            self._selector.register(state.sock, selectors.EVENT_READ, state)
                    except (OSError, ValueError):
                        finish(state)
                now = time.monotonic()
                for state in list(active.values()):
                    if state.deadline <= now:
                        finish(state)
                timeout = min([0.1] + [max(0, state.deadline - now)
                                       for state in active.values()])
                for key, _ in self._selector.select(timeout):
                    state = key.data
                    if state is None:
                        try:
                            self._wake_read.recv(65536)
                        except (BlockingIOError, OSError):
                            pass
                        continue
                    try:
                        data = state.sock.recv(min(4096, self.max_drain - state.drained))
                    except BlockingIOError:
                        continue
                    except OSError:
                        finish(state)
                        continue
                    state.drained += len(data)
                    if not data or state.drained >= self.max_drain:
                        finish(state)
        finally:
            # Serialize the admission cutoff before draining the remaining queue.
            with self._state_lock:
                self._stopping.set()
            for state in list(active.values()):
                finish(state)
            while True:
                try:
                    state = self._pending.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._close(state.sock, send_fin=True)
                finally:
                    self._slots.release()
            self._selector.close()
            self._wake_read.close()
            self._wake_write.close()


class PortalThreadWorker(ThreadWorker):
    def init_process(self):
        # Gunicorn calls this in the forked worker, never in the master process.
        self._portal_closer = GracefulCloseManager()
        try:
            super().init_process()
        finally:
            self._portal_closer.stop()

    def finish_request(self, conn, future):
        # This hook and its connection are owned by the Gunicorn main loop.
        # Intercept just this connection's graceful-close call while delegating
        # all result/keepalive/error bookkeeping to the pinned upstream worker.
        original_close = conn.close

        def close(graceful=False):
            if graceful:
                self._portal_closer.submit(conn.sock)
            else:
                original_close(graceful=False)

        conn.close = close
        try:
            return super().finish_request(conn, future)
        finally:
            conn.close = original_close
