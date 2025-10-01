# uia_helper.py
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from pywinauto import Application, keyboard
from pywinauto.findwindows import ElementAmbiguousError, ElementNotFoundError
from pywinauto.timings import TimeoutError as PywinautoTimeoutError

# Simple alias for selector dictionaries you pass to child_window/window
Criteria = Dict[str, Any]  # e.g. {"auto_id": "LoginButton", "control_type": "Button"}


@contextmanager
def _temporary_chdir(target: Optional[Path]):
    """Temporarily change the working directory (safe even if target is None)."""
    if not target:
        yield
        return
    previous = Path.cwd()
    os.chdir(str(target))
    try:
        yield
    finally:
        os.chdir(str(previous))


class UIAHelper:
    """
    High-level helper for pywinauto (backend='uia').

    Covers:
      - Window actions: focus/minimize/maximize/restore/close/send_keys/wait_app_idle
      - Control actions: click/invoke/get_text/set_text/check/select/expand/collapse
      - Waits: wait_exists / wait_gone
      - Deep search: find_by_path
      - Utilities: draw_outline / screenshot / rect
      - Convenience: click_by_id / get_text_by_id
      - App lifecycle: start / start_in_folder / connect_by_title / connect_by_process / connect_or_start
    """

    __slots__ = ("app", "default_timeout", "retry_attempts", "retry_backoff_sec")

    def __init__(
        self,
        app: Application,
        default_timeout: float = 10.0,
        retry_attempts: int = 2,
        retry_backoff_sec: float = 0.4,
    ) -> None:
        """
        Parameters
        ----------
        app : Application
            A pywinauto Application instance (backend='uia').
        default_timeout : float
            Default timeout for waits/resolution.
        retry_attempts : int
            Number of extra retry attempts for flaky ops.
        retry_backoff_sec : float
            Base seconds for exponential backoff between retries.
        """
        self.app = app
        self.default_timeout = float(default_timeout)
        self.retry_attempts = int(retry_attempts)
        self.retry_backoff_sec = float(retry_backoff_sec)

    # ---------- Factories ----------

    @classmethod
    def connect_by_title(
        cls,
        title: Optional[str] = None,
        title_re: Optional[str] = None,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> "UIAHelper":
        app = Application(backend="uia").connect(
            best_match=title if title else None,
            title=title,
            title_re=title_re,
            timeout=timeout,
        )
        return cls(app, default_timeout=timeout, **kwargs)

    @classmethod
    def connect_by_process(cls, pid: int, timeout: float = 10.0, **kwargs: Any) -> "UIAHelper":
        app = Application(backend="uia").connect(process=pid, timeout=timeout)
        return cls(app, default_timeout=timeout, **kwargs)

    @classmethod
    def start(cls, cmd_line: str, timeout: float = 15.0, **kwargs: Any) -> "UIAHelper":
        """Start an application from a full command line (current working dir)."""
        app = Application(backend="uia").start(cmd_line, timeout=timeout)
        return cls(app, default_timeout=timeout, **kwargs)

    @classmethod
    def start_in_folder(
        cls,
        app_name: str,
        folder_path: str,
        args: Optional[List[str]] = None,
        timeout: float = 15.0,
        **kwargs: Any,
    ) -> "UIAHelper":
        """
        Start an app by executable name using a specific working folder.
        Many apps rely on that working directory for relative assets.
        """
        base = Path(folder_path).expanduser().resolve()
        exe = base / app_name
        if not exe.exists():
            raise FileNotFoundError(f"Executable not found: {exe}")

        cmd = f'"{str(exe)}"'
        if args:
            quoted_args = " ".join([f'"{a}"' if " " in str(a) else str(a) for a in args])
            cmd = f"{cmd} {quoted_args}"

        with _temporary_chdir(base):
            app = Application(backend="uia").start(cmd, timeout=timeout)

        return cls(app, default_timeout=timeout, **kwargs)

    @classmethod
    def connect_or_start(
        cls,
        title: Optional[str] = None,
        title_re: Optional[str] = None,
        app_name: Optional[str] = None,
        folder_path: Optional[str] = None,
        args: Optional[List[str]] = None,
        timeout: float = 15.0,
        **kwargs: Any,
    ) -> "UIAHelper":
        """
        Try to connect by title/title_re; if that fails and (app_name & folder_path)
        are provided, start the app in that folder.
        """
        if title or title_re:
            try:
                return cls.connect_by_title(title=title, title_re=title_re, timeout=timeout, **kwargs)
            except Exception:
                if not (app_name and folder_path):
                    raise

        if app_name and folder_path:
            return cls.start_in_folder(app_name, folder_path, args=args, timeout=timeout, **kwargs)

        raise ValueError("Provide either (title/title_re) to connect OR (app_name & folder_path) to start.")

    # ---------- Core resolution ----------

    def window(self, **win_criteria: Any) -> Any:
        """Return a WindowSpecification for a top-level window."""
        return self.app.window(**win_criteria) if win_criteria else self.app.top_window()

    def _resolve(
        self,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> Any:
        """
        Resolve a control to a Wrapper object, waiting until it's ready.
        If parent is given, search under it; otherwise, search under the top window.
        """
        spec = (parent.child_window(**ctrl_criteria) if parent else self.app.top_window().child_window(**ctrl_criteria))
        spec.wait("exists enabled visible ready", timeout=timeout or self.default_timeout)
        return spec.wrapper_object()

    def _retry(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Retry common flaky UI ops a few times with exponential backoff."""
        attempts = self.retry_attempts + 1
        for i in range(attempts):
            try:
                return func(*args, **kwargs)
            except (ElementNotFoundError, ElementAmbiguousError, PywinautoTimeoutError):
                if i == attempts - 1:
                    raise
                time.sleep(self.retry_backoff_sec * (2 ** i))

    # ---------- Window-level actions ----------

    def focus_window(self, **win_criteria: Any) -> None:
        self.window(**win_criteria).wait("ready", timeout=self.default_timeout)
        self.window(**win_criteria).set_focus()

    def minimize(self, **win_criteria: Any) -> None:
        self.window(**win_criteria).minimize()

    def maximize(self, **win_criteria: Any) -> None:
        self.window(**win_criteria).maximize()

    def restore(self, **win_criteria: Any) -> None:
        self.window(**win_criteria).restore()

    def close_window(self, **win_criteria: Any) -> None:
        self.window(**win_criteria).close()

    def send_keys(self, keys: str, set_focus: bool = True, **win_criteria: Any) -> None:
        if set_focus:
            self.focus_window(**win_criteria)
        keyboard.send_keys(keys, with_spaces=True, pause=0.01)

    def wait_app_idle(self, cpu_lower: float = 5.0, timeout: float = 20.0) -> None:
        """Heuristic wait for app to ‘settle’ (low CPU)."""
        self.app.wait_cpu_usage_lower(threshold=cpu_lower, timeout=timeout)

    # ---------- Control-level actions ----------

    def click(
        self,
        double: bool = False,
        right: bool = False,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Click a control by criteria (auto_id, control_type, name, etc.)."""
        def _action() -> None:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
            if right:
                widget.right_click_input()
            elif double:
                widget.double_click_input()
            else:
                widget.click_input()

        self._retry(_action)

    def invoke(
        self,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Invoke the control (InvokePattern) or click if not available."""
        def _action() -> None:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
            if hasattr(widget, "invoke"):
                widget.invoke()
            else:
                widget.click_input()

        self._retry(_action)

    def get_text(
        self,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> str:
        """Retrieve text; try .window_text() first, then .texts()."""
        def _action() -> str:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
            try:
                txt = widget.window_text()
                if txt:
                    return txt.strip()
            except Exception:
                pass

            try:
                parts = [t.strip() for t in widget.texts() if t and t.strip()]
                return "\n".join(parts)
            except Exception:
                return ""

        return self._retry(_action)

    def set_text(
        self,
        text: str,
        clear: bool = True,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Type into an Edit/Text control; fall back to focus+type when needed."""
        def _action() -> None:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
            try:
                if hasattr(widget, "set_text"):
                    if clear:
                        widget.set_text("")
                    widget.set_text(text)
                else:
                    widget.set_focus()
                    if clear:
                        keyboard.send_keys("^a{BACKSPACE}")
                    keyboard.send_keys(text, with_spaces=True, pause=0.01)
            except Exception:
                widget.set_focus()
                if clear:
                    keyboard.send_keys("^a{BACKSPACE}")
                keyboard.send_keys(text, with_spaces=True, pause=0.01)

        self._retry(_action)

    def check(
        self,
        state: bool,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Set a CheckBox to True/False (uses Toggle pattern when available)."""
        def _action() -> None:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
            if hasattr(widget, "get_toggle_state"):
                desired = 1 if state else 0
                # Try up to 3 toggles to escape indeterminate (2)
                for _ in range(3):
                    current = widget.get_toggle_state()
                    if current == desired:
                        return
                    widget.toggle()
                    time.sleep(0.1)
            else:
                widget.click_input()

        self._retry(_action)

    def select(
        self,
        item: Union[str, int],
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Select a ListItem/ComboBox entry."""
        def _action() -> None:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
            if hasattr(widget, "select"):
                widget.select(item)
            else:
                widget.click_input()
                if isinstance(item, int):
                    keyboard.send_keys("{DOWN " + str(item) + "}{ENTER}")
                else:
                    keyboard.send_keys(str(item) + "{ENTER}", with_spaces=True)

        self._retry(_action)

    def expand(
        self,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Expand an expandable control."""
        def _action() -> None:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
            if hasattr(widget, "expand"):
                widget.expand()
            else:
                widget.double_click_input()

        self._retry(_action)

    def collapse(
        self,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Collapse an expandable control."""
        def _action() -> None:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
            if hasattr(widget, "collapse"):
                widget.collapse()
            else:
                widget.double_click_input()

        self._retry(_action)

    # ---------- Waiting / existence ----------

    def wait_exists(
        self,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> bool:
        spec = (parent.child_window(**ctrl_criteria) if parent
                else self.app.top_window().child_window(**ctrl_criteria))
        return spec.exists(timeout=timeout or self.default_timeout)

    def wait_gone(
        self,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> bool:
        spec = (parent.child_window(**ctrl_criteria) if parent
                else self.app.top_window().child_window(**ctrl_criteria))
        return spec.wait_not("exists", timeout=timeout or self.default_timeout)

    # ---------- Deep-tree resolution ----------

    def find_by_path(self, path: List[Criteria], timeout_each: Optional[float] = None) -> Any:
        """
        Resolve a deeply nested control by walking a list of child criteria.

        Example:
            [
                {"title": "My App"},
                {"auto_id": "MainView",  "control_type": "Pane"},
                {"auto_id": "LoginForm", "control_type": "Group"},
                {"auto_id": "LoginButton","control_type": "Button"},
            ]
        First item can point to a window; the rest are child_window criteria.
        """
        if not path:
            raise ValueError("Empty path")

        head = path[0]
        win = self.window(**head)
        win.wait("ready", timeout=self.default_timeout)

        spec = win
        for step in path[1:]:
            spec = spec.child_window(**step)
            spec.wait("exists enabled visible ready", timeout=timeout_each or self.default_timeout)

        return spec.wrapper_object()

    # ---------- Utilities ----------

    def draw_outline(
        self,
        seconds: float = 0.6,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Highlight a control’s bounds temporarily (useful for debugging)."""
        widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
        try:
            widget.draw_outline()
            time.sleep(seconds)
        finally:
            try:
                widget.draw_outline()  # redraw clears in most cases
            except Exception:
                pass

    def screenshot(
        self,
        path: str,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ) -> None:
        """Save a screenshot of a control (or top window if no criteria)."""
        if ctrl_criteria or parent:
            widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
        else:
            widget = self.app.top_window().wrapper_object()
        image = widget.capture_as_image()
        image.save(path)

    def rect(
        self,
        timeout: Optional[float] = None,
        parent: Optional[Any] = None,
        **ctrl_criteria: Any,
    ):
        """Return the control’s bounding rectangle."""
        widget = self._resolve(timeout=timeout, parent=parent, **ctrl_criteria)
        return widget.rectangle()

    # ---------- Convenience for common selectors ----------

    def click_by_id(self, auto_id: str, control_type: Optional[str] = None, **extras: Any) -> None:
        criteria: Dict[str, Any] = {"auto_id": auto_id}
        if control_type:
            criteria["control_type"] = control_type
        criteria.update(extras)
        self.click(**criteria)

    def get_text_by_id(self, auto_id: str, control_type: Optional[str] = None, **extras: Any) -> str:
        criteria: Dict[str, Any] = {"auto_id": auto_id}
        if control_type:
            criteria["control_type"] = control_type
        criteria.update(extras)
        return self.get_text(**criteria)
