"""
The transformation regsitry manages the internal state of the transformations
"""

from typing import Any, Callable, Dict, Optional, Union


class TransformationRegistry:
    """A simple class to manage the transformation state.
    A poor mans registry - wraps a dict basically"""

    def __init__(self, initial_state: Optional[Dict[str, Any]] = None) -> None:
        """Takes an initial state if needed"""
        self._state = initial_state if initial_state else {}

    def add_or_update(self, key: str, result: Any) -> None:
        """Add a key value pair"""
        self._state[key] = result

    def get(self, key: str) -> Union[None, Any]:
        """Get a value given a key"""
        if key not in self._state:
            return None
        return self._state[key]

    def lazy_get(self, key: str) -> Callable[[], Any]:
        """Returns a function to retrieve a value"""

        def _get():
            return self.get(key)

        return _get

    def lazy_get_lookup(self, table: str, key: str, value: str) -> Callable[[], Any]:
        """Returns a function to retrieve a lookup dict based on a transformation result"""

        def _get_lookup():
            res = self.get(table)
            return (
                None
                if res is None or len(res) == 0
                else dict(zip(res[key], res[value]))
            )

        return _get_lookup
