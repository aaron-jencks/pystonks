from typing import Callable, Any

DARK_MODE_COLOR = '#565956'
"""
The dark mode color definition for GUI programs
"""

BUTTON_HANDLER = Callable[[], None]
"""
A function that can handle button presses
"""

EVENT_HANDLER = Callable[[Any], None]
"""
A function that can handle generic events
"""
