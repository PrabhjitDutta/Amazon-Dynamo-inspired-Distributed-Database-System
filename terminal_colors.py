class TermColors:
    """A simple class to colorize terminal output using ANSI escape codes."""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    
    # Regular Colors
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    
    # Bright/Light Colors
    LIGHT_RED = '\033[91m'
    LIGHT_GREEN = '\033[92m'
    LIGHT_YELLOW = '\033[93m'
    LIGHT_BLUE = '\033[94m'
    LIGHT_MAGENTA = '\033[95m'
    LIGHT_CYAN = '\033[96m'
    GRAY = '\033[90m'

    @staticmethod
    def colorize(text, color, bold=False, dim=False):
        """Applies color and style to a string."""
        style = ''
        if bold: style += TermColors.BOLD
        if dim: style += TermColors.DIM
        return f"{style}{color}{text}{TermColors.RESET}"

# Create a global instance for easy importing and use
TC = TermColors()