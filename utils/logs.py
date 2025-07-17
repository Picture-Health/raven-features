import logging
import os
import re


# verbatim copy from Raven to get the same behaviour
LOGGING_FLAG = True
PXP_LOG_LEVEL = "INFO"
_LOG_LEVEL = os.getenv(PXP_LOG_LEVEL, "INFO") # Defaults to INFO if not set


class SensitiveDataFilter(logging.Filter):
    """Filter to sanitize sensitive data in log messages."""

    def filter(self, record):
        # Combine patterns into one regex with named capturing groups:
        # This pattern matches keys like password, api_key, token, or username
        # along with an optional quote, and the sensitive value.
        sensitive_pattern = re.compile(
            r'(?P<key>password|api_key|token|username|user)\s*=\s*(?P<quote>["\']?)(?P<value>\S+)(?P=quote)',
            flags=re.IGNORECASE,
        )

        # Replace the entire match with "key=[REDACTED]"
        record.msg = sensitive_pattern.sub(lambda m: f"{m.group('key')}=[REDACTED]", record.msg)
        return True


def get_logger(name):
    """This configuration applies only to the loggers created by calling get_logger."""
    if LOGGING_FLAG:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s", datefmt="%Y-%m-%d %I:%M:%S %p")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        #handler.addFilter(SensitiveDataFilter())
        logger = logging.getLogger(name)
        logger.setLevel(_LOG_LEVEL)
        logger.addHandler(handler)
        return logger


def disable_logging():
    """Option to disable logging for compatibility issues with MLExpress @Amogh"""
    global LOGGING_FLAG
    LOGGING_FLAG = False


def enable_logging():
    global LOGGING_FLAG
    LOGGING_FLAG = True