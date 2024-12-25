"""All subclasses of Exception of the package"""

from __future__ import annotations


class DocumentNotFoundError(Exception):
    """One or more documents not found in MongoDB"""
