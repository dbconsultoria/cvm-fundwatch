"""Errors shared across layers."""


class SchemaError(ValueError):
    """A source file does not have the expected structure. Fails the run."""
