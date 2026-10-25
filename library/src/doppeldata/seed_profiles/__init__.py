"""Seed profiles imported from a schema a project already has, so it generates data before any exists to profile.

Each importer reads its schema into declared columns, which expand as a short YAML's columns do. A seed records
`generated.engine: import` and the schema in `generated.imported_from`.
"""
