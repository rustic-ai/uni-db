# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Schema generation from Pydantic models to Uni database schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, get_type_hints

from .base import UniEdge, UniNode
from .exceptions import SchemaError
from .fields import get_field_config
from .types import get_vector_dimensions, is_optional, python_type_to_uni

if TYPE_CHECKING:
    import uni_db


@dataclass
class PropertySchema:
    """Schema for a single property."""

    name: str
    data_type: str
    nullable: bool = False
    index_type: str | None = None
    unique: bool = False
    tokenizer: str | None = None
    metric: str | None = None


@dataclass
class LabelSchema:
    """Schema for a vertex label."""

    name: str
    properties: dict[str, PropertySchema] = field(default_factory=dict)


@dataclass
class EdgeTypeSchema:
    """Schema for an edge type."""

    name: str
    from_labels: list[str] = field(default_factory=list)
    to_labels: list[str] = field(default_factory=list)
    properties: dict[str, PropertySchema] = field(default_factory=dict)


@dataclass
class DatabaseSchema:
    """Complete database schema generated from models."""

    labels: dict[str, LabelSchema] = field(default_factory=dict)
    edge_types: dict[str, EdgeTypeSchema] = field(default_factory=dict)


class SchemaGenerator:
    """Generates Uni database schema from registered models."""

    def __init__(self) -> None:
        self._node_models: dict[str, type[UniNode]] = {}
        self._edge_models: dict[str, type[UniEdge]] = {}
        self._schema: DatabaseSchema | None = None

    def register_node(self, model: type[UniNode]) -> None:
        """Register a node model for schema generation."""
        label = model.__label__
        if not label:
            raise SchemaError(f"Model {model.__name__} has no __label__", model)
        self._node_models[label] = model
        self._schema = None  # Invalidate cached schema

    def register_edge(self, model: type[UniEdge]) -> None:
        """Register an edge model for schema generation."""
        edge_type = model.__edge_type__
        if not edge_type:
            raise SchemaError(f"Model {model.__name__} has no __edge_type__", model)
        self._edge_models[edge_type] = model
        self._schema = None

    def register(self, *models: type[UniNode] | type[UniEdge]) -> None:
        """Register multiple models."""
        for model in models:
            if issubclass(model, UniEdge):
                self.register_edge(model)
            elif issubclass(model, UniNode):
                self.register_node(model)
            else:
                raise SchemaError(
                    f"Model {model.__name__} must be a subclass of UniNode or UniEdge"
                )

    def _generate_property_schema(
        self,
        model: type[UniNode] | type[UniEdge],
        field_name: str,
    ) -> PropertySchema:
        """Generate schema for a single property field."""
        field_info = model.model_fields[field_name]

        # Get type hints with forward refs resolved
        try:
            hints = get_type_hints(model)
            type_hint = hints.get(field_name, field_info.annotation)
        except Exception:
            type_hint = field_info.annotation

        # Check for nullability
        is_nullable, inner_type = is_optional(type_hint)

        # Get Uni data type
        data_type, nullable = python_type_to_uni(type_hint, nullable=is_nullable)

        # Check for vector dimensions
        vec_dims = get_vector_dimensions(inner_type if is_nullable else type_hint)
        if vec_dims:
            data_type = f"vector:{vec_dims}"

        # Get field config for index settings
        config = get_field_config(field_info)
        index_type = config.index if config else None
        unique = config.unique if config else False
        tokenizer = config.tokenizer if config else None
        metric = config.metric if config else None

        # Auto-create vector index for Vector fields (regardless of Field config)
        if vec_dims and not index_type:
            index_type = "vector"

        return PropertySchema(
            name=field_name,
            data_type=data_type,
            nullable=nullable,
            index_type=index_type,
            unique=unique,
            tokenizer=tokenizer,
            metric=metric,
        )

    def _generate_label_schema(self, model: type[UniNode]) -> LabelSchema:
        """Generate schema for a node model."""
        label = model.__label__

        properties = {}
        for field_name in model.get_property_fields():
            prop_schema = self._generate_property_schema(model, field_name)
            properties[field_name] = prop_schema

        return LabelSchema(
            name=label,
            properties=properties,
        )

    def _generate_edge_type_schema(self, model: type[UniEdge]) -> EdgeTypeSchema:
        """Generate schema for an edge model."""
        edge_type = model.__edge_type__
        from_labels = model.get_from_labels()
        to_labels = model.get_to_labels()

        # If from/to not specified, allow any labels
        if not from_labels:
            from_labels = list(self._node_models.keys())
        if not to_labels:
            to_labels = list(self._node_models.keys())

        properties = {}
        for field_name in model.get_property_fields():
            prop_schema = self._generate_property_schema(model, field_name)
            properties[field_name] = prop_schema

        return EdgeTypeSchema(
            name=edge_type,
            from_labels=from_labels,
            to_labels=to_labels,
            properties=properties,
        )

    def generate(self) -> DatabaseSchema:
        """Generate the complete database schema."""
        if self._schema is not None:
            return self._schema

        schema = DatabaseSchema()

        # Generate label schemas
        for label, model in self._node_models.items():
            schema.labels[label] = self._generate_label_schema(model)

        # Generate edge type schemas
        for edge_type_name, edge_model in self._edge_models.items():
            schema.edge_types[edge_type_name] = self._generate_edge_type_schema(
                edge_model
            )

        # Also generate labels from relationships in node models
        for model in self._node_models.values():
            for rel_name, rel_config in model.get_relationship_fields().items():
                edge_type = rel_config.edge_type
                if edge_type not in schema.edge_types:
                    # Create a minimal edge type schema
                    schema.edge_types[edge_type] = EdgeTypeSchema(
                        name=edge_type,
                        from_labels=list(self._node_models.keys()),
                        to_labels=list(self._node_models.keys()),
                    )

        self._schema = schema
        return schema

    def apply_to_database(self, db: uni_db.Database) -> None:
        """Apply the generated schema to a database using SchemaBuilder.

        Uses db.schema() for atomic schema application with additive-only
        semantics. Creates labels, edge types, properties, and indexes.
        """
        schema = self.generate()

        # Build the full schema using SchemaBuilder, skipping existing labels/edge types
        builder = db.schema()
        has_changes = False

        for label, label_schema in schema.labels.items():
            if db.label_exists(label):
                continue  # Additive-only: skip existing labels
            lb = builder.label(label)
            for prop in label_schema.properties.values():
                # Check for vector type
                if prop.data_type.startswith("vector:"):
                    dims = int(prop.data_type.split(":")[1])
                    lb = lb.vector(prop.name, dims)
                elif prop.nullable:
                    lb = lb.property_nullable(prop.name, prop.data_type)
                else:
                    lb = lb.property(prop.name, prop.data_type)

                # Add indexes (not vector — vector is handled by .vector())
                if prop.index_type and prop.index_type in ("btree", "hash"):
                    lb = lb.index(prop.name, prop.index_type)
            builder = lb.done()
            has_changes = True

        for edge_type, edge_schema in schema.edge_types.items():
            if db.edge_type_exists(edge_type):
                continue  # Skip existing edge types
            eb = builder.edge_type(
                edge_type, edge_schema.from_labels, edge_schema.to_labels
            )
            for prop in edge_schema.properties.values():
                if prop.nullable:
                    eb = eb.property_nullable(prop.name, prop.data_type)
                else:
                    eb = eb.property(prop.name, prop.data_type)
            builder = eb.done()
            has_changes = True

        if has_changes:
            builder.apply()

        # Create vector indexes separately (SchemaBuilder may not handle them)
        for label, label_schema in schema.labels.items():
            for prop in label_schema.properties.values():
                if prop.index_type == "vector":
                    metric = prop.metric or "l2"
                    try:
                        db.create_vector_index(label, prop.name, metric)
                    except Exception:
                        pass  # Index may already exist

        # Create fulltext indexes separately
        for label, label_schema in schema.labels.items():
            for prop in label_schema.properties.values():
                if prop.index_type == "fulltext":
                    try:
                        db.create_scalar_index(label, prop.name, "fulltext")
                    except Exception:
                        pass  # Index may already exist or not supported

    async def async_apply_to_database(self, db: uni_db.AsyncDatabase) -> None:
        """Apply the generated schema to an async database.

        Async variant of apply_to_database using AsyncSchemaBuilder.
        """
        schema = self.generate()

        # Build the full schema using AsyncSchemaBuilder, skipping existing labels/edge types
        builder = db.schema()
        has_changes = False

        for label, label_schema in schema.labels.items():
            if await db.label_exists(label):
                continue
            lb = builder.label(label)
            for prop in label_schema.properties.values():
                if prop.data_type.startswith("vector:"):
                    dims = int(prop.data_type.split(":")[1])
                    lb = lb.vector(prop.name, dims)
                elif prop.nullable:
                    lb = lb.property_nullable(prop.name, prop.data_type)
                else:
                    lb = lb.property(prop.name, prop.data_type)

                if prop.index_type and prop.index_type in ("btree", "hash"):
                    lb = lb.index(prop.name, prop.index_type)
            builder = lb.done()
            has_changes = True

        for edge_type, edge_schema in schema.edge_types.items():
            if await db.edge_type_exists(edge_type):
                continue
            eb = builder.edge_type(
                edge_type, edge_schema.from_labels, edge_schema.to_labels
            )
            for prop in edge_schema.properties.values():
                if prop.nullable:
                    eb = eb.property_nullable(prop.name, prop.data_type)
                else:
                    eb = eb.property(prop.name, prop.data_type)
            builder = eb.done()
            has_changes = True

        if has_changes:
            await builder.apply()

        # Create vector indexes separately
        for label, label_schema in schema.labels.items():
            for prop in label_schema.properties.values():
                if prop.index_type == "vector":
                    metric = prop.metric or "l2"
                    try:
                        await db.create_vector_index(label, prop.name, metric)
                    except Exception:
                        pass

        # Create fulltext indexes separately
        for label, label_schema in schema.labels.items():
            for prop in label_schema.properties.values():
                if prop.index_type == "fulltext":
                    try:
                        await db.create_scalar_index(label, prop.name, "fulltext")
                    except Exception:
                        pass


def generate_schema(*models: type[UniNode] | type[UniEdge]) -> DatabaseSchema:
    """Generate a database schema from the given models."""
    generator = SchemaGenerator()
    generator.register(*models)
    return generator.generate()
