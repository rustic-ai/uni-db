# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Session management for uni-pydantic OGM."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
)
from weakref import WeakValueDictionary

from pydantic import ValidationError

from .base import UniEdge, UniNode, _resolve_type_hints
from .exceptions import (
    BulkLoadError,
    NotPersisted,
    SessionError,
    TransactionError,
)
from .fields import RelationshipDescriptor
from .hooks import (
    _AFTER_CREATE,
    _AFTER_DELETE,
    _AFTER_LOAD,
    _AFTER_UPDATE,
    _BEFORE_CREATE,
    _BEFORE_DELETE,
    _BEFORE_LOAD,
    _BEFORE_UPDATE,
    run_class_hooks,
    run_hooks,
)
from .query import (
    QueryBuilder,
    _edge_pattern,
    _row_to_node_dict,
    _validate_property,
    _warn_unhydratable,
)
from .schema import SchemaGenerator
from .types import db_to_python_value, python_to_db_value

if TYPE_CHECKING:
    from types import TracebackType

    import uni_db

NodeT = TypeVar("NodeT", bound=UniNode)
EdgeT = TypeVar("EdgeT", bound=UniEdge)

# Convenience alias: RETURN clause for a single node variable "n".
_NODE_RETURN = "properties(n) AS _props, id(n) AS _vid, labels(n) AS _labels"

# The node object a bare ``RETURN n`` yields (optional, like ``types.py``).
try:
    from uni_db import Node as _RustNode

    _DbNode: type[_RustNode] | None = _RustNode
except ImportError:  # pragma: no cover - uni_db is a hard dependency
    _DbNode = None


def _cypher_node_target(
    value: Any,
    result_type: type[NodeT],
    node_models: dict[str, type[UniNode]],
) -> tuple[dict[str, Any], type[NodeT]] | None:
    """Pick the node data and model to hydrate one ``cypher()`` column as.

    ``value`` is either a ``uni_db.Node`` (what ``RETURN n`` yields) or a dict
    carrying ``_label`` (a node-shaped map). The model is ``result_type`` when
    the node carries its label, else a registered subclass of ``result_type``
    for one of its labels. A node of an unrelated label is not a
    ``result_type`` and yields ``None`` -- it is never coerced into one.
    """
    if _DbNode is not None and isinstance(value, _DbNode):
        labels = list(value.labels)
        data = dict(value.properties)
        data["_id"] = int(value.id)
    elif isinstance(value, dict) and "_label" in value:
        labels = [value["_label"]]
        data = dict(value)
    else:
        return None

    if result_type.__label__ in labels:
        data["_label"] = result_type.__label__
        return data, result_type
    for label in labels:
        model = node_models.get(label)
        if model is not None and issubclass(model, result_type):
            data["_label"] = label
            return data, model
    return None


class UniTransaction:
    """
    Transaction context for atomic operations.

    Provides commit/rollback semantics for a group of operations.

    Example:
        >>> with session.transaction() as tx:
        ...     alice = Person(name="Alice")
        ...     tx.add(alice)
        ...     # Auto-commits on success, rolls back on exception
    """

    def __init__(self, session: UniSession) -> None:
        self._session = session
        self._tx: uni_db.Transaction | None = None
        self._pending_nodes: list[UniNode] = []
        self._pending_edges: list[tuple[UniNode, str, UniNode, UniEdge | None]] = []
        self._committed = False
        self._rolled_back = False

    def __enter__(self) -> UniTransaction:
        self._tx = self._session._db_session.tx()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.rollback()
            return
        if not self._committed and not self._rolled_back:
            self.commit()

    def add(self, entity: UniNode) -> None:
        """Add a node to be created in this transaction."""
        self._pending_nodes.append(entity)

    def create_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: UniEdge | None = None,
        **kwargs: Any,
    ) -> None:
        """Create an edge between two nodes in this transaction."""
        if not source.is_persisted:
            raise NotPersisted(source)
        if not target.is_persisted:
            raise NotPersisted(target)
        self._pending_edges.append((source, edge_type, target, properties))

    def commit(self) -> None:
        """Commit the transaction."""
        if self._committed:
            raise TransactionError("Transaction already committed")
        if self._rolled_back:
            raise TransactionError("Transaction already rolled back")

        if self._tx is None:
            raise TransactionError("Transaction not started")

        try:
            # Create pending nodes
            for node in self._pending_nodes:
                self._session._create_node_in_tx(node, self._tx)

            # Create pending edges
            for source, edge_type, target, props in self._pending_edges:
                self._session._create_edge_in_tx(
                    source, edge_type, target, props, self._tx
                )

            self._tx.commit()
            self._committed = True

            # Mark nodes as clean
            for node in self._pending_nodes:
                node._mark_clean()

        except Exception as e:
            self.rollback()
            raise TransactionError(f"Commit failed: {e}") from e

    def rollback(self) -> None:
        """Rollback the transaction."""
        if self._rolled_back:
            return
        if self._tx is not None:
            self._tx.rollback()
        self._rolled_back = True
        self._pending_nodes.clear()
        self._pending_edges.clear()


class UniSession:
    """
    Session for interacting with the graph database using Pydantic models.

    The session manages model registration, schema synchronization,
    and provides CRUD operations and query building.

    Example:
        >>> from uni_db import Uni
        >>> from uni_pydantic import UniSession
        >>>
        >>> db = Uni("./my_graph")
        >>> session = UniSession(db)
        >>> session.register(Person, Company)
        >>> session.sync_schema()
        >>>
        >>> alice = Person(name="Alice", age=30)
        >>> session.add(alice)
        >>> session.commit()
    """

    def __init__(self, db: uni_db.Uni) -> None:
        self._db = db
        self._db_session = db.session()
        self._schema_gen = SchemaGenerator()
        self._identity_map: WeakValueDictionary[tuple[str, int], UniNode] = (
            WeakValueDictionary()
        )
        self._pending_new: list[UniNode] = []
        self._pending_delete: list[UniNode] = []

    def __enter__(self) -> UniSession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the session and clear all pending state."""
        self._pending_new.clear()
        self._pending_delete.clear()

    @property
    def db(self) -> uni_db.Uni:
        """Access the underlying uni_db.Uni for low-level operations."""
        return self._db

    def locy(
        self, program: str, params: dict[str, Any] | None = None
    ) -> uni_db.LocyResult:
        """
        Evaluate a Locy program and return derived facts, stats, and warnings.

        Delegates to the underlying ``uni_db.Session.locy()``.
        """
        return self._db_session.locy(program, params)

    def register(self, *models: type[UniNode] | type[UniEdge]) -> None:
        """
        Register model classes with the session.

        Registered models can be used for schema generation and queries.

        Args:
            *models: UniNode or UniEdge subclasses to register.
        """
        self._schema_gen.register(*models)

    def sync_schema(self) -> None:
        """
        Synchronize database schema with registered models.

        Creates labels, edge types, properties, and indexes as needed.
        This is additive-only; it won't remove existing schema elements.
        """
        self._schema_gen.apply_to_database(self._db)

    def query(self, model: type[NodeT]) -> QueryBuilder[NodeT]:
        """
        Create a query builder for the given model.

        Args:
            model: The UniNode subclass to query.

        Returns:
            A QueryBuilder for constructing queries.
        """
        return QueryBuilder(self, model)

    def add(self, entity: UniNode) -> None:
        """
        Add a new entity to be persisted.

        The entity will be inserted on the next commit().
        """
        if entity.is_persisted:
            raise SessionError(f"Entity {entity!r} is already persisted")
        entity._session = self
        self._pending_new.append(entity)

    def add_all(self, entities: Sequence[UniNode]) -> None:
        """Add multiple entities to be persisted."""
        for entity in entities:
            self.add(entity)

    def delete(self, entity: UniNode) -> None:
        """Mark an entity for deletion."""
        if not entity.is_persisted:
            raise NotPersisted(entity)
        self._pending_delete.append(entity)

    def get(
        self,
        model: type[NodeT],
        vid: int | None = None,
        uid: str | None = None,
        **kwargs: Any,
    ) -> NodeT | None:
        """
        Get an entity by ID or unique properties.

        Args:
            model: The model type to retrieve.
            vid: Vertex ID to look up.
            uid: Unique ID to look up.
            **kwargs: Property equality filters.

        Returns:
            The model instance or None if not found.
        """
        # Check identity map first
        if vid is not None:
            cached = self._identity_map.get((model.__label__, vid))
            if cached is not None:
                return cached  # type: ignore[return-value]

        # Build query
        label = model.__label__
        params: dict[str, Any] = {}

        if vid is not None:
            cypher = f"MATCH (n:{label}) WHERE id(n) = $vid RETURN {_NODE_RETURN}"
            params["vid"] = vid
        elif uid is not None:
            cypher = f"MATCH (n:{label}) WHERE n._uid = $uid RETURN {_NODE_RETURN}"
            params["uid"] = uid
        elif kwargs:
            # Validate property names
            for k in kwargs:
                _validate_property(k, model)
            conditions = [f"n.{k} = ${k}" for k in kwargs]
            cypher = f"MATCH (n:{label}) WHERE {' AND '.join(conditions)} RETURN {_NODE_RETURN} LIMIT 1"
            params.update(kwargs)
        else:
            raise ValueError("Must provide vid, uid, or property filters")

        results = self._db_session.query(cypher, params)
        if not results:
            return None

        node_data = _row_to_node_dict(results[0].to_dict())
        if node_data is None:
            return None
        return self._result_to_model(node_data, model)

    def refresh(self, entity: UniNode) -> None:
        """Refresh an entity's properties from the database."""
        if not entity.is_persisted:
            raise NotPersisted(entity)

        label = entity.__class__.__label__
        cypher = f"MATCH (n:{label}) WHERE id(n) = $vid RETURN {_NODE_RETURN}"
        results = self._db_session.query(cypher, {"vid": entity._vid})

        if not results:
            raise SessionError(f"Entity with vid={entity._vid} no longer exists")

        # Update properties
        props = _row_to_node_dict(results[0].to_dict())
        if props is None:
            raise SessionError(f"Entity with vid={entity._vid} no longer exists")
        hints = _resolve_type_hints(type(entity))

        for field_name in entity.get_property_fields():
            if field_name in props:
                value = props[field_name]
                if field_name in hints:
                    value = db_to_python_value(value, hints[field_name])
                setattr(entity, field_name, value)

        entity._mark_clean()

    def commit(self) -> None:
        """
        Commit all pending changes to the database.

        This persists new entities, updates dirty entities,
        and deletes marked entities.
        """
        # Insert new entities
        for entity in self._pending_new:
            self._create_node(entity)

        # Update dirty entities in identity map
        for (label, vid), entity in list(self._identity_map.items()):
            if entity.is_dirty and entity.is_persisted:
                self._update_node(entity)

        # Delete marked entities
        for entity in self._pending_delete:
            self._delete_node(entity)

        # Flush to storage
        self._db.flush()

        # Clear pending lists
        self._pending_new.clear()
        self._pending_delete.clear()

    def rollback(self) -> None:
        """Discard all pending changes."""
        # Clear pending new — detach entities
        for entity in self._pending_new:
            entity._session = None
        self._pending_new.clear()

        # Clear pending deletes
        self._pending_delete.clear()

        # Invalidate dirty identity map entries
        for entity in list(self._identity_map.values()):
            if entity.is_dirty:
                self.refresh(entity)

    @contextmanager
    def transaction(self) -> Iterator[UniTransaction]:
        """Create a transaction context."""
        tx = UniTransaction(self)
        with tx:
            yield tx

    def begin(self) -> UniTransaction:
        """Begin a new transaction."""
        tx = UniTransaction(self)
        tx._tx = self._db_session.tx()
        return tx

    def cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        result_type: type[NodeT] | None = None,
    ) -> list[NodeT] | list[dict[str, Any]]:
        """
        Execute a raw Cypher query.

        Args:
            query: Cypher query string.
            params: Query parameters.
            result_type: Optional model type for result mapping.

        Returns:
            List of results (model instances if result_type provided).
        """
        results = self._db_session.query(query, params)

        if result_type is None:
            return [r.to_dict() for r in results]

        # Map results to model instances
        mapped: list[NodeT] = []
        node_models = self._schema_gen._node_models
        for raw_row in results:
            row = raw_row.to_dict()
            # The first column holding a node of `result_type` (or a registered
            # subclass) is the row's entity.
            for value in row.values():
                target = _cypher_node_target(value, result_type, node_models)
                if target is None:
                    continue
                instance = self._result_to_model(*target)
                if instance is not None:
                    mapped.append(instance)
                    break
            else:
                # No node column: an unlabelled map in the first column is a
                # projection of `result_type`'s properties.
                first_value = next(iter(row.values()), None)
                if isinstance(first_value, dict) and "_label" not in first_value:
                    instance = self._result_to_model(first_value, result_type)
                    if instance is not None:
                        mapped.append(instance)

        return mapped

    @staticmethod
    def _validate_edge_endpoints(
        source: UniNode, target: UniNode
    ) -> tuple[int, int, str, str]:
        """Validate that both endpoints are persisted and return (src_vid, dst_vid, src_label, dst_label)."""
        # `is_persisted` is `_vid is not None`; test the ids themselves so the
        # returned vids are known to be ints.
        src_vid = source._vid
        if src_vid is None:
            raise NotPersisted(source)
        dst_vid = target._vid
        if dst_vid is None:
            raise NotPersisted(target)
        return (
            src_vid,
            dst_vid,
            source.__class__.__label__,
            target.__class__.__label__,
        )

    @staticmethod
    def _normalize_edge_properties(
        properties: dict[str, Any] | UniEdge | None,
    ) -> dict[str, Any]:
        """Normalize edge properties from dict, UniEdge, or None."""
        if isinstance(properties, UniEdge):
            return properties.to_properties()
        if properties:
            return properties
        return {}

    def create_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: dict[str, Any] | UniEdge | None = None,
    ) -> None:
        """Create an edge between two nodes."""
        src_vid, dst_vid, src_label, dst_label = self._validate_edge_endpoints(
            source, target
        )
        props = self._normalize_edge_properties(properties)

        # Build CREATE edge query with labels (required by Cypher implementation)
        props_str = ", ".join(f"{k}: ${k}" for k in props)
        if props_str:
            cypher = f"MATCH (a:{src_label}), (b:{dst_label}) WHERE a._vid = $src AND b._vid = $dst CREATE (a)-[r:{edge_type} {{{props_str}}}]->(b)"
        else:
            cypher = f"MATCH (a:{src_label}), (b:{dst_label}) WHERE a._vid = $src AND b._vid = $dst CREATE (a)-[r:{edge_type}]->(b)"

        params = {"src": src_vid, "dst": dst_vid, **props}
        with self._db_session.tx() as tx:
            tx.execute(cypher, params)
            tx.commit()

    def delete_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
    ) -> int:
        """Delete edges between two nodes. Returns the number of deleted edges."""
        src_vid, dst_vid, src_label, dst_label = self._validate_edge_endpoints(
            source, target
        )
        cypher = (
            f"MATCH (a:{src_label})-[r:{edge_type}]->(b:{dst_label}) "
            f"WHERE a._vid = $src AND b._vid = $dst "
            f"DELETE r RETURN count(r) as count"
        )
        with self._db_session.tx() as tx:
            results = tx.query(cypher, {"src": src_vid, "dst": dst_vid})
            tx.commit()
        return cast(int, results[0]["count"]) if results else 0

    def update_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: dict[str, Any],
    ) -> int:
        """Update properties on edges between two nodes. Returns the number of updated edges."""
        src_vid, dst_vid, src_label, dst_label = self._validate_edge_endpoints(
            source, target
        )
        set_parts = [f"r.{k} = ${k}" for k in properties]
        params: dict[str, Any] = {"src": src_vid, "dst": dst_vid, **properties}
        cypher = (
            f"MATCH (a:{src_label})-[r:{edge_type}]->(b:{dst_label}) "
            f"WHERE a._vid = $src AND b._vid = $dst "
            f"SET {', '.join(set_parts)} "
            f"RETURN count(r) as count"
        )
        with self._db_session.tx() as tx:
            results = tx.query(cypher, params)
            tx.commit()
        return cast(int, results[0]["count"]) if results else 0

    def get_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        edge_model: type[EdgeT] | None = None,
    ) -> list[dict[str, Any]] | list[EdgeT]:
        """Get edges between two nodes. Returns dicts or edge model instances."""
        src_vid, dst_vid, src_label, dst_label = self._validate_edge_endpoints(
            source, target
        )
        cypher = (
            f"MATCH (a:{src_label})-[r:{edge_type}]->(b:{dst_label}) "
            f"WHERE a._vid = $src AND b._vid = $dst "
            f"RETURN properties(r) AS _props, id(r) AS _eid"
        )
        results = self._db_session.query(cypher, {"src": src_vid, "dst": dst_vid})
        rows = [r.to_dict() for r in results]

        if edge_model is None:
            edge_dicts: list[dict[str, Any]] = []
            for row in rows:
                props = row.get("_props", {})
                if isinstance(props, dict):
                    edge_dict = dict(props)
                    edge_dict["_eid"] = row.get("_eid")
                    edge_dicts.append(edge_dict)
            return edge_dicts

        edges = []
        for row in rows:
            r_data = row.get("_props", {})
            if isinstance(r_data, dict):
                edge = edge_model.from_properties(
                    r_data,
                    src_vid=src_vid,
                    dst_vid=dst_vid,
                    session=self,
                )
                edges.append(edge)
        return edges

    def bulk_add(self, entities: Sequence[UniNode]) -> list[int]:
        """
        Bulk-add entities using bulk_writer for performance.

        Every label goes into one bulk load with one commit, so the whole call
        is applied or not applied as a unit. Returns VIDs and attaches sessions.

        Args:
            entities: Sequence of UniNode instances to bulk-insert.

        Returns:
            List of assigned vertex IDs, in label-group order.

        Raises:
            BulkLoadError: The load failed before its commit point; nothing was
                applied and retrying is safe.
            uni_db.UniBulkCommitIncompleteError: The load is committed and the
                next open completes it. Do not retry.
            uni_db.UniBulkCommitOutcomeUnknownError: Whether the load committed
                is unknown; check ``bulk_load_applied(e.load_id)`` after
                reopening before retrying.
        """
        if not entities:
            return []

        # Group by label
        by_label: dict[str, list[UniNode]] = {}
        for entity in entities:
            label = entity.__class__.__label__
            if label not in by_label:
                by_label[label] = []
            by_label[label].append(entity)

        import uni_db

        assigned: list[tuple[str, list[UniNode], list[int]]] = []
        try:
            tx = self._db_session.tx()
            with tx.bulk_writer().build() as bw:
                for label, group in by_label.items():
                    for entity in group:
                        run_hooks(entity, _BEFORE_CREATE)
                    prop_dicts = [e.to_properties() for e in group]
                    assigned.append(
                        (label, group, bw.insert_vertices(label, prop_dicts))
                    )
                bw.commit()
            tx.commit()
        except (
            uni_db.UniBulkCommitIncompleteError,
            uni_db.UniBulkCommitOutcomeUnknownError,
        ):
            # Committed, or possibly committed: a BulkLoadError would invite a
            # retry that loads the entities twice.
            raise
        except Exception as e:
            raise BulkLoadError(f"Bulk insert failed: {e}") from e

        all_vids: list[int] = []
        for label, group, vids in assigned:
            for entity, vid in zip(group, vids):
                entity._attach_session(self, vid)
                self._identity_map[(label, vid)] = entity
                run_hooks(entity, _AFTER_CREATE)
                entity._mark_clean()
            all_vids.extend(vids)
        return all_vids

    def explain(self, cypher: str) -> uni_db.ExplainOutput:
        """Get the query execution plan without running it."""
        # `explain`/`profile` live on the builder `query_with()` returns, not on
        # Session itself -- calling them on the session raised AttributeError.
        return self._db_session.query_with(cypher).explain()

    def profile(self, cypher: str) -> tuple[uni_db.QueryResult, uni_db.ProfileOutput]:
        """Run the query with profiling and return results + stats."""
        return self._db_session.query_with(cypher).profile()

    def save_schema(self, path: str) -> None:
        """Save the database schema to a file."""
        self._db.save_schema(path)

    def load_schema(self, path: str) -> None:
        """Load a database schema from a file."""
        self._db.load_schema(path)

    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------

    def _create_node(self, entity: UniNode) -> None:
        """Create a node in the database."""
        # Run before_create hooks
        run_hooks(entity, _BEFORE_CREATE)

        label = entity.__class__.__label__
        props = entity.to_properties()

        # Build CREATE query
        props_str = ", ".join(f"{k}: ${k}" for k in props)
        cypher = f"CREATE (n:{label} {{{props_str}}}) RETURN id(n) as vid"

        with self._db_session.tx() as tx:
            results = tx.query(cypher, props)
            tx.commit()
        if results:
            vid = results[0]["vid"]
            entity._attach_session(self, vid)

            # Add to identity map
            self._identity_map[(label, vid)] = entity

        # Run after_create hooks
        run_hooks(entity, _AFTER_CREATE)
        entity._mark_clean()

    def _create_node_in_tx(self, entity: UniNode, tx: uni_db.Transaction) -> None:
        """Create a node within a transaction."""
        run_hooks(entity, _BEFORE_CREATE)

        label = entity.__class__.__label__
        props = entity.to_properties()

        props_str = ", ".join(f"{k}: ${k}" for k in props)
        cypher = f"CREATE (n:{label} {{{props_str}}}) RETURN id(n) as vid"

        results = tx.query(cypher, props)
        if results:
            vid = results[0]["vid"]
            entity._attach_session(self, vid)
            self._identity_map[(label, vid)] = entity

        run_hooks(entity, _AFTER_CREATE)

    def _create_edge_in_tx(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: UniEdge | None,
        tx: uni_db.Transaction,
    ) -> None:
        """Create an edge within a transaction."""
        props = properties.to_properties() if properties else {}
        src_label = source.__class__.__label__
        dst_label = target.__class__.__label__

        props_str = ", ".join(f"{k}: ${k}" for k in props)
        if props_str:
            cypher = f"MATCH (a:{src_label}), (b:{dst_label}) WHERE a._vid = $src AND b._vid = $dst CREATE (a)-[:{edge_type} {{{props_str}}}]->(b)"
        else:
            cypher = f"MATCH (a:{src_label}), (b:{dst_label}) WHERE a._vid = $src AND b._vid = $dst CREATE (a)-[:{edge_type}]->(b)"

        params = {"src": source._vid, "dst": target._vid, **props}
        tx.query(cypher, params)

    def _update_node(self, entity: UniNode) -> None:
        """Update a node in the database."""
        run_hooks(entity, _BEFORE_UPDATE)

        label = entity.__class__.__label__

        # Convert dirty prop values via python_to_db_value
        hints = _resolve_type_hints(type(entity))

        dirty_props = {}
        for name in entity._dirty:
            value = getattr(entity, name)
            if name in hints:
                value = python_to_db_value(value, hints[name])
            dirty_props[name] = value

        if not dirty_props:
            return

        set_clause = ", ".join(f"n.{k} = ${k}" for k in dirty_props)
        cypher = f"MATCH (n:{label}) WHERE id(n) = $vid SET {set_clause}"
        params = {"vid": entity._vid, **dirty_props}

        with self._db_session.tx() as tx:
            tx.execute(cypher, params)
            tx.commit()

        run_hooks(entity, _AFTER_UPDATE)
        entity._mark_clean()

    def _delete_node(self, entity: UniNode) -> None:
        """Delete a node from the database."""
        run_hooks(entity, _BEFORE_DELETE)

        label = entity.__class__.__label__
        vid = entity._vid

        # DETACH DELETE to also remove connected edges
        cypher = f"MATCH (n:{label}) WHERE id(n) = $vid DETACH DELETE n"
        with self._db_session.tx() as tx:
            tx.execute(cypher, {"vid": vid})
            tx.commit()

        # Remove from identity map
        if vid is not None and (label, vid) in self._identity_map:
            del self._identity_map[(label, vid)]

        # Clear entity IDs
        entity._vid = None
        entity._uid = None
        entity._session = None

        run_hooks(entity, _AFTER_DELETE)

    def _result_to_model(
        self,
        data: dict[str, Any],
        model: type[NodeT],
    ) -> NodeT | None:
        """Convert a query result row to a model instance.

        Does not mutate the input dict.
        """
        if not data:
            return None

        # Work on a copy
        data = dict(data)

        # Run before_load hooks
        data = run_class_hooks(model, _BEFORE_LOAD, data) or data

        # Extract _id → vid (uni-db returns _id as string or int)
        vid = data.pop("_id", None)
        if vid is None:
            vid = data.pop("_vid", None)
        if vid is None:
            vid = data.pop("vid", None)
        if vid is not None and not isinstance(vid, int):
            vid = int(vid)

        # Remove _label (informational)
        data.pop("_label", None)

        try:
            instance = model.from_properties(data, vid=vid, session=self)
        except ValidationError as exc:
            _warn_unhydratable(model, vid, exc)
            return None

        # Add to identity map if we have a vid
        if vid is not None:
            existing = self._identity_map.get((model.__label__, vid))
            if existing is not None:
                return cast(NodeT, existing)
            self._identity_map[(model.__label__, vid)] = instance

        # Run after_load hooks
        run_hooks(instance, _AFTER_LOAD)

        return instance

    def _load_relationship(
        self,
        entity: UniNode,
        descriptor: RelationshipDescriptor[Any],
    ) -> list[UniNode] | UniNode | None:
        """Load a relationship for an entity."""
        if not entity.is_persisted:
            raise NotPersisted(entity)

        config = descriptor.config
        label = entity.__class__.__label__
        pattern = _edge_pattern(config.edge_type, config.direction)

        cypher = (
            f"MATCH (a:{label}){pattern}(b) WHERE id(a) = $vid "
            f"RETURN properties(b) AS _props, id(b) AS _vid, labels(b) AS _labels"
        )
        results = self._db_session.query(cypher, {"vid": entity._vid})

        nodes = []
        for raw_row in results:
            row = raw_row.to_dict()
            node_data = _row_to_node_dict(row)
            if node_data is None:
                continue
            # Try to find the model for this node
            node_label = node_data.get("_label")
            if node_label and node_label in self._schema_gen._node_models:
                model = self._schema_gen._node_models[node_label]
                instance = self._result_to_model(node_data, model)
                if instance is not None:
                    nodes.append(instance)

        if not descriptor.is_list:
            return nodes[0] if nodes else None
        return nodes

    def _eager_load_relationships(
        self,
        entities: list[NodeT],
        relationships: list[str],
    ) -> None:
        """Eager load relationships for a list of entities."""
        if not entities:
            return

        model = type(entities[0])
        rel_configs = model.get_relationship_fields()

        for rel_name in relationships:
            if rel_name not in rel_configs:
                continue

            config = rel_configs[rel_name]
            label = model.__label__
            vids = [e._vid for e in entities if e._vid is not None]

            if not vids:
                continue

            pattern = _edge_pattern(config.edge_type, config.direction)
            cypher = (
                f"MATCH (a:{label}){pattern}(b) WHERE id(a) IN $vids "
                f"RETURN id(a) as src_vid, properties(b) AS _props, id(b) AS _vid, labels(b) AS _labels"
            )
            results = self._db_session.query(cypher, {"vids": vids})

            # Hydrate, exactly as the lazy path does.
            #
            # These rows used to be cached as raw result dicts.
            # `RelationshipDescriptor.__get__` returns the cache verbatim, so
            # eager loading handed back `list[dict]` where lazy loading hands
            # back `list[Model]` -- `user.posts[0].title` raised where the same
            # access worked without `.eager_load()`.
            descriptor = getattr(model, rel_name, None)
            is_list = getattr(descriptor, "is_list", True)

            by_source: dict[int, list[Any]] = {}
            for raw_row in results:
                row = raw_row.to_dict()
                src_vid = row["src_vid"]
                node_data = _row_to_node_dict(row)
                if node_data is None:
                    continue
                node_label = node_data.get("_label")
                if not node_label or node_label not in self._schema_gen._node_models:
                    continue
                target_model = self._schema_gen._node_models[node_label]
                instance = self._result_to_model(node_data, target_model)
                if instance is None:
                    continue
                by_source.setdefault(src_vid, []).append(instance)

            # Every entity gets a cache entry, including the ones with nothing
            # attached. Leaving those unset sends the descriptor down the lazy
            # path on first access -- which on an async session raises, telling
            # the caller to use the `eager_load()` they already used.
            cache_attr = f"_rel_cache_{rel_name}"
            for entity in entities:
                related = (
                    by_source.get(entity._vid, []) if entity._vid is not None else []
                )
                if is_list:
                    setattr(entity, cache_attr, related)
                else:
                    setattr(entity, cache_attr, related[0] if related else None)
