# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async session management for uni-pydantic OGM."""

from __future__ import annotations

from collections.abc import Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
    get_type_hints,
)
from weakref import WeakValueDictionary

from .async_query import AsyncQueryBuilder
from .base import UniEdge, UniNode
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
from .query import _edge_pattern, _row_to_node_dict, _validate_property
from .schema import SchemaGenerator
from .session import _NODE_RETURN, UniSession
from .types import db_to_python_value, python_to_db_value

if TYPE_CHECKING:
    from types import TracebackType

    import uni_db

NodeT = TypeVar("NodeT", bound=UniNode)
EdgeT = TypeVar("EdgeT", bound=UniEdge)


class AsyncUniTransaction:
    """Async transaction context for atomic operations."""

    def __init__(self, session: AsyncUniSession) -> None:
        self._session = session
        self._tx: uni_db.AsyncTransaction | None = None
        self._pending_nodes: list[UniNode] = []
        self._pending_edges: list[tuple[UniNode, str, UniNode, UniEdge | None]] = []
        self._committed = False
        self._rolled_back = False

    async def __aenter__(self) -> AsyncUniTransaction:
        self._tx = await self._session._db.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            await self.rollback()
            return
        if not self._committed and not self._rolled_back:
            await self.commit()

    def add(self, entity: UniNode) -> None:
        """Add a node to be created in this transaction (sync — just collects)."""
        self._pending_nodes.append(entity)

    def create_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: UniEdge | None = None,
    ) -> None:
        """Create an edge between two nodes in this transaction (sync — just collects)."""
        if not source.is_persisted:
            raise NotPersisted(source)
        if not target.is_persisted:
            raise NotPersisted(target)
        self._pending_edges.append((source, edge_type, target, properties))

    async def commit(self) -> None:
        """Commit the transaction."""
        if self._committed:
            raise TransactionError("Transaction already committed")
        if self._rolled_back:
            raise TransactionError("Transaction already rolled back")
        if self._tx is None:
            raise TransactionError("Transaction not started")

        try:
            for node in self._pending_nodes:
                await self._session._create_node_in_tx(node, self._tx)
            for source, edge_type, target, props in self._pending_edges:
                await self._session._create_edge_in_tx(
                    source, edge_type, target, props, self._tx
                )
            await self._tx.commit()
            self._committed = True
            for node in self._pending_nodes:
                node._mark_clean()
        except Exception as e:
            await self.rollback()
            raise TransactionError(f"Commit failed: {e}") from e

    async def rollback(self) -> None:
        """Rollback the transaction."""
        if self._rolled_back:
            return
        if self._tx is not None:
            await self._tx.rollback()
        self._rolled_back = True
        self._pending_nodes.clear()
        self._pending_edges.clear()


class AsyncUniSession:
    """
    Async session for interacting with the graph database.

    Mirrors UniSession with async methods. Uses AsyncDatabase.

    Example:
        >>> from uni_db import AsyncDatabase
        >>> from uni_pydantic import AsyncUniSession
        >>>
        >>> db = await AsyncDatabase.open("./my_graph")
        >>> async with AsyncUniSession(db) as session:
        ...     session.register(Person)
        ...     await session.sync_schema()
        ...     alice = Person(name="Alice", age=30)
        ...     session.add(alice)
        ...     await session.commit()
    """

    def __init__(self, db: uni_db.AsyncDatabase) -> None:
        self._db = db
        self._schema_gen = SchemaGenerator()
        self._identity_map: WeakValueDictionary[tuple[str, int], UniNode] = (
            WeakValueDictionary()
        )
        self._pending_new: list[UniNode] = []
        self._pending_delete: list[UniNode] = []

    async def __aenter__(self) -> AsyncUniSession:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the session and clear pending state."""
        self._pending_new.clear()
        self._pending_delete.clear()

    def register(self, *models: type[UniNode] | type[UniEdge]) -> None:
        """Register model classes with the session (sync)."""
        self._schema_gen.register(*models)

    async def sync_schema(self) -> None:
        """Synchronize database schema with registered models."""
        await self._schema_gen.async_apply_to_database(self._db)

    def query(self, model: type[NodeT]) -> AsyncQueryBuilder[NodeT]:
        """Create an async query builder for the given model."""
        return AsyncQueryBuilder(self, model)

    def add(self, entity: UniNode) -> None:
        """Add a new entity to be persisted (sync — just collects)."""
        if entity.is_persisted:
            raise SessionError(f"Entity {entity!r} is already persisted")
        entity._session = self
        self._pending_new.append(entity)

    def add_all(self, entities: Sequence[UniNode]) -> None:
        """Add multiple entities (sync — just collects)."""
        for entity in entities:
            self.add(entity)

    def delete(self, entity: UniNode) -> None:
        """Mark an entity for deletion (sync — just collects)."""
        if not entity.is_persisted:
            raise NotPersisted(entity)
        self._pending_delete.append(entity)

    async def get(
        self,
        model: type[NodeT],
        vid: int | None = None,
        uid: str | None = None,
        **kwargs: Any,
    ) -> NodeT | None:
        """Get an entity by ID or unique properties."""
        if vid is not None:
            cached = self._identity_map.get((model.__label__, vid))
            if cached is not None:
                return cached  # type: ignore[return-value]

        label = model.__label__
        params: dict[str, Any] = {}

        if vid is not None:
            cypher = f"MATCH (n:{label}) WHERE id(n) = $vid RETURN {_NODE_RETURN}"
            params["vid"] = vid
        elif uid is not None:
            cypher = f"MATCH (n:{label}) WHERE n._uid = $uid RETURN {_NODE_RETURN}"
            params["uid"] = uid
        elif kwargs:
            for k in kwargs:
                _validate_property(k, model)
            conditions = [f"n.{k} = ${k}" for k in kwargs]
            cypher = f"MATCH (n:{label}) WHERE {' AND '.join(conditions)} RETURN {_NODE_RETURN} LIMIT 1"
            params.update(kwargs)
        else:
            raise ValueError("Must provide vid, uid, or property filters")

        results = await self._db.query(cypher, params)
        if not results:
            return None

        node_data = _row_to_node_dict(results[0])
        if node_data is None:
            return None
        return self._result_to_model(node_data, model)

    async def refresh(self, entity: UniNode) -> None:
        """Refresh an entity's properties from the database."""
        if not entity.is_persisted:
            raise NotPersisted(entity)

        label = entity.__class__.__label__
        cypher = f"MATCH (n:{label}) WHERE id(n) = $vid RETURN {_NODE_RETURN}"
        results = await self._db.query(cypher, {"vid": entity._vid})

        if not results:
            raise SessionError(f"Entity with vid={entity._vid} no longer exists")

        props = _row_to_node_dict(results[0])
        if props is None:
            raise SessionError(f"Entity with vid={entity._vid} no longer exists")
        try:
            hints = get_type_hints(type(entity))
        except Exception:
            hints = {}

        for field_name in entity.get_property_fields():
            if field_name in props:
                value = props[field_name]
                if field_name in hints:
                    value = db_to_python_value(value, hints[field_name])
                setattr(entity, field_name, value)

        entity._mark_clean()

    async def commit(self) -> None:
        """Commit all pending changes."""
        for entity in self._pending_new:
            await self._create_node(entity)

        for (label, vid), entity in list(self._identity_map.items()):
            if entity.is_dirty and entity.is_persisted:
                await self._update_node(entity)

        for entity in self._pending_delete:
            await self._delete_node(entity)

        await self._db.flush()
        self._pending_new.clear()
        self._pending_delete.clear()

    async def rollback(self) -> None:
        """Discard all pending changes."""
        for entity in self._pending_new:
            entity._session = None
        self._pending_new.clear()
        self._pending_delete.clear()
        for entity in list(self._identity_map.values()):
            if entity.is_dirty:
                await self.refresh(entity)

    async def transaction(self) -> AsyncUniTransaction:
        """Create an async transaction. Use as `async with session.transaction() as tx:`."""
        return AsyncUniTransaction(self)

    async def cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        result_type: type[NodeT] | None = None,
    ) -> list[NodeT] | list[dict[str, Any]]:
        """Execute a raw Cypher query."""
        results = await self._db.query(query, params)

        if result_type is None:
            return cast(list[dict[str, Any]], results)

        mapped = []
        for row in results:
            for key, value in row.items():
                if isinstance(value, dict):
                    if "_id" in value and "_label" in value:
                        instance = self._result_to_model(value, result_type)
                        if instance:
                            mapped.append(instance)
                            break
                    elif "_label" in value:
                        label = value["_label"]
                        if label in self._schema_gen._node_models:
                            model = self._schema_gen._node_models[label]
                            instance = self._result_to_model(value, model)
                            if instance:
                                mapped.append(instance)
                                break
            else:
                first_value = next(iter(row.values()), None)
                if isinstance(first_value, dict):
                    instance = self._result_to_model(first_value, result_type)
                    if instance:
                        mapped.append(instance)

        return mapped

    async def create_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: dict[str, Any] | UniEdge | None = None,
    ) -> None:
        """Create an edge between two nodes."""
        src_vid, dst_vid, src_label, dst_label = UniSession._validate_edge_endpoints(
            source, target
        )
        props = UniSession._normalize_edge_properties(properties)

        props_str = ", ".join(f"{k}: ${k}" for k in props)
        if props_str:
            cypher = f"MATCH (a:{src_label}), (b:{dst_label}) WHERE a._vid = $src AND b._vid = $dst CREATE (a)-[r:{edge_type} {{{props_str}}}]->(b)"
        else:
            cypher = f"MATCH (a:{src_label}), (b:{dst_label}) WHERE a._vid = $src AND b._vid = $dst CREATE (a)-[r:{edge_type}]->(b)"

        await self._db.query(cypher, {"src": src_vid, "dst": dst_vid, **props})

    async def delete_edge(
        self, source: UniNode, edge_type: str, target: UniNode
    ) -> int:
        """Delete edges between two nodes. Returns the number of deleted edges."""
        src_vid, dst_vid, src_label, dst_label = UniSession._validate_edge_endpoints(
            source, target
        )
        cypher = (
            f"MATCH (a:{src_label})-[r:{edge_type}]->(b:{dst_label}) "
            f"WHERE a._vid = $src AND b._vid = $dst "
            f"DELETE r RETURN count(r) as count"
        )
        results = await self._db.query(cypher, {"src": src_vid, "dst": dst_vid})
        return cast(int, results[0]["count"]) if results else 0

    async def bulk_add(self, entities: Sequence[UniNode]) -> list[int]:
        """Bulk-add entities using bulk_insert_vertices."""
        if not entities:
            return []

        by_label: dict[str, list[UniNode]] = {}
        for entity in entities:
            label = entity.__class__.__label__
            if label not in by_label:
                by_label[label] = []
            by_label[label].append(entity)

        all_vids: list[int] = []
        try:
            for label, group in by_label.items():
                for entity in group:
                    run_hooks(entity, _BEFORE_CREATE)
                prop_dicts = [e.to_properties() for e in group]
                vids = await self._db.bulk_insert_vertices(label, prop_dicts)
                for entity, vid in zip(group, vids):
                    entity._attach_session(self, vid)
                    self._identity_map[(label, vid)] = entity
                    run_hooks(entity, _AFTER_CREATE)
                    entity._mark_clean()
                all_vids.extend(vids)
        except Exception as e:
            raise BulkLoadError(f"Bulk insert failed: {e}") from e

        return all_vids

    async def explain(self, cypher: str) -> dict[str, Any]:
        """Get the query execution plan."""
        return await self._db.explain(cypher)

    async def profile(self, cypher: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Run the query with profiling."""
        return await self._db.profile(cypher)

    async def save_schema(self, path: str) -> None:
        """Save the database schema to a file."""
        await self._db.save_schema(path)

    async def load_schema(self, path: str) -> None:
        """Load a database schema from a file."""
        await self._db.load_schema(path)

    # ---- Internal methods ----

    async def _create_node(self, entity: UniNode) -> None:
        run_hooks(entity, _BEFORE_CREATE)
        label = entity.__class__.__label__
        props = entity.to_properties()
        props_str = ", ".join(f"{k}: ${k}" for k in props)
        cypher = f"CREATE (n:{label} {{{props_str}}}) RETURN id(n) as vid"
        results = await self._db.query(cypher, props)
        if results:
            vid = results[0]["vid"]
            entity._attach_session(self, vid)
            self._identity_map[(label, vid)] = entity
        run_hooks(entity, _AFTER_CREATE)
        entity._mark_clean()

    async def _create_node_in_tx(
        self, entity: UniNode, tx: uni_db.AsyncTransaction
    ) -> None:
        run_hooks(entity, _BEFORE_CREATE)
        label = entity.__class__.__label__
        props = entity.to_properties()
        props_str = ", ".join(f"{k}: ${k}" for k in props)
        cypher = f"CREATE (n:{label} {{{props_str}}}) RETURN id(n) as vid"
        results = await tx.query(cypher, props)
        if results:
            vid = results[0]["vid"]
            entity._attach_session(self, vid)
            self._identity_map[(label, vid)] = entity
        run_hooks(entity, _AFTER_CREATE)

    async def _create_edge_in_tx(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: UniEdge | None,
        tx: uni_db.AsyncTransaction,
    ) -> None:
        props = properties.to_properties() if properties else {}
        src_label = source.__class__.__label__
        dst_label = target.__class__.__label__
        props_str = ", ".join(f"{k}: ${k}" for k in props)
        if props_str:
            cypher = f"MATCH (a:{src_label}), (b:{dst_label}) WHERE a._vid = $src AND b._vid = $dst CREATE (a)-[:{edge_type} {{{props_str}}}]->(b)"
        else:
            cypher = f"MATCH (a:{src_label}), (b:{dst_label}) WHERE a._vid = $src AND b._vid = $dst CREATE (a)-[:{edge_type}]->(b)"
        params = {"src": source._vid, "dst": target._vid, **props}
        await tx.query(cypher, params)

    async def _update_node(self, entity: UniNode) -> None:
        run_hooks(entity, _BEFORE_UPDATE)
        label = entity.__class__.__label__
        try:
            hints = get_type_hints(type(entity))
        except Exception:
            hints = {}
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
        await self._db.query(cypher, params)
        run_hooks(entity, _AFTER_UPDATE)
        entity._mark_clean()

    async def _delete_node(self, entity: UniNode) -> None:
        run_hooks(entity, _BEFORE_DELETE)
        label = entity.__class__.__label__
        vid = entity._vid
        cypher = f"MATCH (n:{label}) WHERE id(n) = $vid DETACH DELETE n"
        await self._db.query(cypher, {"vid": vid})
        if vid is not None and (label, vid) in self._identity_map:
            del self._identity_map[(label, vid)]
        entity._vid = None
        entity._uid = None
        entity._session = None
        run_hooks(entity, _AFTER_DELETE)

    def _result_to_model(
        self,
        data: dict[str, Any],
        model: type[NodeT],
    ) -> NodeT | None:
        """Convert a query result row to a model instance (sync — pure dict processing)."""
        if not data:
            return None

        data = dict(data)
        data = run_class_hooks(model, _BEFORE_LOAD, data) or data

        vid = data.pop("_id", None)
        if vid is None:
            vid = data.pop("_vid", None)
        if vid is None:
            vid = data.pop("vid", None)
        if vid is not None and not isinstance(vid, int):
            vid = int(vid)
        data.pop("_label", None)

        try:
            instance = cast(
                NodeT,
                model.from_properties(data, vid=vid, session=self),
            )
        except Exception:
            return None

        if vid is not None:
            existing = self._identity_map.get((model.__label__, vid))
            if existing is not None:
                return cast(NodeT, existing)
            self._identity_map[(model.__label__, vid)] = instance

        run_hooks(instance, _AFTER_LOAD)
        return instance

    def _load_relationship(
        self,
        entity: UniNode,
        descriptor: RelationshipDescriptor[Any],
    ) -> list[UniNode] | UniNode | None:
        """Sync relationship loading — raises error for async session.
        Use _async_load_relationship instead."""
        raise SessionError(
            "Cannot synchronously load relationships in an async session. "
            "Use eager_load() or access relationships via async queries."
        )

    async def _async_eager_load_relationships(
        self,
        entities: list[NodeT],
        relationships: list[str],
    ) -> None:
        """Eager load relationships for a list of entities (async)."""
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
            results = await self._db.query(cypher, {"vids": vids})

            by_source: dict[int, list[Any]] = {}
            for row in results:
                src_vid = row["src_vid"]
                node_data = _row_to_node_dict(row)
                if node_data is None:
                    continue
                if src_vid not in by_source:
                    by_source[src_vid] = []
                by_source[src_vid].append(node_data)

            for entity in entities:
                if entity._vid in by_source:
                    related = by_source[entity._vid]
                    cache_attr = f"_rel_cache_{rel_name}"
                    setattr(entity, cache_attr, related)
