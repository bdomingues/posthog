from posthog.hogql.ast import SelectQuery
from posthog.hogql.context import HogQLContext
from posthog.hogql.database.argmax import argmax_select
from posthog.hogql.database.lazy_join_tags import GROUPS_REVENUE_ANALYTICS
from posthog.hogql.database.models import (
    DateTimeDatabaseField,
    FieldOrTable,
    IntegerDatabaseField,
    LazyJoin,
    LazyJoinToAdd,
    LazyTable,
    LazyTableToAdd,
    StringDatabaseField,
    StringJSONDatabaseField,
    Table,
)
from posthog.hogql.database.schema.groups_revenue_analytics import GroupsRevenueAnalyticsTable
from posthog.hogql.errors import ResolutionError
from posthog.hogql.visitor import TraversingVisitor, clone_expr

GROUPS_TABLE_FIELDS: dict[str, FieldOrTable] = {
    "index": IntegerDatabaseField(name="group_type_index", nullable=False),
    "team_id": IntegerDatabaseField(name="team_id", nullable=False),
    "key": StringDatabaseField(name="group_key", nullable=False),
    "created_at": DateTimeDatabaseField(name="created_at", nullable=False),
    "updated_at": DateTimeDatabaseField(name="_timestamp", nullable=False),
    "properties": StringJSONDatabaseField(name="group_properties", nullable=False),
    "revenue_analytics": LazyJoin(
        from_field=["key"],
        join_table=GroupsRevenueAnalyticsTable(),
        resolver=GROUPS_REVENUE_ANALYTICS,
    ),
}


def select_from_groups_table(requested_fields: dict[str, list[str | int]]):
    return argmax_select(
        table_name="raw_groups",
        select_fields=requested_fields,
        group_fields=["index", "key"],
        argmax_field="updated_at",
    )


def join_with_group_n_table(
    join_to_add: LazyJoinToAdd,
    context: HogQLContext,
    node: SelectQuery,
):
    from posthog.hogql import ast

    # Which $group_N events column to join on; carried as plain data instead of being
    # captured in a closure so the LazyJoin (and the Database holding it) stays serializable.
    group_index = join_to_add.lazy_join.resolver_params.get("group_index")
    if group_index is None:
        raise ResolutionError("group_n lazy join requires resolver_params['group_index']")

    if not join_to_add.fields_accessed:
        raise ResolutionError("No fields requested from person_distinct_ids")

    select_query = select_from_groups_table(join_to_add.fields_accessed)
    select_query.where = ast.CompareOperation(
        left=ast.Field(chain=["index"]),
        op=ast.CompareOperationOp.Eq,
        right=ast.Constant(value=group_index),
    )

    # If the outer query has a bounded prefilter on `events` (one that references `timestamp` —
    # which is the strongest signal we have that the matched set is small), push an additional
    # `group_key IN (SELECT $group_N FROM events WHERE <outer prefilter>)` predicate into the
    # groups subquery. Without this filter, the LEFT JOIN materializes a hash table containing
    # every group of this type for the team, decompressing the (very wide) `group_properties`
    # blob row by row — on high-volume teams that's enough to OOM the whole query even when
    # only a handful of events are actually being selected. With the filter, the hash table is
    # bounded by the distinct `$group_N` values present in the matched events.
    events_prefilter = _outer_events_prefilter(node)
    if events_prefilter is not None:
        key_subquery = ast.SelectQuery(
            select=[ast.Field(chain=[f"$group_{group_index}"])],
            select_from=ast.JoinExpr(table=ast.Field(chain=["events"])),
            where=events_prefilter,
        )
        select_query.where = ast.And(
            exprs=[
                select_query.where,
                ast.CompareOperation(
                    op=ast.CompareOperationOp.In,
                    left=ast.Field(chain=["key"]),
                    right=key_subquery,
                ),
            ]
        )

    join_expr = ast.JoinExpr(table=select_query)
    join_expr.join_type = "LEFT JOIN"
    join_expr.alias = join_to_add.to_table
    join_expr.constraint = ast.JoinConstraint(
        expr=ast.CompareOperation(
            op=ast.CompareOperationOp.Eq,
            left=ast.Field(chain=[join_to_add.from_table, f"$group_{group_index}"]),
            right=ast.Field(chain=[join_to_add.to_table, "key"]),
        ),
        constraint_type="ON",
    )

    return join_expr


# Aliases on the events table that resolve to lazy joins or traversers. If the outer WHERE
# references any of these (e.g. `WHERE group_0.properties.X = 'Y'`, `WHERE person.id = ...`),
# cloning that WHERE into the inner `SELECT $group_N FROM events WHERE ...` subquery would
# carry the typed `Field` for the lazy join, and the resolver would recursively try to
# resolve the same lazy join inside our inner subquery — producing unbounded recursion or a
# `ResolutionError: Select query must have a type`. Skip the optimization in that case;
# we'd rather pay the original groups-hash-table cost than crash.
EVENTS_LAZY_JOIN_ALIASES = frozenset(
    {
        "person",
        "person_id",
        "pdi",
        "poe",
        "group_0",
        "group_1",
        "group_2",
        "group_3",
        "group_4",
        "goe_0",
        "goe_1",
        "goe_2",
        "goe_3",
        "goe_4",
        "session",
        "revenue_analytics",
    }
)


def _outer_events_prefilter(node: SelectQuery):
    """
    Extract a clone of the outer query's WHERE that we can safely embed inside the groups
    join subquery. We only return it when:

    1. It references the `timestamp` field — cheap heuristic for "the matched event set is
       bounded by a date range." Without that guard, a query whose WHERE is only
       `team_id = X` (or empty) would push a key subquery that scans every event for the
       team, which is strictly worse than no filter at all.
    2. It does not reference any lazy-join alias on the events table. Cloning a
       `group_N.X` / `person.X` reference into the inner subquery would re-trigger the
       resolver on the same lazy join during inner-subquery resolution.
    """
    from posthog.hogql.transforms.lazy_tables import find_field_chains

    where = node.where
    if where is None:
        return None
    if not _references_timestamp(where):
        return None
    if any(
        chain and isinstance(chain[0], str) and chain[0] in EVENTS_LAZY_JOIN_ALIASES
        for chain in find_field_chains(where)
    ):
        return None
    return clone_expr(where)


class _TimestampReferenceFinder(TraversingVisitor):
    """Walks an expression to see whether it touches the `timestamp` field."""

    def __init__(self):
        super().__init__()
        self.found = False

    def visit_field(self, node):
        if node.chain and node.chain[-1] == "timestamp":
            self.found = True


def _references_timestamp(expr) -> bool:
    finder = _TimestampReferenceFinder()
    finder.visit(expr)
    return finder.found


class RawGroupsTable(Table):
    fields: dict[str, FieldOrTable] = GROUPS_TABLE_FIELDS

    def to_printed_clickhouse(self, context):
        return "groups"

    def to_printed_hogql(self):
        return "raw_groups"


class GroupsTable(LazyTable):
    fields: dict[str, FieldOrTable] = GROUPS_TABLE_FIELDS

    def lazy_select(self, table_to_add: LazyTableToAdd, context, node):
        return select_from_groups_table(table_to_add.fields_accessed)

    def to_printed_clickhouse(self, context):
        return "groups"

    def to_printed_hogql(self):
        return "groups"
