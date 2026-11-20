# Methodology and project explanation

## Data preparation and quality

Acquisition downloads the official UCI archive, reads the workbook in streaming mode, and selects real rows in original order. It applies the schema adaptation and currency assumption described in `data/README.md`. Missing descriptions, returns, and cancellations remain present for audit. A streaming local CSV preflight checks unique required headers, record width, and strict quoting before Spark loads the file. Quoted multiline fields are supported. The Spark pipeline trims text and validates quantity's whole-digit representation before casting it to a 64-bit integer. It casts price to decimal and date to timestamp. It counts missing values in each required raw column and flags failed or invalid values after casting.

The positive-sales view requires all four identifying text fields, positive whole-number quantity within the 64-bit range, positive price, a valid timestamp, a unique line ID, and an invoice that is not a cancellation. The invoice cancellation prefix C is checked case-insensitively. Fractional, scientific, and nonfinite quantity text is rejected before casting. Every source row is written with flags, including rejected rows. The audit retains the original trimmed quantity text. Rule counts can overlap because one row can violate several rules. The summary's rejected-row count counts each rejected row once.

## Analytical calculations

The pipeline uses Spark SQL expressions throughout. Each retained transaction line receives `round(quantity_sold * unit_price, 2)`. Decimal arithmetic preserves monetary precision under the stated conversion and line-rounding policy. Category and market totals sum line totals. The top five products sum positive quantity and use a deterministic product-name tie order. The threshold uses a strict `>` comparison, so exactly ₹10,000 is excluded.

Grouping and summing create aggregation shuffles. Sorting orders results for inspection, and filtering selects valid or high-value lines. CSV tables preserve all calculated results. Charts display up to the largest 20 category or location groups and all five ranked products. No chart claims to display every group when more than 20 exist.

## RDD and DataFrame comparison

An RDD is a partitioned distributed collection. Its records can be arbitrary Python objects, and the programmer expresses transformations such as map and reduceByKey. Spark has limited knowledge of those function bodies. A DataFrame is a distributed table with named, typed columns and a schema. Spark SQL understands column expressions, builds relational plans, and can optimize them.

For the runtime comparison, both approaches read the same cached cleaned sales, aggregate the same rounded decimal line totals by category, and order by category. The DataFrame uses groupBy and sum. The RDD maps each row to a category/value pair, reduces by key, and sorts the keys. An exact equality check precedes timing. One warm-up runs each approach, followed by the configured number of alternating timed actions. The JSON report records actual wall-clock seconds and scope.

Small local data, Python serialization/worker startup, JVM warm-up, caching, operating-system scheduling, fixed action order, and shared resources influence the measurements. The benchmark demonstrates comparable execution and mathematical agreement. Interpreting speed requires the actual measurements and their limitations.

## Lazy evaluation and query optimization

The lazy demonstration constructs filter, select, aggregate, and sort transformations inside a dedicated Spark job group. It queries the group's job IDs before executing an action. It then collects the aggregate and records the group's jobs afterward. This establishes an observed action boundary for that run. Schema loading and earlier quality checks already perform their own actions under other groups.

Extended explain output shows parsed, analysed, optimized logical, and physical plans. Catalyst resolves columns/types and applies relational rules such as projection/filter simplification. Spark's execution engine can use whole-stage code generation for supported expressions. Adaptive Query Execution is enabled and can adjust physical execution using runtime statistics, including shuffle partition handling. The formatted plan after the action exposes the resulting execution plan when Spark provides it.

RDD lineage describes dependencies between transformations. DataFrame plans describe relational and physical operators. The actual job/stage DAG is visible in the Spark UI and retained in event logs. These represent related views at different execution levels. Follow the screenshot instructions to inspect a job, its stages, SQL operators, and executor tasks.

## Spark architecture and distributed processing

The driver builds plans and schedules jobs. The cluster manager allocates executors. Executors process partitions through tasks. A job triggered by an action is divided into stages at shuffle boundaries, with tasks operating on stage partitions. Narrow transformations can process records within existing partitions. Aggregations and sorting can require data exchange between partitions and create shuffle dependencies.

In `local[2]`, two local worker threads process scheduled partition tasks under one local application. A cluster master schedules tasks on remote executors. The same DataFrame/RDD expressions retain their distributed semantics, while shared data access, Python/dependency consistency, memory, and cluster resource configuration determine whether a remote run succeeds.

## Interpretation

Outputs describe the selected source subset and the positive-sales policy. Country revenue compares sales markets. Prefix groups compare a documented product-code partition, and product-name rankings compare unit sales. The GBP/INR constant affects monetary totals and the high-value filter. Costs and physical store identities are absent from the source, so measured net profit and store-level analysis require a more detailed real dataset supplied through the documented CSV schema.
