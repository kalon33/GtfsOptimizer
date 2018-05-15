# GtfsOptimizer

This is designed to work with databases built from GTFS using https://github.com/TransitFeeds/GtfsToSql.

It is used to shrink the amount of data in the two largest tables, `shapes` and `stop_times`.

Note however that this repository only currently supports Sqlite. It could be modified pretty easily though, since the GtfsToSql supports PostgreSQL too.

Usage:

`java -jar GtfsSqlOptimizer.jar /path/to/db.sqlite`
