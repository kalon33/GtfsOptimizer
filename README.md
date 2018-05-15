# GtfsOptimizer

This is designed to work with databases built from GTFS using https://github.com/TransitFeeds/GtfsToSql.

Note however that this repository only currently supports Sqlite. It could be modified pretty easily though, since the GtfsToSql supports PostgreSQL too.

Usage:

`java -jar GtfsSqlOptimizer.jar /path/to/db.sqlite`
