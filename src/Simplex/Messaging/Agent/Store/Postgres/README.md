# Postgres setup

Create three databases - `agent_poc_1`, `agent_poc_2`, `agent_poc_3` - and have Postgres server running.

~~`brew install postgresql` - required by postgresql-simple.~~

~~You may run into compilation errors, then you might also need to `brew install libpq --build-from-source`, see [this Stack Overflow answer](https://stackoverflow.com/a/70012033).~~

In the end I managed to build using cabal.
