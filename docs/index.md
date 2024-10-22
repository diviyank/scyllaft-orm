# scylla-bridge

A simple interface over ScyllaFT or ScyllaPy.

## Create all tables and views

To create them, you must import them and then use the `MetaData` class:

```py
from scylla_bridge import MetaData
m = MetaData()
await m.create_all()
```
