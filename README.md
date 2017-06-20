# Query Executor
Is a wrapper for querying data in MySQL and Mongo.
You dont have to understand SQL, however you still have to understand about query logical.

## How To Use
### Initiating Object
```
qe = QueEx()
```

### Filtering mongo collection rows
You can set your collection in _Config class
```
result = qe.mongo_query({'name': ('kadek', 'partial')}, '$or')
for coll, rows in result.iteritems():
    pprint(rows)
```
Its mean, looking for rows that contain kadek in key `name`

### Querying to MySQL
```
result = qe.mysql_query(['nama', 'jenis_kelamin'], 'contact_person',
    {'conditions': [('logical', 'or'), ('nama', ('riki', 'contain'))]},
    {'logical': 'and', 'conditions': [('logical', 'and'), ('nama', ('hidayat', 'endswith')), ('alamat', ('bandung', 'equal'))]},
)
```
Its mean, looking for rows that (contain 'riki' in `nama` column) and (ends with 'hidayat' in `nama` column and equal with bandung in `alamat` column)


Full documentation is available in its function.

## Depedencies
* MySQLdb
* pymongo
