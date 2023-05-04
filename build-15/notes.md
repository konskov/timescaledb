for the second query (good one)
```
explain
select distinct machine_id, message_key from
datalake.sensor_values
where sensor_values.message_key in ('epec_supply', 'latitude', 'longitude')
and sensor_values.machine_id IN ('123') and sensor_values.ts between '2023-02-01' and '2023-02-02';
```

the clauses are two and both are about the timestamp values
```
(gdb) p *clauses
$1 = <List <RestrictInfo clause=<OpExpr no=timestamptz_ge, args=<List <Var no=2, attno=1, type=timestamptz>, <Const type=timestamptz, value=728517600000000, isnull=false>>>, is_pushed_down=true, can_join=false, pseudoconstant=false, leakproof=false, has_volatile=VOLATILITY_UNKNOWN, security_level=0, clause_relids=<Bitmapset 2>, required_relids=<Bitmapset 2>, left_relids=<Bitmapset 2>, outer_is_left=false, hashjoinoperator=0, left_hasheqoperator=0, right_hasheqoperator=0>, <RestrictInfo clause=<OpExpr no=timestamptz_le, args=<List <Var no=2, attno=1, type=timestamptz>, <Const type=timestamptz, value=728604000000000, isnull=false>>>, is_pushed_down=true, can_join=false, pseudoconstant=false, leakproof=false, has_volatile=VOLATILITY_UNKNOWN, security_level=0, clause_relids=<Bitmapset 2>, required_relids=<Bitmapset 2>, left_relids=<Bitmapset 2>, outer_is_left=false, hashjoinoperator=0, left_hasheqoperator=0, right_hasheqoperator=0>>
```
and they should be the filter 
what about the index conditions?

p *ipath->indexclauses
```
(gdb) p *ipath->indexclauses
$4 = <List <IndexClause>, <IndexClause>>
```
p *(IndexClause *)ipath->indexclauses->elements[0].ptr_value
p *((IndexClause *)ipath->indexclauses->elements[0].ptr_value)->indexquals
```
(gdb) p *((IndexClause *)ipath->indexclauses->elements[0].ptr_value)->indexquals 
$8 = <List <RestrictInfo clause=<ScalarArrayOpExpr opno=98, opfuncid=67, hashfuncid=0, negfuncid=0, useOr=true, args=<List <RelabelType>, <Const type=_text, value=94284494718600, isnull=false>>>, is_pushed_down=true, can_join=false, pseudoconstant=false, leakproof=false, has_volatile=VOLATILITY_UNKNOWN, security_level=0, clause_relids=<Bitmapset 3>, required_relids=<Bitmapset 3>, outer_is_left=false, hashjoinoperator=0, left_hasheqoperator=0, right_hasheqoperator=0>>
```
and 
```
(gdb) p *((IndexClause *)ipath->indexclauses->elements[1].ptr_value)->indexquals 
$9 = <List <RestrictInfo clause=<OpExpr no=texteq, args=<List <Var no=3, attno=2, type=text, collid=100>, <Const type=text, value=94284495670344, isnull=false>>>, is_pushed_down=true, can_join=false, pseudoconstant=false, leakproof=false, has_volatile=VOLATILITY_UNKNOWN, security_level=0, clause_relids=<Bitmapset 3>, required_relids=<Bitmapset 3>, left_relids=<Bitmapset 3>, outer_is_left=false, hashjoinoperator=0, left_hasheqoperator=0, right_hasheqoperator=0>>
```

hint: look at baserestrictinfo in decompress_chunk_generate_paths

when the chunk is decompressed, I see there is a JOIN FILTER node present. 

### need to read about relation indentification and qual clause placement

I should check the `get_index_paths` function to see what paths it produces with the given quals

btw, I noticed that no joininfo is available and also, jclauseset is empty. Additionally, rclauseset is empty, and also eclauseset is non-empty.