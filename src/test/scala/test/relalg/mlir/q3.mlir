module {
  func.func @main() {
    %0 = relalg.query (){
      %1 = relalg.basetable  {table_identifier = "customer"} columns: {c_custkey => @customer::@c_custkey({type = i32}), c_mktsegment => @customer::@c_mktsegment({type = !db.char<10>})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %2 = relalg.basetable  {table_identifier = "orders"} columns: {o_custkey => @orders::@o_custkey({type = i32}), o_orderdate => @orders::@o_orderdate({type = !db.date<day>}), o_orderkey => @orders::@o_orderkey({type = i32}), o_shippriority => @orders::@o_shippriority({type = i32})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %3 = relalg.crossproduct %1, %2
      %4 = relalg.basetable  {table_identifier = "lineitem"} columns: {l_discount => @lineitem::@l_discount({type = !db.decimal<12, 2>}), l_extendedprice => @lineitem::@l_extendedprice({type = !db.decimal<12, 2>}), l_orderkey => @lineitem::@l_orderkey({type = i32}), l_shipdate => @lineitem::@l_shipdate({type = !db.date<day>})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %5 = relalg.crossproduct %3, %4
      %6 = relalg.selection %5 (%arg0: !tuples.tuple){
        %12 = tuples.getcol %arg0 @customer::@c_mktsegment : !db.char<10>
        %13 = db.constant("BUILDING") : !db.string
        %14 = db.cast %12 : !db.char<10> -> !db.string
        %15 = db.compare eq %14 : !db.string, %13 : !db.string
        %16 = tuples.getcol %arg0 @customer::@c_custkey : i32
        %17 = tuples.getcol %arg0 @orders::@o_custkey : i32
        %18 = db.compare eq %16 : i32, %17 : i32
        %19 = tuples.getcol %arg0 @orders::@o_orderdate : !db.date<day>
        %20 = db.constant("1995-03-15") : !db.date<day>
        %21 = db.compare lt %19 : !db.date<day>, %20 : !db.date<day>
        %22 = tuples.getcol %arg0 @lineitem::@l_orderkey : i32
        %23 = tuples.getcol %arg0 @orders::@o_orderkey : i32
        %24 = db.compare eq %22 : i32, %23 : i32
        %25 = tuples.getcol %arg0 @lineitem::@l_shipdate : !db.date<day>
        %26 = db.constant("1995-03-15") : !db.date<day>
        %27 = db.compare gt %25 : !db.date<day>, %26 : !db.date<day>
        %28 = db.and %15, %18, %21, %24, %27 : i1, i1, i1, i1, i1
        tuples.return %28 : i1
      }
      %7 = relalg.map %6 computes : [@aggMap::@tmp_attr_u_1({type = !db.decimal<24, 4>})] (%arg0: !tuples.tuple){
        %12 = tuples.getcol %arg0 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %13 = db.constant("1.0") : !db.decimal<12, 2>
        %14 = tuples.getcol %arg0 @lineitem::@l_discount : !db.decimal<12, 2>
        %15 = db.sub %13 : !db.decimal<12, 2>, %14 : !db.decimal<12, 2>
        %16 = db.mul %12 : !db.decimal<12, 2>, %15 : !db.decimal<12, 2>
        tuples.return %16 : !db.decimal<24, 4>
      }
      %8 = relalg.aggregation %7 [@lineitem::@l_orderkey,@orders::@o_orderdate,@orders::@o_shippriority] computes : [@tmp_attr::@sum_0({type = !db.decimal<24, 4>})] (%arg0: !tuples.tuplestream,%arg1: !tuples.tuple){
        %12 = relalg.aggrfn sum @aggMap::@tmp_attr_u_1 %arg0 : !db.decimal<24, 4>
        tuples.return %12 : !db.decimal<24, 4>
      }
      %9 = relalg.sort %8 [(@tmp_attr::@sum_0,desc),(@orders::@o_orderdate,asc)]
      %10 = relalg.limit 10 %9
      %11 = relalg.materialize %10 [@lineitem::@l_orderkey,@tmp_attr::@sum_0,@orders::@o_orderdate,@orders::@o_shippriority] => ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"] : !subop.local_table<[l_orderkey$0 : i32, sum_0$0 : !db.decimal<24, 4>, o_orderdate$0 : !db.date<day>, o_shippriority$0 : i32], ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]>
      relalg.query_return %11 : !subop.local_table<[l_orderkey$0 : i32, sum_0$0 : !db.decimal<24, 4>, o_orderdate$0 : !db.date<day>, o_shippriority$0 : i32], ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]>
    } -> !subop.local_table<[l_orderkey$0 : i32, sum_0$0 : !db.decimal<24, 4>, o_orderdate$0 : !db.date<day>, o_shippriority$0 : i32], ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]>
    subop.set_result 0 %0 : !subop.local_table<[l_orderkey$0 : i32, sum_0$0 : !db.decimal<24, 4>, o_orderdate$0 : !db.date<day>, o_shippriority$0 : i32], ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]>
    return
  }
}
