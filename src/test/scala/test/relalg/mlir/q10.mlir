module {
  func.func @main() {
    %0 = relalg.query (){
      %1 = relalg.basetable  {table_identifier = "customer"} columns: {c_acctbal => @customer::@c_acctbal({type = !db.decimal<12, 2>}), c_address => @customer::@c_address({type = !db.string}), c_comment => @customer::@c_comment({type = !db.string}), c_custkey => @customer::@c_custkey({type = i32}), c_name => @customer::@c_name({type = !db.string}), c_nationkey => @customer::@c_nationkey({type = i32}), c_phone => @customer::@c_phone({type = !db.char<15>})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %2 = relalg.basetable  {table_identifier = "orders"} columns: {o_custkey => @orders::@o_custkey({type = i32}), o_orderdate => @orders::@o_orderdate({type = !db.date<day>}), o_orderkey => @orders::@o_orderkey({type = i32})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %3 = relalg.crossproduct %1, %2
      %4 = relalg.basetable  {table_identifier = "lineitem"} columns: {l_discount => @lineitem::@l_discount({type = !db.decimal<12, 2>}), l_extendedprice => @lineitem::@l_extendedprice({type = !db.decimal<12, 2>}), l_orderkey => @lineitem::@l_orderkey({type = i32}), l_returnflag => @lineitem::@l_returnflag({type = !db.char<1>})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %5 = relalg.crossproduct %3, %4
      %6 = relalg.basetable  {table_identifier = "nation"} columns: {n_name => @nation::@n_name({type = !db.char<25>}), n_nationkey => @nation::@n_nationkey({type = i32})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %7 = relalg.crossproduct %5, %6
      %8 = relalg.selection %7 (%arg0: !tuples.tuple){
        %14 = tuples.getcol %arg0 @customer::@c_custkey : i32
        %15 = tuples.getcol %arg0 @orders::@o_custkey : i32
        %16 = db.compare eq %14 : i32, %15 : i32
        %17 = tuples.getcol %arg0 @orders::@o_orderdate : !db.date<day>
        %18 = db.constant("1993-10-01") : !db.date<day>
        %19 = db.compare gte %17 : !db.date<day>, %18 : !db.date<day>
        %20 = tuples.getcol %arg0 @orders::@o_orderdate : !db.date<day>
        %21 = db.constant("1994-01-01") : !db.date<day>
        %22 = db.compare lt %20 : !db.date<day>, %21 : !db.date<day>
        %23 = tuples.getcol %arg0 @lineitem::@l_orderkey : i32
        %24 = tuples.getcol %arg0 @orders::@o_orderkey : i32
        %25 = db.compare eq %23 : i32, %24 : i32
        %26 = tuples.getcol %arg0 @lineitem::@l_returnflag : !db.char<1>
        %27 = db.constant("R") : !db.char<1>
        %28 = db.compare eq %26 : !db.char<1>, %27 : !db.char<1>
        %29 = tuples.getcol %arg0 @customer::@c_nationkey : i32
        %30 = tuples.getcol %arg0 @nation::@n_nationkey : i32
        %31 = db.compare eq %29 : i32, %30 : i32
        %32 = db.and %16, %19, %22, %25, %28, %31 : i1, i1, i1, i1, i1, i1
        tuples.return %32 : i1
      }
      %9 = relalg.map %8 computes : [@aggMap::@tmp_attr_u_1({type = !db.decimal<24, 4>})] (%arg0: !tuples.tuple){
        %14 = tuples.getcol %arg0 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %15 = db.constant("1.0") : !db.decimal<12, 2>
        %16 = tuples.getcol %arg0 @lineitem::@l_discount : !db.decimal<12, 2>
        %17 = db.sub %15 : !db.decimal<12, 2>, %16 : !db.decimal<12, 2>
        %18 = db.mul %14 : !db.decimal<12, 2>, %17 : !db.decimal<12, 2>
        tuples.return %18 : !db.decimal<24, 4>
      }
      %10 = relalg.aggregation %9 [@customer::@c_custkey,@customer::@c_name,@customer::@c_acctbal,@customer::@c_phone,@nation::@n_name,@customer::@c_address,@customer::@c_comment] computes : [@tmp_attr::@sum_0({type = !db.decimal<24, 4>})] (%arg0: !tuples.tuplestream,%arg1: !tuples.tuple){
        %14 = relalg.aggrfn sum @aggMap::@tmp_attr_u_1 %arg0 : !db.decimal<24, 4>
        tuples.return %14 : !db.decimal<24, 4>
      }
      %11 = relalg.sort %10 [(@tmp_attr::@sum_0,desc)]
      %12 = relalg.limit 20 %11
      %13 = relalg.materialize %12 [@customer::@c_custkey,@customer::@c_name,@tmp_attr::@sum_0,@customer::@c_acctbal,@nation::@n_name,@customer::@c_address,@customer::@c_phone,@customer::@c_comment] => ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"] : !subop.local_table<[c_custkey$0 : i32, c_name$0 : !db.string, sum_0$0 : !db.decimal<24, 4>, c_acctbal$0 : !db.decimal<12, 2>, n_name$0 : !db.char<25>, c_address$0 : !db.string, c_phone$0 : !db.char<15>, c_comment$0 : !db.string], ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"]>
      relalg.query_return %13 : !subop.local_table<[c_custkey$0 : i32, c_name$0 : !db.string, sum_0$0 : !db.decimal<24, 4>, c_acctbal$0 : !db.decimal<12, 2>, n_name$0 : !db.char<25>, c_address$0 : !db.string, c_phone$0 : !db.char<15>, c_comment$0 : !db.string], ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"]>
    } -> !subop.local_table<[c_custkey$0 : i32, c_name$0 : !db.string, sum_0$0 : !db.decimal<24, 4>, c_acctbal$0 : !db.decimal<12, 2>, n_name$0 : !db.char<25>, c_address$0 : !db.string, c_phone$0 : !db.char<15>, c_comment$0 : !db.string], ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"]>
    subop.set_result 0 %0 : !subop.local_table<[c_custkey$0 : i32, c_name$0 : !db.string, sum_0$0 : !db.decimal<24, 4>, c_acctbal$0 : !db.decimal<12, 2>, n_name$0 : !db.char<25>, c_address$0 : !db.string, c_phone$0 : !db.char<15>, c_comment$0 : !db.string], ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"]>
    return
  }
}
