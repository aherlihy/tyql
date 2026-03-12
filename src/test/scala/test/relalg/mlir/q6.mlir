module {
  func.func @main() {
    %0 = relalg.query (){
      %1 = relalg.basetable  {table_identifier = "lineitem"} columns: {l_discount => @lineitem::@l_discount({type = !db.decimal<12, 2>}), l_extendedprice => @lineitem::@l_extendedprice({type = !db.decimal<12, 2>}), l_quantity => @lineitem::@l_quantity({type = !db.decimal<12, 2>}), l_shipdate => @lineitem::@l_shipdate({type = !db.date<day>})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %2 = relalg.selection %1 (%arg0: !tuples.tuple){
        %6 = tuples.getcol %arg0 @lineitem::@l_shipdate : !db.date<day>
        %7 = db.constant("1994-01-01") : !db.date<day>
        %8 = db.compare gte %6 : !db.date<day>, %7 : !db.date<day>
        %9 = tuples.getcol %arg0 @lineitem::@l_shipdate : !db.date<day>
        %10 = db.constant("1995-01-01") : !db.date<day>
        %11 = db.compare lt %9 : !db.date<day>, %10 : !db.date<day>
        %12 = tuples.getcol %arg0 @lineitem::@l_discount : !db.decimal<12, 2>
        %13 = db.constant("0.05") : !db.decimal<12, 2>
        %14 = db.compare gte %12 : !db.decimal<12, 2>, %13 : !db.decimal<12, 2>
        %15 = tuples.getcol %arg0 @lineitem::@l_discount : !db.decimal<12, 2>
        %16 = db.constant("0.07") : !db.decimal<12, 2>
        %17 = db.compare lte %15 : !db.decimal<12, 2>, %16 : !db.decimal<12, 2>
        %18 = tuples.getcol %arg0 @lineitem::@l_quantity : !db.decimal<12, 2>
        %19 = db.constant("24.0") : !db.decimal<12, 2>
        %20 = db.compare lt %18 : !db.decimal<12, 2>, %19 : !db.decimal<12, 2>
        %21 = db.and %8, %11, %14, %17, %20 : i1, i1, i1, i1, i1
        tuples.return %21 : i1
      }
      %3 = relalg.map %2 computes : [@aggMap::@tmp_attr_u_1({type = !db.decimal<24, 4>})] (%arg0: !tuples.tuple){
        %6 = tuples.getcol %arg0 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %7 = tuples.getcol %arg0 @lineitem::@l_discount : !db.decimal<12, 2>
        %8 = db.mul %6 : !db.decimal<12, 2>, %7 : !db.decimal<12, 2>
        tuples.return %8 : !db.decimal<24, 4>
      }
      %4 = relalg.aggregation %3 [] computes : [@tmp_attr::@sum_0({type = !db.nullable<!db.decimal<24, 4>>})] (%arg0: !tuples.tuplestream,%arg1: !tuples.tuple){
        %6 = relalg.aggrfn sum @aggMap::@tmp_attr_u_1 %arg0 : !db.nullable<!db.decimal<24, 4>>
        tuples.return %6 : !db.nullable<!db.decimal<24, 4>>
      }
      %5 = relalg.materialize %4 [@tmp_attr::@sum_0] => ["revenue"] : !subop.local_table<[sum_0$0 : !db.nullable<!db.decimal<24, 4>>], ["revenue"]>
      relalg.query_return %5 : !subop.local_table<[sum_0$0 : !db.nullable<!db.decimal<24, 4>>], ["revenue"]>
    } -> !subop.local_table<[sum_0$0 : !db.nullable<!db.decimal<24, 4>>], ["revenue"]>
    subop.set_result 0 %0 : !subop.local_table<[sum_0$0 : !db.nullable<!db.decimal<24, 4>>], ["revenue"]>
    return
  }
}
