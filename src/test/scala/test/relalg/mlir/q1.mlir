module {
  func.func @main() {
    %0 = relalg.query (){
      %1 = relalg.basetable  {table_identifier = "lineitem"} columns: {l_discount => @lineitem::@l_discount({type = !db.decimal<12, 2>}), l_extendedprice => @lineitem::@l_extendedprice({type = !db.decimal<12, 2>}), l_linestatus => @lineitem::@l_linestatus({type = !db.char<1>}), l_quantity => @lineitem::@l_quantity({type = !db.decimal<12, 2>}), l_returnflag => @lineitem::@l_returnflag({type = !db.char<1>}), l_shipdate => @lineitem::@l_shipdate({type = !db.date<day>}), l_tax => @lineitem::@l_tax({type = !db.decimal<12, 2>})} datasource: "0000FEFF000000000000000000000000FFFF0000"
      %2 = relalg.selection %1 (%arg0: !tuples.tuple){
        %7 = tuples.getcol %arg0 @lineitem::@l_shipdate : !db.date<day>
        %8 = db.constant("1998-09-02") : !db.date<day>
        %9 = db.compare lte %7 : !db.date<day>, %8 : !db.date<day>
        tuples.return %9 : i1
      }
      %3 = relalg.map %2 computes : [@aggMap::@tmp_attr_u_8({type = !db.decimal<24, 4>}),@aggMap::@tmp_attr_u_9({type = !db.decimal<36, 6>})] (%arg0: !tuples.tuple){
        %7 = tuples.getcol %arg0 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %8 = db.constant("1.0") : !db.decimal<12, 2>
        %9 = tuples.getcol %arg0 @lineitem::@l_discount : !db.decimal<12, 2>
        %10 = db.sub %8 : !db.decimal<12, 2>, %9 : !db.decimal<12, 2>
        %11 = db.mul %7 : !db.decimal<12, 2>, %10 : !db.decimal<12, 2>
        %12 = tuples.getcol %arg0 @lineitem::@l_extendedprice : !db.decimal<12, 2>
        %13 = db.constant("1.0") : !db.decimal<12, 2>
        %14 = tuples.getcol %arg0 @lineitem::@l_discount : !db.decimal<12, 2>
        %15 = db.sub %13 : !db.decimal<12, 2>, %14 : !db.decimal<12, 2>
        %16 = db.mul %12 : !db.decimal<12, 2>, %15 : !db.decimal<12, 2>
        %17 = db.constant("1.0") : !db.decimal<12, 2>
        %18 = tuples.getcol %arg0 @lineitem::@l_tax : !db.decimal<12, 2>
        %19 = db.add %17 : !db.decimal<12, 2>, %18 : !db.decimal<12, 2>
        %20 = db.mul %16 : !db.decimal<24, 4>, %19 : !db.decimal<12, 2>
        tuples.return %11, %20 : !db.decimal<24, 4>, !db.decimal<36, 6>
      }
      %4 = relalg.aggregation %3 [@lineitem::@l_returnflag,@lineitem::@l_linestatus] computes : [@tmp_attr_u_7::@sum_0({type = !db.decimal<12, 2>}),@tmp_attr_u_6::@sum_1({type = !db.decimal<12, 2>}),@tmp_attr_u_5::@sum_2({type = !db.decimal<24, 4>}),@tmp_attr_u_4::@sum_3({type = !db.decimal<36, 6>}),@tmp_attr_u_3::@avg_4({type = !db.decimal<31, 21>}),@tmp_attr_u_2::@avg_5({type = !db.decimal<31, 21>}),@tmp_attr_u_1::@avg_6({type = !db.decimal<31, 21>}),@tmp_attr::@count_7({type = i64})] (%arg0: !tuples.tuplestream,%arg1: !tuples.tuple){
        %7 = relalg.aggrfn sum @lineitem::@l_quantity %arg0 : !db.decimal<12, 2>
        %8 = relalg.aggrfn sum @lineitem::@l_extendedprice %arg0 : !db.decimal<12, 2>
        %9 = relalg.aggrfn sum @aggMap::@tmp_attr_u_8 %arg0 : !db.decimal<24, 4>
        %10 = relalg.aggrfn sum @aggMap::@tmp_attr_u_9 %arg0 : !db.decimal<36, 6>
        %11 = relalg.aggrfn avg @lineitem::@l_quantity %arg0 : !db.decimal<31, 21>
        %12 = relalg.aggrfn avg @lineitem::@l_extendedprice %arg0 : !db.decimal<31, 21>
        %13 = relalg.aggrfn avg @lineitem::@l_discount %arg0 : !db.decimal<31, 21>
        %14 = relalg.count %arg0
        tuples.return %7, %8, %9, %10, %11, %12, %13, %14 : !db.decimal<12, 2>, !db.decimal<12, 2>, !db.decimal<24, 4>, !db.decimal<36, 6>, !db.decimal<31, 21>, !db.decimal<31, 21>, !db.decimal<31, 21>, i64
      }
      %5 = relalg.sort %4 [(@lineitem::@l_returnflag,asc),(@lineitem::@l_linestatus,asc)]
      %6 = relalg.materialize %5 [@lineitem::@l_returnflag,@lineitem::@l_linestatus,@tmp_attr_u_7::@sum_0,@tmp_attr_u_6::@sum_1,@tmp_attr_u_5::@sum_2,@tmp_attr_u_4::@sum_3,@tmp_attr_u_3::@avg_4,@tmp_attr_u_2::@avg_5,@tmp_attr_u_1::@avg_6,@tmp_attr::@count_7] => ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"] : !subop.local_table<[l_returnflag$0 : !db.char<1>, l_linestatus$0 : !db.char<1>, sum_0$0 : !db.decimal<12, 2>, sum_1$0 : !db.decimal<12, 2>, sum_2$0 : !db.decimal<24, 4>, sum_3$0 : !db.decimal<36, 6>, avg_4$0 : !db.decimal<31, 21>, avg_5$0 : !db.decimal<31, 21>, avg_6$0 : !db.decimal<31, 21>, count_7$0 : i64], ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"]>
      relalg.query_return %6 : !subop.local_table<[l_returnflag$0 : !db.char<1>, l_linestatus$0 : !db.char<1>, sum_0$0 : !db.decimal<12, 2>, sum_1$0 : !db.decimal<12, 2>, sum_2$0 : !db.decimal<24, 4>, sum_3$0 : !db.decimal<36, 6>, avg_4$0 : !db.decimal<31, 21>, avg_5$0 : !db.decimal<31, 21>, avg_6$0 : !db.decimal<31, 21>, count_7$0 : i64], ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"]>
    } -> !subop.local_table<[l_returnflag$0 : !db.char<1>, l_linestatus$0 : !db.char<1>, sum_0$0 : !db.decimal<12, 2>, sum_1$0 : !db.decimal<12, 2>, sum_2$0 : !db.decimal<24, 4>, sum_3$0 : !db.decimal<36, 6>, avg_4$0 : !db.decimal<31, 21>, avg_5$0 : !db.decimal<31, 21>, avg_6$0 : !db.decimal<31, 21>, count_7$0 : i64], ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"]>
    subop.set_result 0 %0 : !subop.local_table<[l_returnflag$0 : !db.char<1>, l_linestatus$0 : !db.char<1>, sum_0$0 : !db.decimal<12, 2>, sum_1$0 : !db.decimal<12, 2>, sum_2$0 : !db.decimal<24, 4>, sum_3$0 : !db.decimal<36, 6>, avg_4$0 : !db.decimal<31, 21>, avg_5$0 : !db.decimal<31, 21>, avg_6$0 : !db.decimal<31, 21>, count_7$0 : i64], ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"]>
    return
  }
}
