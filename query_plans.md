## Top campaigns by revenue in September

### Parquet partitioned by date input:
```
== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'SUM('billingCost) AS revenue#109]
         +- 'Filter ('isConfirmed = true)
            +- 'UnresolvedRelation [purchasesAttribution], [], false

== Analyzed Logical Plan ==
campaignId: string, revenue: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#109 DESC NULLS LAST], true
      +- Aggregate [campaignId#54], [campaignId#54, sum(billingCost#14) AS revenue#109]
         +- Filter (isConfirmed#15 = true)
            +- SubqueryAlias purchasesattribution
               +- Project [purchaseId#76, purchaseTime#13, billingCost#14, isConfirmed#15, sessionId#37, campaignId#54, channelIid#65]
                  +- Project [purchaseId#76, userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37, campaignId#54, channelIid#65, purchaseTime#13, billingCost#14, isConfirmed#15, purchaseTimeDate#22]
                     +- Join Inner, (purchaseId#76 = purchaseId#12)
                        :- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37, campaignId#54, channelIid#65, attributes#4[purchase_id] AS purchaseId#76]
                        :  +- Filter (eventType#2 = purchase)
                        :     +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37, campaignId#54, channelIid#65]
                        :        +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37, campaignId#54, channelIid#65, channelIid#65]
                        :           +- Window [last(attributes#4[channel_id], true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIid#65], [userId#0], [eventTime#3 ASC NULLS FIRST]
                        :              +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37, campaignId#54]
                        :                 +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37, campaignId#54]
                        :                    +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37, campaignId#54, campaignId#54]
                        :                       +- Window [last(attributes#4[campaign_id], true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#54], [userId#0], [eventTime#3 ASC NULLS FIRST]
                        :                          +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37]
                        :                             +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#37]
                        :                                +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, tmpSessionId#28, sessionId#37]
                        :                                   +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, tmpSessionId#28, sessionId#37, sessionId#37]
                        :                                      +- Window [last(tmpSessionId#28, true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS sessionId#37], [userId#0], [eventTime#3 ASC NULLS FIRST]
                        :                                         +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, tmpSessionId#28]
                        :                                            +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, CASE WHEN (eventType#2 = app_open) THEN eventId#1 END AS tmpSessionId#28]
                        :                                               +- Filter ((eventTimeDate#5 >= cast(2020-10-01 as date)) AND (eventTimeDate#5 <= cast(2020-10-31 as date)))
                        :                                                  +- Relation[userId#0,eventId#1,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] parquet
                        +- Filter ((purchaseTimeDate#22 >= cast(2020-10-01 as date)) AND (purchaseTimeDate#22 <= cast(2020-10-31 as date)))
                           +- Project [purchaseId#12, purchaseTime#13, billingCost#14, isConfirmed#15, to_date('purchaseTime, Some(yyyy-mm-dd)) AS purchaseTimeDate#22]
                              +- Relation[purchaseId#12,purchaseTime#13,billingCost#14,isConfirmed#15,purchaseTimeDate#16] parquet

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#109 DESC NULLS LAST], true
      +- Aggregate [campaignId#54], [campaignId#54, sum(billingCost#14) AS revenue#109]
         +- Project [billingCost#14, campaignId#54]
            +- Join Inner, (purchaseId#76 = purchaseId#12)
               :- Project [campaignId#54, attributes#4[purchase_id] AS purchaseId#76]
               :  +- Filter ((isnotnull(eventType#2) AND (eventType#2 = purchase)) AND isnotnull(attributes#4[purchase_id]))
               :     +- Window [last(attributes#4[campaign_id], true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#54], [userId#0], [eventTime#3 ASC NULLS FIRST]
               :        +- Project [userId#0, eventType#2, eventTime#3, attributes#4]
               :           +- Filter ((isnotnull(eventTimeDate#5) AND (eventTimeDate#5 >= 18536)) AND (eventTimeDate#5 <= 18566))
               :              +- Relation[userId#0,eventId#1,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] parquet
               +- Project [purchaseId#12, billingCost#14]
                  +- Filter ((((isnotnull(isConfirmed#15) AND (cast(gettimestamp(purchaseTime#13, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) >= 18536)) AND (cast(gettimestamp(purchaseTime#13, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) <= 18566)) AND (isConfirmed#15 = true)) AND isnotnull(purchaseId#12))
                     +- Relation[purchaseId#12,purchaseTime#13,billingCost#14,isConfirmed#15,purchaseTimeDate#16] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#109 DESC NULLS LAST], output=[campaignId#54,revenue#109])
+- *(8) HashAggregate(keys=[campaignId#54], functions=[sum(billingCost#14)], output=[campaignId#54, revenue#109])
   +- Exchange hashpartitioning(campaignId#54, 200), ENSURE_REQUIREMENTS, [id=#106]
      +- *(7) HashAggregate(keys=[campaignId#54], functions=[partial_sum(billingCost#14)], output=[campaignId#54, sum#115])
         +- *(7) Project [billingCost#14, campaignId#54]
            +- *(7) SortMergeJoin [purchaseId#76], [purchaseId#12], Inner
               :- *(4) Sort [purchaseId#76 ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(purchaseId#76, 200), ENSURE_REQUIREMENTS, [id=#87]
               :     +- *(3) Project [campaignId#54, attributes#4[purchase_id] AS purchaseId#76]
               :        +- *(3) Filter ((isnotnull(eventType#2) AND (eventType#2 = purchase)) AND isnotnull(attributes#4[purchase_id]))
               :           +- Window [last(attributes#4[campaign_id], true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#54], [userId#0], [eventTime#3 ASC NULLS FIRST]
               :              +- *(2) Sort [userId#0 ASC NULLS FIRST, eventTime#3 ASC NULLS FIRST], false, 0
               :                 +- Exchange hashpartitioning(userId#0, 200), ENSURE_REQUIREMENTS, [id=#78]
               :                    +- *(1) Project [userId#0, eventType#2, eventTime#3, attributes#4]
               :                       +- FileScan parquet [userId#0,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] Batched: false, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/mobile..., PartitionFilters: [isnotnull(eventTimeDate#5), (eventTimeDate#5 >= 18536), (eventTimeDate#5 <= 18566)], PushedFilters: [], ReadSchema: struct<userId:string,eventType:string,eventTime:timestamp,attributes:map<string,string>>
               +- *(6) Sort [purchaseId#12 ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(purchaseId#12, 200), ENSURE_REQUIREMENTS, [id=#97]
                     +- *(5) Project [purchaseId#12, billingCost#14]
                        +- *(5) Filter ((((isnotnull(isConfirmed#15) AND (cast(gettimestamp(purchaseTime#13, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) >= 18536)) AND (cast(gettimestamp(purchaseTime#13, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) <= 18566)) AND (isConfirmed#15 = true)) AND isnotnull(purchaseId#12))
                           +- *(5) ColumnarToRow
                              +- FileScan parquet [purchaseId#12,purchaseTime#13,billingCost#14,isConfirmed#15,purchaseTimeDate#16] Batched: true, DataFilters: [isnotnull(isConfirmed#15), (cast(gettimestamp(purchaseTime#13, yyyy-mm-dd, Some(Europe/Warsaw), ..., Format: Parquet, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/user_p..., PartitionFilters: [], PushedFilters: [IsNotNull(isConfirmed), EqualTo(isConfirmed,true), IsNotNull(purchaseId)], ReadSchema: struct<purchaseId:string,purchaseTime:timestamp,billingCost:double,isConfirmed:boolean>
```

### CSV input:
```
== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'SUM('billingCost) AS revenue#152]
         +- 'Filter ('isConfirmed = true)
            +- 'UnresolvedRelation [purchasesAttribution], [], false

== Analyzed Logical Plan ==
campaignId: string, revenue: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#152 DESC NULLS LAST], true
      +- Aggregate [campaignId#97], [campaignId#97, sum(billingCost#52) AS revenue#152]
         +- Filter (isConfirmed#53 = true)
            +- SubqueryAlias purchasesattribution
               +- Project [purchaseId#119, purchaseTime#51, billingCost#52, isConfirmed#53, sessionId#80, campaignId#97, channelIid#108]
                  +- Project [purchaseId#119, userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, channelIid#108, purchaseTime#51, billingCost#52, isConfirmed#53, purchaseTimeDate#65]
                     +- Join Inner, (purchaseId#119 = purchaseId#50)
                        :- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, channelIid#108, attributes#44[purchase_id] AS purchaseId#119]
                        :  +- Filter (eventType#18 = purchase)
                        :     +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, channelIid#108]
                        :        +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, channelIid#108, channelIid#108]
                        :           +- Window [last(attributes#44[channel_id], true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIid#108], [userId#16], [eventTime#26 ASC NULLS FIRST]
                        :              +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97]
                        :                 +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97]
                        :                    +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, campaignId#97]
                        :                       +- Window [last(attributes#44[campaign_id], true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#97], [userId#16], [eventTime#26 ASC NULLS FIRST]
                        :                          +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80]
                        :                             +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80]
                        :                                +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, tmpSessionId#71, sessionId#80]
                        :                                   +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, tmpSessionId#71, sessionId#80, sessionId#80]
                        :                                      +- Window [last(tmpSessionId#71, true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS sessionId#80], [userId#16], [eventTime#26 ASC NULLS FIRST]
                        :                                         +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, tmpSessionId#71]
                        :                                            +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, CASE WHEN (eventType#18 = app_open) THEN eventId#17 END AS tmpSessionId#71]
                        :                                               +- Filter ((eventTimeDate#58 >= cast(2020-10-01 as date)) AND (eventTimeDate#58 <= cast(2020-10-31 as date)))
                        :                                                  +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, to_date('eventTime, Some(yyyy-mm-dd)) AS eventTimeDate#58]
                        :                                                     +- Project [userId#16, eventId#17, eventType#18, eventTime#26, from_json(MapType(StringType,StringType,true), attributes#38, Some(Europe/Warsaw)) AS attributes#44]
                        :                                                        +- Project [userId#16, eventId#17, eventType#18, eventTime#26, regexp_replace(attributes#32, "", ', 1) AS attributes#38]
                        :                                                           +- Project [userId#16, eventId#17, eventType#18, eventTime#26, regexp_extract(attributes#20, "?(\{.+\})"?, 1) AS attributes#32]
                        :                                                              +- Project [userId#16, eventId#17, eventType#18, cast(eventTime#19 as timestamp) AS eventTime#26, attributes#20]
                        :                                                                 +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
                        +- Filter ((purchaseTimeDate#65 >= cast(2020-10-01 as date)) AND (purchaseTimeDate#65 <= cast(2020-10-31 as date)))
                           +- Project [purchaseId#50, purchaseTime#51, billingCost#52, isConfirmed#53, to_date('purchaseTime, Some(yyyy-mm-dd)) AS purchaseTimeDate#65]
                              +- Relation[purchaseId#50,purchaseTime#51,billingCost#52,isConfirmed#53] csv

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#152 DESC NULLS LAST], true
      +- Aggregate [campaignId#97], [campaignId#97, sum(billingCost#52) AS revenue#152]
         +- Project [billingCost#52, campaignId#97]
            +- Join Inner, (purchaseId#119 = purchaseId#50)
               :- Project [campaignId#97, attributes#44[purchase_id] AS purchaseId#119]
               :  +- Filter ((isnotnull(eventType#18) AND (eventType#18 = purchase)) AND isnotnull(attributes#44[purchase_id]))
               :     +- Window [last(attributes#44[campaign_id], true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#97], [userId#16], [eventTime#26 ASC NULLS FIRST]
               :        +- Project [userId#16, eventType#18, cast(eventTime#19 as timestamp) AS eventTime#26, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw)) AS attributes#44]
               :           +- Filter ((cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) >= 18536) AND (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) <= 18566))
               :              +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
               +- Project [purchaseId#50, billingCost#52]
                  +- Filter ((((isnotnull(isConfirmed#53) AND (cast(gettimestamp(purchaseTime#51, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) >= 18536)) AND (cast(gettimestamp(purchaseTime#51, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) <= 18566)) AND (isConfirmed#53 = true)) AND isnotnull(purchaseId#50))
                     +- Relation[purchaseId#50,purchaseTime#51,billingCost#52,isConfirmed#53] csv

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#152 DESC NULLS LAST], output=[campaignId#97,revenue#152])
+- *(8) HashAggregate(keys=[campaignId#97], functions=[sum(billingCost#52)], output=[campaignId#97, revenue#152])
   +- Exchange hashpartitioning(campaignId#97, 200), ENSURE_REQUIREMENTS, [id=#116]
      +- *(7) HashAggregate(keys=[campaignId#97], functions=[partial_sum(billingCost#52)], output=[campaignId#97, sum#158])
         +- *(7) Project [billingCost#52, campaignId#97]
            +- *(7) SortMergeJoin [purchaseId#119], [purchaseId#50], Inner
               :- *(4) Sort [purchaseId#119 ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(purchaseId#119, 200), ENSURE_REQUIREMENTS, [id=#98]
               :     +- *(3) Project [campaignId#97, attributes#44[purchase_id] AS purchaseId#119]
               :        +- *(3) Filter ((isnotnull(eventType#18) AND (eventType#18 = purchase)) AND isnotnull(attributes#44[purchase_id]))
               :           +- Window [last(attributes#44[campaign_id], true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#97], [userId#16], [eventTime#26 ASC NULLS FIRST]
               :              +- *(2) Sort [userId#16 ASC NULLS FIRST, eventTime#26 ASC NULLS FIRST], false, 0
               :                 +- Exchange hashpartitioning(userId#16, 200), ENSURE_REQUIREMENTS, [id=#89]
               :                    +- Project [userId#16, eventType#18, cast(eventTime#19 as timestamp) AS eventTime#26, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw)) AS attributes#44]
               :                       +- *(1) Filter ((cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) >= 18536) AND (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) <= 18566))
               :                          +- FileScan csv [userId#16,eventType#18,eventTime#19,attributes#20] Batched: false, DataFilters: [(cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as d..., Format: CSV, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/mobile..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<userId:string,eventType:string,eventTime:string,attributes:string>
               +- *(6) Sort [purchaseId#50 ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(purchaseId#50, 200), ENSURE_REQUIREMENTS, [id=#107]
                     +- *(5) Project [purchaseId#50, billingCost#52]
                        +- *(5) Filter ((((isnotnull(isConfirmed#53) AND (cast(gettimestamp(purchaseTime#51, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) >= 18536)) AND (cast(gettimestamp(purchaseTime#51, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) <= 18566)) AND (isConfirmed#53 = true)) AND isnotnull(purchaseId#50))
                           +- FileScan csv [purchaseId#50,purchaseTime#51,billingCost#52,isConfirmed#53] Batched: false, DataFilters: [isnotnull(isConfirmed#53), (cast(gettimestamp(purchaseTime#51, yyyy-mm-dd, Some(Europe/Warsaw), ..., Format: CSV, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/user_p..., PartitionFilters: [], PushedFilters: [IsNotNull(isConfirmed), EqualTo(isConfirmed,true), IsNotNull(purchaseId)], ReadSchema: struct<purchaseId:string,purchaseTime:timestamp,billingCost:double,isConfirmed:boolean>
```


## Most popular channels for each campaign in September

### Parquet partitioned by date input:
```
== Parsed Logical Plan ==
'Distinct
+- 'Aggregate ['campaignId, 'channelIid], ['campaignId, 'FIRST('channelIid) windowspecdefinition('campaignId, 'COUNT('eventId) DESC NULLS LAST, unspecifiedframe$()) AS channelIiD#24]
   +- 'SubqueryAlias __auto_generated_subquery_name
      +- 'Project ['eventId, 'attributes.campaign_id AS campaignId#22, 'attributes.channel_id AS channelIid#23]
         +- 'Filter ('eventType = app_open)
            +- 'UnresolvedRelation [events], [], false
== Analyzed Logical Plan ==
campaignId: string, channelIiD: string
Distinct
+- Project [campaignId#22, channelIiD#24]
   +- Project [campaignId#22, channelIid#23, _w1#29L, channelIiD#24, channelIiD#24]
      +- Window [first(channelIid#23, false) windowspecdefinition(campaignId#22, _w1#29L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#24], [campaignId#22], [_w1#29L DESC NULLS LAST]
         +- Aggregate [campaignId#22, channelIid#23], [campaignId#22, channelIid#23, count(eventId#1) AS _w1#29L]
            +- SubqueryAlias __auto_generated_subquery_name
               +- Project [eventId#1, attributes#4[campaign_id] AS campaignId#22, attributes#4[channel_id] AS channelIid#23]
                  +- Filter (eventType#2 = app_open)
                     +- SubqueryAlias events
                        +- Filter ((eventTimeDate#5 >= cast(2020-10-01 as date)) AND (eventTimeDate#5 <= cast(2020-10-31 as date)))
                           +- Relation[userId#0,eventId#1,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] parquet
== Optimized Logical Plan ==
Aggregate [campaignId#22, channelIiD#24], [campaignId#22, channelIiD#24]
+- Project [campaignId#22, channelIiD#24]
   +- Window [first(channelIid#23, false) windowspecdefinition(campaignId#22, _w1#29L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#24], [campaignId#22], [_w1#29L DESC NULLS LAST]
      +- Aggregate [campaignId#22, channelIid#23], [campaignId#22, channelIid#23, count(eventId#1) AS _w1#29L]
         +- Project [eventId#1, attributes#4[campaign_id] AS campaignId#22, attributes#4[channel_id] AS channelIid#23]
            +- Filter ((((isnotnull(eventTimeDate#5) AND isnotnull(eventType#2)) AND (eventTimeDate#5 >= 18536)) AND (eventTimeDate#5 <= 18566)) AND (eventType#2 = app_open))
               +- Relation[userId#0,eventId#1,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] parquet
== Physical Plan ==
*(4) HashAggregate(keys=[campaignId#22, channelIiD#24], functions=[], output=[campaignId#22, channelIiD#24])
+- *(4) HashAggregate(keys=[campaignId#22, channelIiD#24], functions=[], output=[campaignId#22, channelIiD#24])
   +- *(4) Project [campaignId#22, channelIiD#24]
      +- Window [first(channelIid#23, false) windowspecdefinition(campaignId#22, _w1#29L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#24], [campaignId#22], [_w1#29L DESC NULLS LAST]
         +- *(3) Sort [campaignId#22 ASC NULLS FIRST, _w1#29L DESC NULLS LAST], false, 0
            +- Exchange hashpartitioning(campaignId#22, 200), ENSURE_REQUIREMENTS, [id=#50]
               +- *(2) HashAggregate(keys=[campaignId#22, channelIid#23], functions=[count(eventId#1)], output=[campaignId#22, channelIid#23, _w1#29L])
                  +- Exchange hashpartitioning(campaignId#22, channelIid#23, 200), ENSURE_REQUIREMENTS, [id=#46]
                     +- *(1) HashAggregate(keys=[campaignId#22, channelIid#23], functions=[partial_count(eventId#1)], output=[campaignId#22, channelIid#23, count#33L])
                        +- *(1) Project [eventId#1, attributes#4[campaign_id] AS campaignId#22, attributes#4[channel_id] AS channelIid#23]
                           +- *(1) Filter (isnotnull(eventType#2) AND (eventType#2 = app_open))
                              +- FileScan parquet [eventId#1,eventType#2,attributes#4,eventTimeDate#5] Batched: false, DataFilters: [isnotnull(eventType#2), (eventType#2 = app_open)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/in/par..., PartitionFilters: [isnotnull(eventTimeDate#5), (eventTimeDate#5 >= 18536), (eventTimeDate#5 <= 18566)], PushedFilters: [IsNotNull(eventType), EqualTo(eventType,app_open)], ReadSchema: struct<eventId:string,eventType:string,attributes:map<string,string>>
```

### CSV input:
```
== Parsed Logical Plan ==
'Distinct
+- 'Aggregate ['campaignId, 'channelIid], ['campaignId, 'FIRST('channelIid) windowspecdefinition('campaignId, 'COUNT('eventId) DESC NULLS LAST, unspecifiedframe$()) AS channelIiD#67]
   +- 'SubqueryAlias __auto_generated_subquery_name
      +- 'Project ['eventId, 'attributes.campaign_id AS campaignId#65, 'attributes.channel_id AS channelIid#66]
         +- 'Filter ('eventType = app_open)
            +- 'UnresolvedRelation [events], [], false
== Analyzed Logical Plan ==
campaignId: string, channelIiD: string
Distinct
+- Project [campaignId#65, channelIiD#67]
   +- Project [campaignId#65, channelIid#66, _w1#72L, channelIiD#67, channelIiD#67]
      +- Window [first(channelIid#66, false) windowspecdefinition(campaignId#65, _w1#72L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#67], [campaignId#65], [_w1#72L DESC NULLS LAST]
         +- Aggregate [campaignId#65, channelIid#66], [campaignId#65, channelIid#66, count(eventId#17) AS _w1#72L]
            +- SubqueryAlias __auto_generated_subquery_name
               +- Project [eventId#17, attributes#44[campaign_id] AS campaignId#65, attributes#44[channel_id] AS channelIid#66]
                  +- Filter (eventType#18 = app_open)
                     +- SubqueryAlias events
                        +- Filter ((eventTimeDate#58 >= cast(2020-10-01 as date)) AND (eventTimeDate#58 <= cast(2020-10-31 as date)))
                           +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, to_date('eventTime, Some(yyyy-mm-dd)) AS eventTimeDate#58]
                              +- Project [userId#16, eventId#17, eventType#18, eventTime#26, from_json(MapType(StringType,StringType,true), attributes#38, Some(Europe/Warsaw)) AS attributes#44]
                                 +- Project [userId#16, eventId#17, eventType#18, eventTime#26, regexp_replace(attributes#32, "", ', 1) AS attributes#38]
                                    +- Project [userId#16, eventId#17, eventType#18, eventTime#26, regexp_extract(attributes#20, "?(\{.+\})"?, 1) AS attributes#32]
                                       +- Project [userId#16, eventId#17, eventType#18, cast(eventTime#19 as timestamp) AS eventTime#26, attributes#20]
                                          +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
== Optimized Logical Plan ==
Aggregate [campaignId#65, channelIiD#67], [campaignId#65, channelIiD#67]
+- Project [campaignId#65, channelIiD#67]
   +- Window [first(channelIid#66, false) windowspecdefinition(campaignId#65, _w1#72L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#67], [campaignId#65], [_w1#72L DESC NULLS LAST]
      +- Aggregate [campaignId#65, channelIid#66], [campaignId#65, channelIid#66, count(eventId#17) AS _w1#72L]
         +- Project [eventId#17, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw))[campaign_id] AS campaignId#65, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw))[channel_id] AS channelIid#66]
            +- Filter (((isnotnull(eventType#18) AND (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) >= 18536)) AND (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) <= 18566)) AND (eventType#18 = app_open))
               +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
== Physical Plan ==
*(5) HashAggregate(keys=[campaignId#65, channelIiD#67], functions=[], output=[campaignId#65, channelIiD#67])
+- *(5) HashAggregate(keys=[campaignId#65, channelIiD#67], functions=[], output=[campaignId#65, channelIiD#67])
   +- *(5) Project [campaignId#65, channelIiD#67]
      +- Window [first(channelIid#66, false) windowspecdefinition(campaignId#65, _w1#72L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#67], [campaignId#65], [_w1#72L DESC NULLS LAST]
         +- *(4) Sort [campaignId#65 ASC NULLS FIRST, _w1#72L DESC NULLS LAST], false, 0
            +- Exchange hashpartitioning(campaignId#65, 200), ENSURE_REQUIREMENTS, [id=#71]
               +- *(3) HashAggregate(keys=[campaignId#65, channelIid#66], functions=[count(eventId#17)], output=[campaignId#65, channelIid#66, _w1#72L])
                  +- Exchange hashpartitioning(campaignId#65, channelIid#66, 200), ENSURE_REQUIREMENTS, [id=#67]
                     +- *(2) HashAggregate(keys=[campaignId#65, channelIid#66], functions=[partial_count(eventId#17)], output=[campaignId#65, channelIid#66, count#76L])
                        +- Project [eventId#17, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw))[campaign_id] AS campaignId#65, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw))[channel_id] AS channelIid#66]
                           +- *(1) Filter (((isnotnull(eventType#18) AND (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) >= 18536)) AND (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) <= 18566)) AND (eventType#18 = app_open))
                              +- FileScan csv [eventId#17,eventType#18,eventTime#19,attributes#20] Batched: false, DataFilters: [isnotnull(eventType#18), (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Eu..., Format: CSV, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/mobile..., PartitionFilters: [], PushedFilters: [IsNotNull(eventType), EqualTo(eventType,app_open)], ReadSchema: struct<eventId:string,eventType:string,eventTime:string,attributes:string>
```

## Top campaigns by revenue at 2020-11-11

### Parquet partitioned by date input:
```
== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'SUM('billingCost) AS revenue#103]
         +- 'Filter ('isConfirmed = true)
            +- 'UnresolvedRelation [purchasesAttribution], [], false

== Analyzed Logical Plan ==
campaignId: string, revenue: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#103 DESC NULLS LAST], true
      +- Aggregate [campaignId#48], [campaignId#48, sum(billingCost#14) AS revenue#103]
         +- Filter (isConfirmed#15 = true)
            +- SubqueryAlias purchasesattribution
               +- Project [purchaseId#70, purchaseTime#13, billingCost#14, isConfirmed#15, sessionId#31, campaignId#48, channelIid#59]
                  +- Project [purchaseId#70, userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31, campaignId#48, channelIid#59, purchaseTime#13, billingCost#14, isConfirmed#15, purchaseTimeDate#16]
                     +- Join Inner, (purchaseId#70 = purchaseId#12)
                        :- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31, campaignId#48, channelIid#59, attributes#4[purchase_id] AS purchaseId#70]
                        :  +- Filter (eventType#2 = purchase)
                        :     +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31, campaignId#48, channelIid#59]
                        :        +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31, campaignId#48, channelIid#59, channelIid#59]
                        :           +- Window [last(attributes#4[channel_id], true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIid#59], [userId#0], [eventTime#3 ASC NULLS FIRST]
                        :              +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31, campaignId#48]
                        :                 +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31, campaignId#48]
                        :                    +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31, campaignId#48, campaignId#48]
                        :                       +- Window [last(attributes#4[campaign_id], true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#48], [userId#0], [eventTime#3 ASC NULLS FIRST]
                        :                          +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31]
                        :                             +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, sessionId#31]
                        :                                +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, tmpSessionId#22, sessionId#31]
                        :                                   +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, tmpSessionId#22, sessionId#31, sessionId#31]
                        :                                      +- Window [last(tmpSessionId#22, true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS sessionId#31], [userId#0], [eventTime#3 ASC NULLS FIRST]
                        :                                         +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, tmpSessionId#22]
                        :                                            +- Project [userId#0, eventId#1, eventType#2, eventTime#3, attributes#4, eventTimeDate#5, CASE WHEN (eventType#2 = app_open) THEN eventId#1 END AS tmpSessionId#22]
                        :                                               +- Filter (eventTimeDate#5 = cast(2020-11-11 as date))
                        :                                                  +- Relation[userId#0,eventId#1,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] parquet
                        +- Filter (purchaseTimeDate#16 = cast(2020-11-11 as date))
                           +- Relation[purchaseId#12,purchaseTime#13,billingCost#14,isConfirmed#15,purchaseTimeDate#16] parquet

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#103 DESC NULLS LAST], true
      +- Aggregate [campaignId#48], [campaignId#48, sum(billingCost#14) AS revenue#103]
         +- Project [billingCost#14, campaignId#48]
            +- Join Inner, (purchaseId#70 = purchaseId#12)
               :- Project [campaignId#48, attributes#4[purchase_id] AS purchaseId#70]
               :  +- Filter ((isnotnull(eventType#2) AND (eventType#2 = purchase)) AND isnotnull(attributes#4[purchase_id]))
               :     +- Window [last(attributes#4[campaign_id], true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#48], [userId#0], [eventTime#3 ASC NULLS FIRST]
               :        +- Project [userId#0, eventType#2, eventTime#3, attributes#4]
               :           +- Filter (isnotnull(eventTimeDate#5) AND (eventTimeDate#5 = 18577))
               :              +- Relation[userId#0,eventId#1,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] parquet
               +- Project [purchaseId#12, billingCost#14]
                  +- Filter ((((isnotnull(purchaseTimeDate#16) AND isnotnull(isConfirmed#15)) AND (purchaseTimeDate#16 = 18577)) AND (isConfirmed#15 = true)) AND isnotnull(purchaseId#12))
                     +- Relation[purchaseId#12,purchaseTime#13,billingCost#14,isConfirmed#15,purchaseTimeDate#16] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#103 DESC NULLS LAST], output=[campaignId#48,revenue#103])
+- *(8) HashAggregate(keys=[campaignId#48], functions=[sum(billingCost#14)], output=[campaignId#48, revenue#103])
   +- Exchange hashpartitioning(campaignId#48, 200), ENSURE_REQUIREMENTS, [id=#106]
      +- *(7) HashAggregate(keys=[campaignId#48], functions=[partial_sum(billingCost#14)], output=[campaignId#48, sum#109])
         +- *(7) Project [billingCost#14, campaignId#48]
            +- *(7) SortMergeJoin [purchaseId#70], [purchaseId#12], Inner
               :- *(4) Sort [purchaseId#70 ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(purchaseId#70, 200), ENSURE_REQUIREMENTS, [id=#87]
               :     +- *(3) Project [campaignId#48, attributes#4[purchase_id] AS purchaseId#70]
               :        +- *(3) Filter ((isnotnull(eventType#2) AND (eventType#2 = purchase)) AND isnotnull(attributes#4[purchase_id]))
               :           +- Window [last(attributes#4[campaign_id], true) windowspecdefinition(userId#0, eventTime#3 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#48], [userId#0], [eventTime#3 ASC NULLS FIRST]
               :              +- *(2) Sort [userId#0 ASC NULLS FIRST, eventTime#3 ASC NULLS FIRST], false, 0
               :                 +- Exchange hashpartitioning(userId#0, 200), ENSURE_REQUIREMENTS, [id=#78]
               :                    +- *(1) Project [userId#0, eventType#2, eventTime#3, attributes#4]
               :                       +- FileScan parquet [userId#0,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] Batched: false, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/mobile..., PartitionFilters: [isnotnull(eventTimeDate#5), (eventTimeDate#5 = 18577)], PushedFilters: [], ReadSchema: struct<userId:string,eventType:string,eventTime:timestamp,attributes:map<string,string>>
               +- *(6) Sort [purchaseId#12 ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(purchaseId#12, 200), ENSURE_REQUIREMENTS, [id=#97]
                     +- *(5) Project [purchaseId#12, billingCost#14]
                        +- *(5) Filter ((isnotnull(isConfirmed#15) AND (isConfirmed#15 = true)) AND isnotnull(purchaseId#12))
                           +- *(5) ColumnarToRow
                              +- FileScan parquet [purchaseId#12,billingCost#14,isConfirmed#15,purchaseTimeDate#16] Batched: true, DataFilters: [isnotnull(isConfirmed#15), (isConfirmed#15 = true), isnotnull(purchaseId#12)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/user_p..., PartitionFilters: [isnotnull(purchaseTimeDate#16), (purchaseTimeDate#16 = 18577)], PushedFilters: [IsNotNull(isConfirmed), EqualTo(isConfirmed,true), IsNotNull(purchaseId)], ReadSchema: struct<purchaseId:string,billingCost:double,isConfirmed:boolean>
```

### CSV input:
```
== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'SUM('billingCost) AS revenue#152]
         +- 'Filter ('isConfirmed = true)
            +- 'UnresolvedRelation [purchasesAttribution], [], false

== Analyzed Logical Plan ==
campaignId: string, revenue: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#152 DESC NULLS LAST], true
      +- Aggregate [campaignId#97], [campaignId#97, sum(billingCost#52) AS revenue#152]
         +- Filter (isConfirmed#53 = true)
            +- SubqueryAlias purchasesattribution
               +- Project [purchaseId#119, purchaseTime#51, billingCost#52, isConfirmed#53, sessionId#80, campaignId#97, channelIid#108]
                  +- Project [purchaseId#119, userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, channelIid#108, purchaseTime#51, billingCost#52, isConfirmed#53, purchaseTimeDate#65]
                     +- Join Inner, (purchaseId#119 = purchaseId#50)
                        :- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, channelIid#108, attributes#44[purchase_id] AS purchaseId#119]
                        :  +- Filter (eventType#18 = purchase)
                        :     +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, channelIid#108]
                        :        +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, channelIid#108, channelIid#108]
                        :           +- Window [last(attributes#44[channel_id], true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIid#108], [userId#16], [eventTime#26 ASC NULLS FIRST]
                        :              +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97]
                        :                 +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97]
                        :                    +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80, campaignId#97, campaignId#97]
                        :                       +- Window [last(attributes#44[campaign_id], true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#97], [userId#16], [eventTime#26 ASC NULLS FIRST]
                        :                          +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80]
                        :                             +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, sessionId#80]
                        :                                +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, tmpSessionId#71, sessionId#80]
                        :                                   +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, tmpSessionId#71, sessionId#80, sessionId#80]
                        :                                      +- Window [last(tmpSessionId#71, true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS sessionId#80], [userId#16], [eventTime#26 ASC NULLS FIRST]
                        :                                         +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, tmpSessionId#71]
                        :                                            +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, eventTimeDate#58, CASE WHEN (eventType#18 = app_open) THEN eventId#17 END AS tmpSessionId#71]
                        :                                               +- Filter (eventTimeDate#58 = cast(2020-11-11 as date))
                        :                                                  +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, to_date('eventTime, Some(yyyy-mm-dd)) AS eventTimeDate#58]
                        :                                                     +- Project [userId#16, eventId#17, eventType#18, eventTime#26, from_json(MapType(StringType,StringType,true), attributes#38, Some(Europe/Warsaw)) AS attributes#44]
                        :                                                        +- Project [userId#16, eventId#17, eventType#18, eventTime#26, regexp_replace(attributes#32, "", ', 1) AS attributes#38]
                        :                                                           +- Project [userId#16, eventId#17, eventType#18, eventTime#26, regexp_extract(attributes#20, "?(\{.+\})"?, 1) AS attributes#32]
                        :                                                              +- Project [userId#16, eventId#17, eventType#18, cast(eventTime#19 as timestamp) AS eventTime#26, attributes#20]
                        :                                                                 +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
                        +- Filter (purchaseTimeDate#65 = cast(2020-11-11 as date))
                           +- Project [purchaseId#50, purchaseTime#51, billingCost#52, isConfirmed#53, to_date('purchaseTime, Some(yyyy-mm-dd)) AS purchaseTimeDate#65]
                              +- Relation[purchaseId#50,purchaseTime#51,billingCost#52,isConfirmed#53] csv

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#152 DESC NULLS LAST], true
      +- Aggregate [campaignId#97], [campaignId#97, sum(billingCost#52) AS revenue#152]
         +- Project [billingCost#52, campaignId#97]
            +- Join Inner, (purchaseId#119 = purchaseId#50)
               :- Project [campaignId#97, attributes#44[purchase_id] AS purchaseId#119]
               :  +- Filter ((isnotnull(eventType#18) AND (eventType#18 = purchase)) AND isnotnull(attributes#44[purchase_id]))
               :     +- Window [last(attributes#44[campaign_id], true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#97], [userId#16], [eventTime#26 ASC NULLS FIRST]
               :        +- Project [userId#16, eventType#18, cast(eventTime#19 as timestamp) AS eventTime#26, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw)) AS attributes#44]
               :           +- Filter (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) = 18577)
               :              +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
               +- Project [purchaseId#50, billingCost#52]
                  +- Filter (((isnotnull(isConfirmed#53) AND (cast(gettimestamp(purchaseTime#51, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) = 18577)) AND (isConfirmed#53 = true)) AND isnotnull(purchaseId#50))
                     +- Relation[purchaseId#50,purchaseTime#51,billingCost#52,isConfirmed#53] csv

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#152 DESC NULLS LAST], output=[campaignId#97,revenue#152])
+- *(8) HashAggregate(keys=[campaignId#97], functions=[sum(billingCost#52)], output=[campaignId#97, revenue#152])
   +- Exchange hashpartitioning(campaignId#97, 200), ENSURE_REQUIREMENTS, [id=#116]
      +- *(7) HashAggregate(keys=[campaignId#97], functions=[partial_sum(billingCost#52)], output=[campaignId#97, sum#158])
         +- *(7) Project [billingCost#52, campaignId#97]
            +- *(7) SortMergeJoin [purchaseId#119], [purchaseId#50], Inner
               :- *(4) Sort [purchaseId#119 ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(purchaseId#119, 200), ENSURE_REQUIREMENTS, [id=#98]
               :     +- *(3) Project [campaignId#97, attributes#44[purchase_id] AS purchaseId#119]
               :        +- *(3) Filter ((isnotnull(eventType#18) AND (eventType#18 = purchase)) AND isnotnull(attributes#44[purchase_id]))
               :           +- Window [last(attributes#44[campaign_id], true) windowspecdefinition(userId#16, eventTime#26 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS campaignId#97], [userId#16], [eventTime#26 ASC NULLS FIRST]
               :              +- *(2) Sort [userId#16 ASC NULLS FIRST, eventTime#26 ASC NULLS FIRST], false, 0
               :                 +- Exchange hashpartitioning(userId#16, 200), ENSURE_REQUIREMENTS, [id=#89]
               :                    +- Project [userId#16, eventType#18, cast(eventTime#19 as timestamp) AS eventTime#26, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw)) AS attributes#44]
               :                       +- *(1) Filter (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) = 18577)
               :                          +- FileScan csv [userId#16,eventType#18,eventTime#19,attributes#20] Batched: false, DataFilters: [(cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as d..., Format: CSV, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/mobile..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<userId:string,eventType:string,eventTime:string,attributes:string>
               +- *(6) Sort [purchaseId#50 ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(purchaseId#50, 200), ENSURE_REQUIREMENTS, [id=#107]
                     +- *(5) Project [purchaseId#50, billingCost#52]
                        +- *(5) Filter (((isnotnull(isConfirmed#53) AND (cast(gettimestamp(purchaseTime#51, yyyy-mm-dd, Some(Europe/Warsaw), false) as date) = 18577)) AND (isConfirmed#53 = true)) AND isnotnull(purchaseId#50))
                           +- FileScan csv [purchaseId#50,purchaseTime#51,billingCost#52,isConfirmed#53] Batched: false, DataFilters: [isnotnull(isConfirmed#53), (cast(gettimestamp(purchaseTime#51, yyyy-mm-dd, Some(Europe/Warsaw), ..., Format: CSV, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/user_p..., PartitionFilters: [], PushedFilters: [IsNotNull(isConfirmed), EqualTo(isConfirmed,true), IsNotNull(purchaseId)], ReadSchema: struct<purchaseId:string,purchaseTime:timestamp,billingCost:double,isConfirmed:boolean>
```

## Most popular channels for each campaign at 2020-11-11

### Parquet partitioned by date input:
```
== Parsed Logical Plan ==
'Distinct
+- 'Aggregate ['campaignId, 'channelIid], ['campaignId, 'FIRST('channelIid) windowspecdefinition('campaignId, 'COUNT('eventId) DESC NULLS LAST, unspecifiedframe$()) AS channelIiD#24]
   +- 'SubqueryAlias __auto_generated_subquery_name
      +- 'Project ['eventId, 'attributes.campaign_id AS campaignId#22, 'attributes.channel_id AS channelIid#23]
         +- 'Filter ('eventType = app_open)
            +- 'UnresolvedRelation [events], [], false
== Analyzed Logical Plan ==
campaignId: string, channelIiD: string
Distinct
+- Project [campaignId#22, channelIiD#24]
   +- Project [campaignId#22, channelIid#23, _w1#29L, channelIiD#24, channelIiD#24]
      +- Window [first(channelIid#23, false) windowspecdefinition(campaignId#22, _w1#29L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#24], [campaignId#22], [_w1#29L DESC NULLS LAST]
         +- Aggregate [campaignId#22, channelIid#23], [campaignId#22, channelIid#23, count(eventId#1) AS _w1#29L]
            +- SubqueryAlias __auto_generated_subquery_name
               +- Project [eventId#1, attributes#4[campaign_id] AS campaignId#22, attributes#4[channel_id] AS channelIid#23]
                  +- Filter (eventType#2 = app_open)
                     +- SubqueryAlias events
                        +- Filter (eventTimeDate#5 = cast(2020-11-11 as date))
                           +- Relation[userId#0,eventId#1,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] parquet
== Optimized Logical Plan ==
Aggregate [campaignId#22, channelIiD#24], [campaignId#22, channelIiD#24]
+- Project [campaignId#22, channelIiD#24]
   +- Window [first(channelIid#23, false) windowspecdefinition(campaignId#22, _w1#29L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#24], [campaignId#22], [_w1#29L DESC NULLS LAST]
      +- Aggregate [campaignId#22, channelIid#23], [campaignId#22, channelIid#23, count(eventId#1) AS _w1#29L]
         +- Project [eventId#1, attributes#4[campaign_id] AS campaignId#22, attributes#4[channel_id] AS channelIid#23]
            +- Filter (((isnotnull(eventTimeDate#5) AND isnotnull(eventType#2)) AND (eventTimeDate#5 = 18577)) AND (eventType#2 = app_open))
               +- Relation[userId#0,eventId#1,eventType#2,eventTime#3,attributes#4,eventTimeDate#5] parquet
== Physical Plan ==
*(4) HashAggregate(keys=[campaignId#22, channelIiD#24], functions=[], output=[campaignId#22, channelIiD#24])
+- *(4) HashAggregate(keys=[campaignId#22, channelIiD#24], functions=[], output=[campaignId#22, channelIiD#24])
   +- *(4) Project [campaignId#22, channelIiD#24]
      +- Window [first(channelIid#23, false) windowspecdefinition(campaignId#22, _w1#29L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#24], [campaignId#22], [_w1#29L DESC NULLS LAST]
         +- *(3) Sort [campaignId#22 ASC NULLS FIRST, _w1#29L DESC NULLS LAST], false, 0
            +- Exchange hashpartitioning(campaignId#22, 200), ENSURE_REQUIREMENTS, [id=#50]
               +- *(2) HashAggregate(keys=[campaignId#22, channelIid#23], functions=[count(eventId#1)], output=[campaignId#22, channelIid#23, _w1#29L])
                  +- Exchange hashpartitioning(campaignId#22, channelIid#23, 200), ENSURE_REQUIREMENTS, [id=#46]
                     +- *(1) HashAggregate(keys=[campaignId#22, channelIid#23], functions=[partial_count(eventId#1)], output=[campaignId#22, channelIid#23, count#33L])
                        +- *(1) Project [eventId#1, attributes#4[campaign_id] AS campaignId#22, attributes#4[channel_id] AS channelIid#23]
                           +- *(1) Filter (isnotnull(eventType#2) AND (eventType#2 = app_open))
                              +- FileScan parquet [eventId#1,eventType#2,attributes#4,eventTimeDate#5] Batched: false, DataFilters: [isnotnull(eventType#2), (eventType#2 = app_open)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/in/par..., PartitionFilters: [isnotnull(eventTimeDate#5), (eventTimeDate#5 = 18577)], PushedFilters: [IsNotNull(eventType), EqualTo(eventType,app_open)], ReadSchema: struct<eventId:string,eventType:string,attributes:map<string,string>>
```

### CSV input:
```
== Parsed Logical Plan ==
'Distinct
+- 'Aggregate ['campaignId, 'channelIid], ['campaignId, 'FIRST('channelIid) windowspecdefinition('campaignId, 'COUNT('eventId) DESC NULLS LAST, unspecifiedframe$()) AS channelIiD#67]
   +- 'SubqueryAlias __auto_generated_subquery_name
      +- 'Project ['eventId, 'attributes.campaign_id AS campaignId#65, 'attributes.channel_id AS channelIid#66]
         +- 'Filter ('eventType = app_open)
            +- 'UnresolvedRelation [events], [], false
== Analyzed Logical Plan ==
campaignId: string, channelIiD: string
Distinct
+- Project [campaignId#65, channelIiD#67]
   +- Project [campaignId#65, channelIid#66, _w1#72L, channelIiD#67, channelIiD#67]
      +- Window [first(channelIid#66, false) windowspecdefinition(campaignId#65, _w1#72L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#67], [campaignId#65], [_w1#72L DESC NULLS LAST]
         +- Aggregate [campaignId#65, channelIid#66], [campaignId#65, channelIid#66, count(eventId#17) AS _w1#72L]
            +- SubqueryAlias __auto_generated_subquery_name
               +- Project [eventId#17, attributes#44[campaign_id] AS campaignId#65, attributes#44[channel_id] AS channelIid#66]
                  +- Filter (eventType#18 = app_open)
                     +- SubqueryAlias events
                        +- Filter (eventTimeDate#58 = cast(2020-11-11 as date))
                           +- Project [userId#16, eventId#17, eventType#18, eventTime#26, attributes#44, to_date('eventTime, Some(yyyy-mm-dd)) AS eventTimeDate#58]
                              +- Project [userId#16, eventId#17, eventType#18, eventTime#26, from_json(MapType(StringType,StringType,true), attributes#38, Some(Europe/Warsaw)) AS attributes#44]
                                 +- Project [userId#16, eventId#17, eventType#18, eventTime#26, regexp_replace(attributes#32, "", ', 1) AS attributes#38]
                                    +- Project [userId#16, eventId#17, eventType#18, eventTime#26, regexp_extract(attributes#20, "?(\{.+\})"?, 1) AS attributes#32]
                                       +- Project [userId#16, eventId#17, eventType#18, cast(eventTime#19 as timestamp) AS eventTime#26, attributes#20]
                                          +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
== Optimized Logical Plan ==
Aggregate [campaignId#65, channelIiD#67], [campaignId#65, channelIiD#67]
+- Project [campaignId#65, channelIiD#67]
   +- Window [first(channelIid#66, false) windowspecdefinition(campaignId#65, _w1#72L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#67], [campaignId#65], [_w1#72L DESC NULLS LAST]
      +- Aggregate [campaignId#65, channelIid#66], [campaignId#65, channelIid#66, count(eventId#17) AS _w1#72L]
         +- Project [eventId#17, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw))[campaign_id] AS campaignId#65, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw))[channel_id] AS channelIid#66]
            +- Filter ((isnotnull(eventType#18) AND (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) = 18577)) AND (eventType#18 = app_open))
               +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
== Physical Plan ==
*(5) HashAggregate(keys=[campaignId#65, channelIiD#67], functions=[], output=[campaignId#65, channelIiD#67])
+- *(5) HashAggregate(keys=[campaignId#65, channelIiD#67], functions=[], output=[campaignId#65, channelIiD#67])
   +- *(5) Project [campaignId#65, channelIiD#67]
      +- Window [first(channelIid#66, false) windowspecdefinition(campaignId#65, _w1#72L DESC NULLS LAST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS channelIiD#67], [campaignId#65], [_w1#72L DESC NULLS LAST]
         +- *(4) Sort [campaignId#65 ASC NULLS FIRST, _w1#72L DESC NULLS LAST], false, 0
            +- Exchange hashpartitioning(campaignId#65, 200), ENSURE_REQUIREMENTS, [id=#71]
               +- *(3) HashAggregate(keys=[campaignId#65, channelIid#66], functions=[count(eventId#17)], output=[campaignId#65, channelIid#66, _w1#72L])
                  +- Exchange hashpartitioning(campaignId#65, channelIid#66, 200), ENSURE_REQUIREMENTS, [id=#67]
                     +- *(2) HashAggregate(keys=[campaignId#65, channelIid#66], functions=[partial_count(eventId#17)], output=[campaignId#65, channelIid#66, count#76L])
                        +- Project [eventId#17, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw))[campaign_id] AS campaignId#65, from_json(MapType(StringType,StringType,true), regexp_replace(regexp_extract(attributes#20, "?(\{.+\})"?, 1), "", ', 1), Some(Europe/Warsaw))[channel_id] AS channelIid#66]
                           +- *(1) Filter ((isnotnull(eventType#18) AND (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Europe/Warsaw), false) as date) = 18577)) AND (eventType#18 = app_open))
                              +- FileScan csv [eventId#17,eventType#18,eventTime#19,attributes#20] Batched: false, DataFilters: [isnotnull(eventType#18), (cast(gettimestamp(cast(eventTime#19 as timestamp), yyyy-mm-dd, Some(Eu..., Format: CSV, Location: InMemoryFileIndex[file:/Users/bbaj/IdeaProjects/GridUMarketingAnalytics/src/main/resources/mobile..., PartitionFilters: [], PushedFilters: [IsNotNull(eventType), EqualTo(eventType,app_open)], ReadSchema: struct<eventId:string,eventType:string,eventTime:string,attributes:string>
```