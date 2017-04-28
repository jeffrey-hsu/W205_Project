def backfill(df):
    my_cols = df.columns
    for x in my_cols:
        if x not in ("GVKEY, public_date"):
            df_na = df.na.fill(-1)
            set_lag = df_na.withColumn('id_lag', lag(x,1,-1)\
                            .over(Window.partitionBy('GVKEY')\
                            .orderBy('public_date')))
            switch = set_lag.withColumn(x+'_change',        # flag for whether the original column has data
                                    ((set_lag[x] != set_lag['id_lag']) &
                                     (set_lag[x] != -1)).cast('integer'))
            switch_sess = switch.withColumn('sub_session',  # grouping to determine what gets filled by what
                sum(x+'_change')
                .over(
                    Window.partitionBy('GVKEY')
                    .orderBy('public_date')
                    .rowsBetween(-sys.maxsize, 0))
            )
            fid = switch_sess.withColumn('nn_id',   # fill values based off sub_session
                                   first(x)\
                                   .over(Window.partitionBy('GVKEY', 'sub_session')\
                                   .orderBy('public_date')))
            fid_na = fid.replace(-1, 'null')    # replace -1 with null
            df = fid_na.drop('id').drop('id_lag')\
                                  .drop(x+'_change')\
                                  .drop('sub_session')\
                                  .drop(x)\
                                  .withColumnRenamed('nn_id', x+'_bfilled')
    return df


def forwardfill(df):
    my_cols = df.columns
    for x in my_cols:
        if x not in ("GVKEY, public_date"):
            df_na = df.na.fill(-1)
            set_lag = df_na.withColumn('id_lag', lag(x,-1,-1)\
                            .over(Window.partitionBy('GVKEY')\
                            .orderBy('public_date')))
            switch = set_lag.withColumn(x+'_change',
                                    ((set_lag[x] != set_lag['id_lag']) &
                                     (set_lag[x] != -1)).cast('integer'))
            switch_sess = switch.withColumn('sub_session',
                sum(x+'_change')
                .over(
                    Window.partitionBy('GVKEY')
                    .orderBy('public_date')
                    .rowsBetween(0, sys.maxsize))
            )
            fid = switch_sess.withColumn('nn_id',
                                   first(x)\
                                   .over(Window.partitionBy('GVKEY', 'sub_session')\
                                   .orderBy(desc('public_date'))))
            fid_na = fid.replace(-1, 'null')
            df = fid_na.drop('id').drop('id_lag')\
                                  .drop(x+'_change')\
                                  .drop('sub_session')\
                                  .drop(x)\
                                  .withColumnRenamed('nn_id', x+'_ffilled')
    return df

######################################################
## BELOW IS FOR FILLING SINGLE COLUMNS (HARD-CODED) ##
## SAVING IN CASE I NEED IT FOR REFERENCE LATER     ##
######################################################

# def back_fill(df):
#     df_na = df.na.fill(-1)
#     set_lag = df_na.withColumn('id_lag', lag('Returns',1,-1).over(Window.partitionBy('GVKEY').orderBy('public_date')))
#     switch = set_lag.withColumn('Returns_change',
#                             ((set_lag['Returns'] != set_lag['id_lag']) &
#                              (set_lag['Returns'] != -1)).cast('integer'))
#     switch_sess = switch.withColumn('sub_session',
#         sum("Returns_change")
#         .over(
#             Window.partitionBy("GVKEY")
#             .orderBy("public_date")
#             .rowsBetween(-sys.maxsize, 0))
#     )
#     fid = switch_sess.withColumn('nn_id',
#                            first('Returns')\
#                            .over(Window.partitionBy('GVKEY', 'sub_session')\
#                            .orderBy('public_date')))
#     fid_na = fid.replace(-1, 'null')
#     ff = fid_na.drop('id').drop('id_lag')\
#                           .drop('Returns_change')\
#                           .drop('sub_session')\
#                           .drop('Returns')\
#                           .withColumnRenamed('nn_id', 'Returns')
#     return ff
#
# def forward_fill(df):
#     df_na = df.na.fill(-1)
#     set_lag = df_na.withColumn('id_lag', lag('Returns',-1,-1).over(Window.partitionBy('GVKEY').orderBy('public_date')))
#     switch = set_lag.withColumn('Returns_change',
#                             ((set_lag['Returns'] != set_lag['id_lag']) &
#                              (set_lag['Returns'] != -1)).cast('integer'))
#     switch_sess = switch.withColumn('sub_session',
#         sum("Returns_change")
#         .over(
#             Window.partitionBy("GVKEY")
#             .orderBy("public_date")
#             .rowsBetween(0, sys.maxsize))
#     )
#     fid = switch_sess.withColumn('nn_id',
#                            first('Returns')\
#                            .over(Window.partitionBy('GVKEY', 'sub_session')\
#                            .orderBy(desc('public_date'))))
#     fid_na = fid.replace(-1, 'null')
#     ff = fid_na.drop('id').drop('id_lag')\
#                           .drop('Returns_change')\
#                           .drop('sub_session')\
#                           .drop('Returns')\
#                           .withColumnRenamed('nn_id', 'Returns')
#     return ff
