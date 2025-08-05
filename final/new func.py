{
    "part_fields": "oid_clsno,oid_instid",
    "rnk_fields": "_timestamp desc",
    "sel_fields": "oid_clsno,oid_instid,_operation,aAccidentRoleOtherDetails,aUniqueId,aDrivingWhileSuspended,aType,aDriversLicenseExpiry,aIsInjured,aDriversLicenseUnknown,aUnlicensed,aDriversLicenseNumber,myPoliceFinding_clsno,myPoliceFinding_instid,myDriversLicenseState_clsno,myDriversLicenseState_instid,myDriversLicenceType_clsno,myDriversLicenceType_instid,myClaim_clsno,myClaim_instid,myAccident_clsno,myAccident_instid,myParticipant_clsno,myParticipant_instid",
    "filter_con": "_operation != 'D'",
    "active_field": "_timestamp",
    "days_purge": 2          # <–– NEW KEY → use last 2 days of data only
}


def selective_df_hub(spark, db_table, column_list):
    """
    Fetch the latest active record per business key from a hub table.

    If `days_purge` key is present in column_list, read only recent records
    based on that number of days, otherwise read full table like the custom logic.
    """

    # If ALL columns requested
    if isinstance(column_list, str) and column_list.upper() == "ALL":
        return spark.read.table(db_table)

    # If a list of columns requested
    if isinstance(column_list, list):
        return spark.read.table(db_table).select(*column_list)

    # Otherwise, dict based logic
    if isinstance(column_list, dict):

        part_fields = column_list['part_fields']
        rnk_fields = column_list['rnk_fields']
        sel_fields = column_list['sel_fields']
        filter_con = column_list.get('filter_con', '1=1')
        active_field = column_list['active_field']

        # Check whether purge logic is required
        days_purge = column_list.get("days_purge", None)

        # Build appropriate WHERE condition
        if days_purge:
            base_filter = f"{active_field} >= date_sub(current_date(), {int(days_purge)})"
        else:
            base_filter = "1=1"    # No date restriction

        sql_query = f"""
        select * from (
            select *, rank() over(partition by {part_fields} order by {rnk_fields}) as rnk
            from {db_table}
            where {base_filter}
        ) hub
        where hub.rnk = 1 and hub.{filter_con}
        """

        df = spark.sql(sql_query).select(*[c.strip() for c in sel_fields.split(",")])
        return df
