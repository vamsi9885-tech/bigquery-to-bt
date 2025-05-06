package com.hpe.batch.driver.facts.channel_partner

import main.scala.com.hpe.config_sls_ops._
import main.scala.com.hpe.utils_sls_ops.Utilities
import org.apache.log4j.Logger
import org.apache.spark.sql.AnalysisException
import org.apache.spark.storage.StorageLevel
import java.net.ConnectException

object ChannelPartnerSFPOSLoadINC extends App {
  //**************************Driver properties******************************//

  val configObject: ConfigObject = SetUpConfiguration.setup(0L, "")
  val spark = configObject.spark
  val auditObj: AuditLoadObject = new AuditLoadObject("", "", "", "", "", "", "", 0, 0, 0, "", "", "", "", "")
  val propertiesFilePath = String.valueOf(args(0).trim())
  val propertiesObject: StreamingPropertiesObject = Utilities.getStreamingPropertiesobject(propertiesFilePath)
  val envPropertiesFilePath = propertiesFilePath.substring(0, propertiesFilePath.lastIndexOf("/") + 1) + "connection.properties"
  val sKeyFilePath = propertiesFilePath.substring(0, propertiesFilePath.lastIndexOf("/") + 1) + "sKey"
  val sk: SKeyObject = Utilities.getSKeyPropertiesobject(sKeyFilePath)
  val envPropertiesObject: EnvPropertiesObject = Utilities.getEnvPropertiesobject(envPropertiesFilePath, sk)
  val auditTbl = envPropertiesObject.getMySqlDBName() + "." + envPropertiesObject.getMySqlAuditTbl()
  val sqlCon = Utilities.getConnection(envPropertiesObject)

  //***************************Audit Properties********************************//
  val logger = Logger.getLogger(getClass.getName)
  auditObj.setAudJobStartTimeStamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val src_sys_ky = propertiesObject.getMasterDataFields().split(",", -1)(0)
  val fl_nm = propertiesObject.getMasterDataFields().split(",", -1)(1)
  val ld_jb_nr = propertiesObject.getMasterDataFields().split(",", -1)(2)
  val jb_nm = propertiesObject.getMasterDataFields().split(",", -1)(3)
  val config_obj_file_nm = propertiesObject.getMasterDataFields().split(",", -1)(4)
  val batchId = Utilities.getCurrentTimestamp("yyyyMMddHHmmss")
  val objName = propertiesObject.getObjName()
  val ChannelPartnerSFPOSLoadPSDMNSWSN: String = "ChannelPartnerSFPOSLoadPSDMNSWSN_an"
  val ChannelPartnerSFPOSLoadOPEPSDMNSWSN: String = "ChannelPartnerSFPOSLoadOPEPSDMNSWSN_an"

  logger.info("****Object Name****"+objName)

  var dbNameConsmtn: String = null
  var consmptnTable: String = null
  val dbName = propertiesObject.getDbName()
  if (propertiesObject.getTgtTblConsmtn().trim().split("\\.", -1).size == 2) {
    dbNameConsmtn = propertiesObject.getTgtTblConsmtn().trim().split("\\.", -1)(0)
    consmptnTable = propertiesObject.getTgtTblConsmtn().trim().split("\\.", -1)(1)
  } else {
    logger.error("Please update tgtTblConsmtn properties to add database name!")
    sqlCon.close()
    System.exit(1)
  }

  val dbNameSrcTbl = propertiesObject.getSrcTblConsmtn().trim().split("\\.", -1)(0)
  logger.info("Source DataBase: " + dbNameSrcTbl)

  val consumptionEntryFlag = Utilities.consumptionEntry(sqlCon, propertiesObject.getObjName())

  var jobStatusFlag = true

  val objNamePos = ChannelPartnerSFPOSLoadINC

  var load_pos_sn_fact_load = false

  var tgt_count = 0

  try {
    
    import spark.implicits._

    //Reading Max ld_ts from audit table to get the incremental data based upon this date
    val max_ld_ps_tme = Utilities.readChnlMaxLoadTimestamp(sqlCon, ChannelPartnerSFPOSLoadPSDMNSWSN)
    val max_ld_ope_tme = Utilities.readChnlMaxLoadTimestamp(sqlCon, ChannelPartnerSFPOSLoadOPEPSDMNSWSN)

    //val max_ld_tme = Utilities.readChnlMaxLoadTimestamp(sqlCon, propertiesObject.getObjName())
    
    //Checking if load is happening as FULL LOAD or INCREMENTAL LOAD

    val data_history_check = if (max_ld_ps_tme.equals("1900-01-01 00:00:00.0")) true else false
    logger.info("ps_max_date " + max_ld_ps_tme + " history check " + data_history_check)
    logger.info("ope_max_date " + max_ld_ope_tme + " history check " + data_history_check)

    //Reading  incremental data from source table

    val psDmnsnIncTrsn = spark.sql(
      s"""
         |select trsn_i_1_id from ea_common.ps_dmnsn WHERE iud_fl_1_cd <> 'D' and ins_gmt_ts >='${max_ld_ps_tme}'
         |UNION select trsn_i_1_id from ea_common.ope_ps_dmnsn_empty WHERE iud_fl_1_cd <> 'D' and ins_gmt_ts >='$max_ld_ope_tme'
         |""".stripMargin).dropDuplicates().persist(StorageLevel.MEMORY_AND_DISK_SER)
    psDmnsnIncTrsn.createOrReplaceTempView("psDmnsnIncTrsn")

    val psDmnsnIncTrsnTimeStamp = spark.sql(
      s"""
         |select '${ChannelPartnerSFPOSLoadPSDMNSWSN}' as obj_nm,max(ins_gmt_ts) as ins_gmt_ts from ea_common.ps_dmnsn WHERE iud_fl_1_cd <> 'D' and ins_gmt_ts >='${max_ld_ps_tme}'
         |UNION select '${ChannelPartnerSFPOSLoadOPEPSDMNSWSN}' as obj_nm,max(ins_gmt_ts) as ins_gmt_ts from ea_common.ope_ps_dmnsn_empty WHERE iud_fl_1_cd <> 'D' and ins_gmt_ts >='$max_ld_ope_tme'
         |""".stripMargin).persist(StorageLevel.MEMORY_AND_DISK_SER)


    //Creating dataframe based upon FULL LOAD or INCREMENTAL LOAD

    var ps_dmnsnDF = spark.emptyDataFrame

    if (!data_history_check) {
      ps_dmnsnDF = spark.sql(
        s"""
           |select 'E2OPEN' as src_sys_nm, 'POS' as rec_src,ps_ky, PS.trsn_i_1_id, prnt_trsn_i_1_id, prv_trsn_i_1_id, orgl_trsn_i_1_id, fl_i_1_id,
           |reporter_i_2_id, iud_fl_1_cd, fl_rcv_1_dt, trs_1_dt, inv_dt, sm_dt, rptd_prod_i_1_id, hpe_prod_i_1_id, ptnr_inrn_sk_1_cd, vl_vol_fl_1_cd,
           |bndl_prod_i_1_id, ptnr_bndl_prod_i_1_id, prod_ln_i_1_id, sale_qty_cd, ptnr_sl_prc_lcy_un_1_cd, ptnr_sl_prc_usd_un_1_cd, ptnr_sl_prc_curr_cd,
           |nt_cst_aftr_rbt_lcy_ex_1_cd, nt_cst_aftr_rbt_curr_cd, ptnr_prch_prc_lcy_un_1_cd, ptnr_prch_curr_cd, ptnr_inv_nr, hpe_inv_nr, ptnr_to_hpe_po_nr,
           |agt_flg_cd, cust_to_ptnr_po_nr, bndl_qt_1_cd, prod_orgn_cd, prchg_src_ws_ind_cd, splyr_iso_ctry_cd, ptnr_sls_rep_nm, org_sls_nm,
           |backend_deal_1_cleansed_cd, backend_deal_1_rptd_cd, backend_deal_2_cleansed_cd, backend_deal_2_rptd_cd, backend_deal_3_cleansed_cd,
           |backend_deal_3_rptd_cd, backend_deal_4_cleansed_cd, backend_deal_4_rptd_cd, deal_reg_1_cleansed_cd, deal_reg_1_rptd_cd,
           |deal_reg_2_cleansed_cd, deal_reg_2_rptd_cd, upfrnt_deal_1_cleansed_cd, upfrnt_deal_1_rptd_cd, upfrnt_deal_2_cleansed_cd,
           |upfrnt_deal_2_rptd_cd, upfrnt_deal_3_cleansed_cd, upfrnt_deal_3_rptd_cd, nmso_nm, nmso_dt, stndg_ofr_nr, bll_to_asian_addr_cd, bll_to_co_tx_id,
           |bll_to_co_tx_id_enr_id, bll_to_cntct_nm, bll_to_ph_nr, bll_to_rw_addr_id, bll_to_ky_cd, bll_to_rptd_id, bll_to_ctry_rptd_cd, end_usr_addr_src_rptd_cd,
           |end_usr_asian_addr_cd, end_usr_co_tx_id, end_usr_co_tx_id_enr_id, end_usr_cntct_nm, end_usr_ph_nr, end_usr_rw_addr_id, end_usr_ky_cd, end_usr_rptd_id,
           |end_usr_ctry_rptd_cd, sl_frm_cntc_1_nm, sl_frm_p_1_nr, sl_frm_rw_addr_i_1_id, sl_frm_ky_cd, sl_frm_rptd_id, sl_frm_ctry_rptd_cd, shp_frm_cntct_nm,
           |shp_frm_ph_nr, shp_frm_rptd_id, shp_frm_rw_addr_id, shp_frm_ky_cd, shp_frm_ctry_rptd_cd, shp_to_asian_addr_cd, shp_to_co_tx_id, shp_to_co_tx_id_enr_id,
           |shp_to_cntct_nm, shp_to_ph_nr, shp_to_rw_addr_id, shp_to_ky_cd, shipto_rptd_id, shp_to_ctry_rptd_cd, sld_to_asian_addr_cd, sld_to_co_tx_id, sld_to_co_tx_id_enr_id,
           |sld_to_cntct_nm, sld_to_ph_nr, sld_to_rw_addr_id, sld_to_ky_cd, sld_to_rptd_id, sld_to_ctry_rptd_cd, enttld_asian_addr_cd, enttld_co_tx_id, enttld_co_tx_id_enr_id,
           |enttld_cntct_nm, enttld_ph_nr, enttld_rw_addr_id, enttld_ky_cd, enttld_rptd_id, enttld_ctry_rptd_cd, drp_shp_flg_cd, crss_shp_flg_cd, orgl_hpe_asngd_trsn_id, ptnr_inrn_trsn_id,
           |brm_err_flg_cd, tty_mgr_cd, vldtn_wrnn_1_cd, rsrv_fld_1_cd, rsrv_fld_2_cd, rsrv_fld_3_cd as rptg_prd_strt_dt, rsrv_fld_4_cd as rptg_prd_end_dt, rsrv_fld_5_cd, rsrv_fld_6_cd,
           |rsrv_fld_7_cd, rsrv_fld_8_cd, rsrv_fld_9_cd, rsrv_fld_10_cd, src_sys_upd_ts, src_sys_ky, lgcl_dlt_ind, ins_gmt_ts, upd_gmt_ts, src_sys_extrc_gmt_ts, src_sys_btch_nr,
           |fl_nm, ld_jb_nr, ins_ts from ea_common.ps_dmnsn PS INNER JOIN psDmnsnIncTrsn incPS ON incPS.trsn_i_1_id=PS.trsn_i_1_id
           |WHERE iud_fl_1_cd <> 'D'
           |""".stripMargin)
      logger.info("max_ld_tme detail=" + max_ld_ps_tme)
      logger.info(""" select trsn_i_1_id from ea_common.ps_dmnsn WHERE iud_fl_1_cd <> 'D' and ins_gmt_ts >=""" + " '" + max_ld_ps_tme + "' ")

    } else {
      var psdmnsnIncDF = spark.sql(
        s"""
           |select 'E2OPEN' as src_sys_nm, 'POS' as rec_src,ps_ky, trsn_i_1_id, prnt_trsn_i_1_id, prv_trsn_i_1_id, orgl_trsn_i_1_id, fl_i_1_id, reporter_i_2_id,
           |iud_fl_1_cd, fl_rcv_1_dt, trs_1_dt, inv_dt, sm_dt, rptd_prod_i_1_id, hpe_prod_i_1_id, ptnr_inrn_sk_1_cd, vl_vol_fl_1_cd, bndl_prod_i_1_id, ptnr_bndl_prod_i_1_id,
           |prod_ln_i_1_id, sale_qty_cd, ptnr_sl_prc_lcy_un_1_cd, ptnr_sl_prc_usd_un_1_cd, ptnr_sl_prc_curr_cd, nt_cst_aftr_rbt_lcy_ex_1_cd, nt_cst_aftr_rbt_curr_cd,
           |ptnr_prch_prc_lcy_un_1_cd, ptnr_prch_curr_cd, ptnr_inv_nr, hpe_inv_nr, ptnr_to_hpe_po_nr, agt_flg_cd, cust_to_ptnr_po_nr, bndl_qt_1_cd, prod_orgn_cd,
           |prchg_src_ws_ind_cd, splyr_iso_ctry_cd, ptnr_sls_rep_nm, org_sls_nm, backend_deal_1_cleansed_cd, backend_deal_1_rptd_cd, backend_deal_2_cleansed_cd,
           |backend_deal_2_rptd_cd, backend_deal_3_cleansed_cd, backend_deal_3_rptd_cd, backend_deal_4_cleansed_cd, backend_deal_4_rptd_cd, deal_reg_1_cleansed_cd,
           |deal_reg_1_rptd_cd, deal_reg_2_cleansed_cd, deal_reg_2_rptd_cd, upfrnt_deal_1_cleansed_cd, upfrnt_deal_1_rptd_cd, upfrnt_deal_2_cleansed_cd, upfrnt_deal_2_rptd_cd,
           |upfrnt_deal_3_cleansed_cd, upfrnt_deal_3_rptd_cd, nmso_nm, nmso_dt, stndg_ofr_nr, bll_to_asian_addr_cd, bll_to_co_tx_id, bll_to_co_tx_id_enr_id, bll_to_cntct_nm,
           |bll_to_ph_nr, bll_to_rw_addr_id, bll_to_ky_cd, bll_to_rptd_id, bll_to_ctry_rptd_cd, end_usr_addr_src_rptd_cd, end_usr_asian_addr_cd, end_usr_co_tx_id,
           |end_usr_co_tx_id_enr_id, end_usr_cntct_nm, end_usr_ph_nr, end_usr_rw_addr_id, end_usr_ky_cd, end_usr_rptd_id, end_usr_ctry_rptd_cd, sl_frm_cntc_1_nm, sl_frm_p_1_nr,
           |sl_frm_rw_addr_i_1_id, sl_frm_ky_cd, sl_frm_rptd_id, sl_frm_ctry_rptd_cd, shp_frm_cntct_nm, shp_frm_ph_nr, shp_frm_rptd_id, shp_frm_rw_addr_id, shp_frm_ky_cd,
           |shp_frm_ctry_rptd_cd, shp_to_asian_addr_cd, shp_to_co_tx_id, shp_to_co_tx_id_enr_id, shp_to_cntct_nm, shp_to_ph_nr, shp_to_rw_addr_id, shp_to_ky_cd, shipto_rptd_id,
           |shp_to_ctry_rptd_cd, sld_to_asian_addr_cd, sld_to_co_tx_id, sld_to_co_tx_id_enr_id, sld_to_cntct_nm, sld_to_ph_nr, sld_to_rw_addr_id, sld_to_ky_cd, sld_to_rptd_id,
           |sld_to_ctry_rptd_cd, enttld_asian_addr_cd, enttld_co_tx_id, enttld_co_tx_id_enr_id, enttld_cntct_nm, enttld_ph_nr, enttld_rw_addr_id, enttld_ky_cd, enttld_rptd_id,
           |enttld_ctry_rptd_cd, drp_shp_flg_cd, crss_shp_flg_cd, orgl_hpe_asngd_trsn_id, ptnr_inrn_trsn_id, brm_err_flg_cd, tty_mgr_cd, vldtn_wrnn_1_cd, rsrv_fld_1_cd, rsrv_fld_2_cd,
           |rsrv_fld_3_cd as rptg_prd_strt_dt, rsrv_fld_4_cd as rptg_prd_end_dt, rsrv_fld_5_cd, rsrv_fld_6_cd, rsrv_fld_7_cd, rsrv_fld_8_cd, rsrv_fld_9_cd, rsrv_fld_10_cd, src_sys_upd_ts,
           |src_sys_ky, lgcl_dlt_ind, ins_gmt_ts, upd_gmt_ts, src_sys_extrc_gmt_ts, src_sys_btch_nr, fl_nm, ld_jb_nr, ins_ts from ea_common.ps_dmnsn WHERE iud_fl_1_cd <> 'D'
           |""".stripMargin)

      var psdmnsnHistDF = spark.sql(
        s"""
           |SELECT 'PDW' as src_sys_nm, 'POS' as rec_src, cast(NULL as bigint) as ps_ky,CASE WHEN trans_id_zyme_trans_id  not like '%-%' THEN cast(trans_id_zyme_trans_id as bigint)
           |WHEN trans_id_zyme_trans_id like '%-%' THEN Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint) END as trsn_i_1_id, NULL as prnt_trsn_i_1_id, NULL as prv_trsn_i_1_id,
           |NULL as orgl_trsn_i_1_id, NULL as fl_i_1_id, co_id_seller_co_id as reporter_i_2_id, NULL as iud_fl_1_cd, NULL as fl_rcv_1_dt, coverage_end_date_dt as trs_1_dt, txn_date_dt as inv_dt,
           |NULL as sm_dt, partner_prod_num_cd as rptd_prod_i_1_id, sku_num_cd as hpe_prod_i_1_id, NULL as ptnr_inrn_sk_1_cd, reserve_01_value_volume_flag_cd as vl_vol_fl_1_cd,
           |COALESCE(reserve_04_cd,bundle_id) as bndl_prod_i_1_id, NULL as ptnr_bndl_prod_i_1_id, pl_id as prod_ln_i_1_id,CASE WHEN shipped_qty_cd=0 THEN shipped_opt_qty_cd ELSE
           |shipped_qty_cd end as sale_qty_cd, NULL as ptnr_sl_prc_lcy_un_1_cd, NULL as ptnr_sl_prc_usd_un_1_cd, NULL as ptnr_sl_prc_curr_cd, cast(ext_net_cost_after_rebate_pc_cd as decimal(23,2)) as nt_cst_aftr_rbt_lcy_ex_1_cd,
           |ext_net_cost_after_rebate_pc_cd as nt_cst_aftr_rbt_curr_cd, NULL as ptnr_prch_prc_lcy_un_1_cd, ptnr_purch_price_txn_curncy_pc_cd as ptnr_prch_curr_cd, txn_doc_id as ptnr_inv_nr,
           |hp_inv_number_nr as hpe_inv_nr, reporter_purchase_order_id as ptnr_to_hpe_po_nr, NULL as agt_flg_cd, NULL as cust_to_ptnr_po_nr, NULL as bndl_qt_1_cd, NULL as prod_orgn_cd,
           |NULL as prchg_src_ws_ind_cd, NULL as splyr_iso_ctry_cd, sales_rep_name_nm as ptnr_sls_rep_nm,cOALESCE(reserve_11_cd,sales_rep_org_cd) as org_sls_nm,
           |cleansed_back_end_deal_1_cd as backend_deal_1_cleansed_cd, NULL as backend_deal_1_rptd_cd, cleansed_back_end_deal_2_cd as backend_deal_2_cleansed_cd,
           |NULL as backend_deal_2_rptd_cd, cleansed_back_end_deal_3_cd as backend_deal_3_cleansed_cd, NULL as backend_deal_3_rptd_cd, cleansed_back_end_deal_4_cd as backend_deal_4_cleansed_cd,
           |NULL as backend_deal_4_rptd_cd, cleansed_deal_reg_id1_id as deal_reg_1_cleansed_cd, NULL as deal_reg_1_rptd_cd, cleansed_deal_reg_id2_id as deal_reg_2_cleansed_cd, NULL as deal_reg_2_rptd_cd,
           |cleansed_upfront_deal_id_1_id as upfrnt_deal_1_cleansed_cd, eclipse_id as upfrnt_deal_1_rptd_cd, cleansed_upfront_deal_id_2_id as upfrnt_deal_2_cleansed_cd, NULL as upfrnt_deal_2_rptd_cd,
           |cleansed_upfront_deal_id_3_id as upfrnt_deal_3_cleansed_cd, NULL as upfrnt_deal_3_rptd_cd, NULL as nmso_nm, NULL as nmso_dt, NULL as stndg_ofr_nr, NULL as bll_to_asian_addr_cd,
           |NULL as bll_to_co_tx_id, NULL as bll_to_co_tx_id_enr_id, NULL as bll_to_cntct_nm, NULL as bll_to_ph_nr, NULL as bll_to_rw_addr_id, NULL as bll_to_ky_cd,
           |bill_to_loc_id_asgn_by_hp_id as bll_to_rptd_id, NULL as bll_to_ctry_rptd_cd,COALESCE(end_user_address_source_cd,reported_end_user_address_source_cd) as end_usr_addr_src_rptd_cd,
           |NULL as end_usr_asian_addr_cd, NULL as end_usr_co_tx_id, NULL as end_usr_co_tx_id_enr_id, NULL as end_usr_cntct_nm, NULL as end_usr_ph_nr, NULL as end_usr_rw_addr_id,
           |NULL as end_usr_ky_cd, end_user_id as end_usr_rptd_id, NULL as end_usr_ctry_rptd_cd, NULL as sl_frm_cntc_1_nm, NULL as sl_frm_p_1_nr, NULL as sl_frm_rw_addr_i_1_id, NULL as sl_frm_ky_cd,
           |NULL as sl_frm_rptd_id, NULL as sl_frm_ctry_rptd_cd, NULL as shp_frm_cntct_nm, NULL as shp_frm_ph_nr, NULL as shp_frm_rptd_id, NULL as shp_frm_rw_addr_id, NULL as shp_frm_ky_cd,
           |NULL as shp_frm_ctry_rptd_cd, NULL as shp_to_asian_addr_cd, NULL as shp_to_co_tx_id, NULL as shp_to_co_tx_id_enr_id, NULL as shp_to_cntct_nm, NULL as shp_to_ph_nr, NULL as shp_to_rw_addr_id,
           |NULL as shp_to_ky_cd, ship_to_loc_id_asgn_by_hp_id as shipto_rptd_id, NULL as shp_to_ctry_rptd_cd, NULL as sld_to_asian_addr_cd, NULL as sld_to_co_tx_id, NULL as sld_to_co_tx_id_enr_id,
           |NULL as sld_to_cntct_nm, NULL as sld_to_ph_nr, NULL as sld_to_rw_addr_id, NULL as sld_to_ky_cd, NULL as sld_to_rptd_id, NULL as sld_to_ctry_rptd_cd, NULL as enttld_asian_addr_cd,
           |NULL as enttld_co_tx_id, NULL as enttld_co_tx_id_enr_id, NULL as enttld_cntct_nm, NULL as enttld_ph_nr, NULL as enttld_rw_addr_id, NULL as enttld_ky_cd, NULL as enttld_rptd_id,
           |NULL as enttld_ctry_rptd_cd, drop_ship_flag_cd as drp_shp_flg_cd, cross_ship_flag_cd as crss_shp_flg_cd, orig_hpe_asgn_trx_no_cd as orgl_hpe_asngd_trsn_id, NULL as ptnr_inrn_trsn_id,
           |brm_error_flag_cd as brm_err_flg_cd, territory_manager_cd as tty_mgr_cd, validation_warning_code_cd as vldtn_wrnn_1_cd, NULL as rsrv_fld_1_cd, NULL as rsrv_fld_2_cd,
           |coverage_start_date_dt as rptg_prd_strt_dt, coverage_end_date_dt as rptg_prd_end_dt, NULL as rsrv_fld_5_cd, NULL as rsrv_fld_6_cd, NULL as rsrv_fld_7_cd, NULL as rsrv_fld_8_cd,
           |txn_doc_type_cd as rsrv_fld_9_cd, pdw_src_sys_ky_cd as rsrv_fld_10_cd, src_sys_upd_ts as src_sys_upd_ts, src_sys_ky as src_sys_ky, lgcl_dlt_ind as lgcl_dlt_ind, ins_gmt_dt as ins_gmt_ts,
           |upd_gmt_ts as upd_gmt_ts, src_sys_extrc_gmt_ts as src_sys_extrc_gmt_ts, src_sys_btch_nr as src_sys_btch_nr, fl_nm as fl_nm, ld_jb_nr as ld_jb_nr, NULL as ins_ts
           |from ea_common.dwtf_sl_thru_ref D LEFT JOIN psDmnsnIncTrsn ps ON Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint)=ps.trsn_i_1_id where ps.trsn_i_1_id IS NULL
           |and Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint) IS NOT NULL AND pdw_src_sys_ky_cd='ASPEN'
           |""".stripMargin)

           ps_dmnsnDF = psdmnsnIncDF.union(psdmnsnHistDF)
          logger.info(" PS_Hisotry_Data_Read")
    }

    ps_dmnsnDF.createOrReplaceTempView("ps_dmnsnDF")

    // Taking Source count 
    var src_count = 0

    val PosIncrementalDataCount = if (!data_history_check) {
      ps_dmnsnDF = ps_dmnsnDF.repartition($"trsn_i_1_id").persist(StorageLevel.MEMORY_AND_DISK_SER)
      src_count = psDmnsnIncTrsn.count().toInt
      logger.info("""******count_of_driver_in_incremental*****""" + src_count)
    } else {
      ps_dmnsnDF.repartition($"trsn_i_1_id")
      src_count = psDmnsnIncTrsn.count().toInt
      logger.info("""****Full_Data_Count**** :""" + src_count)
    }
    
    // If there is no incremental data , the job will come out and complete 

    if (src_count > 0) {

      //keeping deleted transaction in a table

      val Deleted_DF = spark.sql(s""" select  trsn_i_1_id,trs_1_dt,ins_gmt_ts from ea_common.ps_dmnsn WHERE iud_fl_1_cd = 'D' and ins_gmt_ts >=""" + " '" + max_ld_ps_tme + "' ")
      


      //creating OPE DF with the required columns
      val psDmnsnInc = spark.sql(
        s"""
           |select trsn_i_1_id from ea_common.ps_dmnsn  WHERE iud_fl_1_cd <> 'D' and  ins_gmt_ts >= '$max_ld_ps_tme'
           |UNION select trsn_i_1_id from ea_common.ope_ps_dmnsn_empty WHERE iud_fl_1_cd <> 'D' and  ins_gmt_ts >= '$max_ld_ope_tme'
           |""".stripMargin).dropDuplicates()

      psDmnsnInc.createOrReplaceTempView("psDmnsnInc")

      val OpePsDmnsnSubSet = spark.sql(
        s"""
           |select ope_rslr_prty_id, ope_sldt_prty_id, ptnr_pro_i_1_id, trsn_i_1_id, ope_dstr_prty_id, ope_enttld_prty_id, ope_end_cust_prty_id, ope_rslr_br_typ,
           |ope_sldt_br_typ, ope_dstr_br_typ, ent_mdm_br_ty_1_cd, ope_sldt_ctry, ope_src_end_cust_prty_id, ope_src_end_cust_ctry, ope_enttld_prty_ctry, ope_dstr_rsn_cd,
           |sldt_sbl_prm_id_ope, ope_end_cust_rsn_cd,deal_id,ope_end_cust_br_typ FROM ea_common.ope_ps_dmnsn_empty
           |""".stripMargin).repartition($"trsn_i_1_id").persist(StorageLevel.MEMORY_AND_DISK_SER)
      OpePsDmnsnSubSet.createOrReplaceTempView("OpePsDmnsnSubSet")

      val OpePsDmnsnSuperSet = spark.sql(
        s"""
           |select ope_bmt_dstr_prty_id, ope_bmt_dstr_br_typ, ope_bmt_dstr_sls_comp_rlvnt, ope_dstr_br_typ, ope_dstr_prty_id, ope_dstr_rsn_cd,
           |ope_bmt_end_cust_br_typ, ope_bmt_end_cust_prty_id, ope_bmt_end_cust_sls_comp_rlvnt, ope_end_cust_br_typ, end_cust_prty_id_ope,
           |ope_end_cust_prty_id, ope_end_cust_rsn_cd, ope_lvl_3_br_typ, ope_lvl_3_prty_id, ope_lvl_3_rshp, ope_enttld_prty_ctry, ope_enttld_prty_id,
           |ope_src_end_cust_ctry, ope_src_end_cust_prty_id, dbm_rptg_dstr_ope, dstr_rptg_dstr_ope, end_cust_sls_rep_rptg_dstr_ope, flipped_dbm_vw_flg_ope,
           |flipped_dstr_vw_flg_ope, flipped_end_cust_vw_flg_ope, flipped_pbm_vw_flg_ope, flipped_rslr_vw_flg_ope, ope_lvl_1_br_typ, ope_lvl_1_prty_id, ope_lvl_1_rshp,
           |pbm_rptg_dstr_ope, rslr_rptg_dstr_ope, ope_bmt_rslr_br_typ, ope_bmt_rslr_prty_id, ope_rslr_br_typ, ope_rslr_prty_id, ope_rslr_rsn_cd, ope_lvl_2_br_typ,
           |ope_lvl_2_prty_id, ope_lvl_2_rshp, ope_sldt_br_typ, ope_sldt_ctry, ope_sldt_prty_id, ope_big_deal_xclsn, ope_crss_brdr, ope_crss_srcd, dstr_rptg_dstr_to_rptg_dstr_ope,
           |ope_dstr_comp_vw, ope_dstr_bookings_vw, ope_dbm_bookings_vw, ope_dbm_comp_vw, ope_drp_shp, emb_srv_ln_itms_ope, end_cust_sls_rep_rptg_dstr_to_rptg_dstr_ope,
           |ope_end_cust_sls_rep_bookings_vw, ope_fdrl_bsn, ope_pbm_bookings_vw, ope_pbm_comp_vw, pft_cntr_cd_ope, ope_rslr_bookings_vw, ope_rslr_comp_vw, srv_ln_itm_ope,
           |trsn_i_1_id, reporter_prty_id, ent_mdm_br_ty_1_cd, ope_deal_end_cust_prty_id, sldt_sbl_prm_id_ope, reporter_emdm_prty_id_ope, reporter_new_br_typ_ope
           |from ea_common.ope_ps_dmnsn_empty
           |""".stripMargin).repartition($"trsn_i_1_id")
      
      OpePsDmnsnSuperSet.createOrReplaceTempView("OpePsDmnsnSuperSet")

      //creating Source DF with the transaction Id  columns

      val PsDmnsnSubSet = spark.sql(
        s"""
           |select  trsn_i_1_id ,hpe_prod_i_1_id, prod_ln_i_1_id ,trs_1_dt,upfrnt_deal_2_cleansed_cd,reporter_i_2_id,sld_to_rw_addr_id, sld_to_ky_cd,
           |ptnr_inv_nr ,sld_to_rptd_id ,sale_qty_cd from ea_common.ps_dmnsn WHERE iud_fl_1_cd <> 'D'
           |""".stripMargin)
      PsDmnsnSubSet.createOrReplaceTempView("PsDmnsnSubSet")

      val PsDmnsnSubSetTrsn = spark.sql(
        s"""
           |select PS.trsn_i_1_id ,hpe_prod_i_1_id, prod_ln_i_1_id ,trs_1_dt,upfrnt_deal_2_cleansed_cd,reporter_i_2_id,sld_to_rw_addr_id,
           |sld_to_ky_cd,ptnr_inv_nr,sld_to_rptd_id,sale_qty_cd,ins_gmt_ts from ea_common.ps_dmnsn PS
           |INNER JOIN psDmnsnInc incPS ON incPS.trsn_i_1_id=PS.trsn_i_1_id WHERE iud_fl_1_cd <> 'D'
           |""".stripMargin).repartition($"trsn_i_1_id").persist(StorageLevel.MEMORY_AND_DISK_SER)

      PsDmnsnSubSetTrsn.createOrReplaceTempView("PsDmnsnSubSetTrsn")

      val prtyPtnrAsscnDF = spark.sql(
        s"""
           |select sbl_prm_id,
           |intrm_prty_ptnr_asscn_prty_id,
           |mdcp_bsn_rshp_typ_nm from (select sbl_prm_id ,
           |intrm_prty_ptnr_asscn_prty_id,
           |mdcp_bsn_rshp_typ_nm,
           |row_number() over(partition by sbl_prm_id order by ins_gmt_ts desc)rk
           |from ea_common.prty_ptnr_asscn_dmnsn)A where rk=1
           |""".stripMargin)

      prtyPtnrAsscnDF.createOrReplaceTempView("prtyPtnrAsscnDF")

      val chnlptnrCommonPrtyDmnsn = spark.sql(
        s"""
           |select mdm_id,
           |prty_ctry_cd,
           |addr_2_cd,
           |cty_cd,
           |prty_ctry_nm,
           |cmpt_ind_cd,
           |cust_ind_cd,
           |out_of_business_ind,
           |hq_ind_cd,
           |indy_vtcl_nm,
           |indy_vtcl_sgm_nm,
           |lgl_ctry_nm,
           |prty_lcl_nm,
           |ptnr_ind_cd,
           |prty_stts_nm,
           |prty_typ_cd,
           |prty_typ_nm,
           |prty_pstl_cd,
           |st_prvc_cd,
           |st_prvc_nm,
           |vndr_ind_cd,
           |cy_profile_entity_name,
           |ctry_lvl_lcl_languague_grp_nm,
           |gbl_lvl_grp_id,
           |gbl_lvl_grp_nm,
           |ctry_lvl_grp_nm,
           |gbl_lvl_lcl_languague_grp_nm,
           |cy_profile_lvl_grp_id_rslr_dstr,
           |cy_profile_lvl_grp_nm_rslr_dstr,
           |addr_1_cd,
           |cy_country_entity_aruba_rad_name,
           |cy_country_entity_eg_rad_name,
           |cy_global_entity_aruba_rad_name,
           |cy_global_entity_eg_rad_name,
           |ctry_lvl_grp_id,
           |emdmh_ctry_cd,
           |emdmh_ctry_nm,
           |cy_profile_entity_id
           |from ea_common.chnlptnr_common_prty_dmnsn
           |""".stripMargin).persist(StorageLevel.MEMORY_AND_DISK_SER)
	  
      chnlptnrCommonPrtyDmnsn.createOrReplaceTempView("chnlptnr_common_prty_dmnsn")

      //****bmt_bi_end_customer_entitled_dmnsn****
      val bmt_bi_end_customer_entitled_dmnsn = spark.sql(
        s"""
           |select bmt_bi_end_customer_entitled_ky,
           |eff_upd_strt_dt,
           |eff_upd_end_dt,
           |reporter_id,
           |reporter_party_id,
           |reporter_br_type,
           |reporting_type,
           |end_customer_rawaddr_id,
           |end_customer_rawaddr_key,
           |end_user_reported_id,
           |entitled_raw_address_id,
           |entitled_key,
           |entitled_reported_id,
           |partner_invoice_numbersales_ord_nr,
           |ope_mf_trsn_ide2open_trsn_id,
           |deal_id,
           |rec_src,
           |bi_end_cust_party_id,
           |bi_end_cust_grp_id,
           |bi_end_cust_pfl_lvl_grp_id,
           |bi_end_cust_ctry_lvl_grp_id,
           |bi_end_cust_ww_lvl_grp_id,
           |bi_end_cust_ctry_cd,
           |bi_end_customer_legacy_opsi_id,
           |bi_end_customer_legacy_sta_id,
           |bi_enttld_party_id,
           |bi_enttld_ctry_lvl_grp_id,
           |bi_enttld_pfl_lvl_grp_id,
           |bi_enttld_ctry_cd,
           |end_cust_opsi_id,
           |reserved_fld_1,
           |reserved_fld_2,
           |reserved_fld_3,
           |reserved_fld_4,
           |reserved_fld_5,
           |reserved_fld_6,
           |reserved_fld_7,
           |reserved_fld_8,
           |reserved_fld_9,
           |reserved_fld_10,
           |src_sys_upd_ts,
           |src_sys_ky,
           |lgcl_dlt_ind,
           |ins_gmt_ts,
           |upd_gmt_ts,
           |src_sys_extrc_gmt_ts,
           |src_sys_btch_nr,
           |fl_nm,
           |ld_jb_nr,
           |bi_enttld_prty_br_typ,
           |bi_end_cust_br_typ,
           |bi_end_cust_lgcy_sls_cvrg_sgm,
           |bi_end_cust_lgcy_xtrn_mkt_sgm,
           |bi_end_cust_lgcy_ctry,
           |bi_end_cust_lgcy_nm,
           |bi_end_cust_ctry_lvl_aruba_covtier,
           |bi_end_cust_ctry_lvl_hybrid_it_covtier,
           |bi_end_cust_ctry_lvl_srvs_covtier,
           |bi_end_cust_ctry_lvl_ww_mkt_sgm,
           |bi_end_cust_ctry_lvl_mkt_sgm,
           |ins_ts from ea_common.bmt_bi_end_customer_entitled_dmnsn
           |where (UPPER(rec_src)='POS' or rec_src is null or UPPER(rec_src)='ALL' or rec_src='' or UPPER(rec_src)='E2OPEN')
           |""".stripMargin).persist(StorageLevel.MEMORY_AND_DISK_SER)
      bmt_bi_end_customer_entitled_dmnsn.createOrReplaceTempView("bmt_bi_end_customer_entitled_dmnsn")

      val csisAmsSimNtBmtRef = spark.sql(
        s"""
           |SELECT trsn_id,
           |reporter_party_id,
           |reporter_br_typ_cd,
           |reporter_lgcy_pro_id,
           |bi_dstr_prty_id,
           |bi_dstr_br_typ_nm,
           |bi_dstr_lgcy_pro_id,
           |bi_dstr_rsn_cd,
           |bi_rslr_prty_id,
           |bi_rslr_br_typ_nm,
           |bi_rslr_lgcy_pro_id,
           |bi_rslr_rsn_cd,
           |COALESCE(A.bi_rslr_ctry_cd,rslrPrty.prty_ctry_cd) as bi_rslr_ctry_cd,
           |bi_rslr_lgcy_hq_ctry_cd,
           |bi_end_cust_prty_id,
           |bi_end_cust_lgcy_opsi_id,
           |dy_dt,
           |byr_sb_region2_cd,
           |byr_ctry_nm,
           |slr_hq_rowid_id,
           |slr_site_rowid_id,
           |byr_hq_rowid_id,
           |byr_site_rowid_id,
           |prod_nr,
           |drvd_big_deal_no_cd,
           |inv_nr,
           |vol_sku_flg_cd,
           |uf_big_deal_id,
           |be_big_deal_id,
           |prcg_prgm_cd,
           |prcg_prgm_grp_cd,
           |drvd_eu_id,
           |drvd_src_nm,
           |eu_opsi_id,
           |drvd_eclipse_eu_opsi_id,
           |tcm_deal_nt_usd_cd,
           |src_sytm_nm,
           |src_sys_upd_ts,
           |src_sys_ky,
           |lgcl_dlt_ind,
           |A.ins_gmt_ts,
           |upd_gmt_ts,
           |src_sys_extrc_gmt_ts,
           |src_sys_btch_nr,
           |fl_nm,
           |ld_jb_nr,
           |dstrPrty.prty_ctry_cd as bi_dstr_ctry_cd,
           |endPrty.prty_ctry_cd as bi_end_cust_ctry_cd from
           |(select trsn_id,
           |COALESCE(dstr_rslr_rprtBMT.reporter_party_id,AMS.reporter_prty_id) as reporter_party_id,--not mapped
           |COALESCE(dstr_rslr_rprtBMT.reporter_br_type,AMS.reporter_br_typ_cd) as reporter_br_typ_cd,--not mapped
           |AMS.reporter_lgcy_pro_id,--not mapped
           |COALESCE(dstr_rslr_rprtBMT.bi_dstr_party_id,AMS.bi_dstr_prty_id) as bi_dstr_prty_id,
           |COALESCE(dstr_rslr_rprtBMT.bi_dstr_br_typ,AMS.bi_dstr_br_typ_cd) as BI_Dstr_BR_typ_nm,
           |COALESCE(dstr_rslr_rprtBMT.bi_dstr_lgcy_pro_id,AMS.bi_dstr_lgcy_pro_id) as bi_dstr_lgcy_pro_id,
           |bi_dstr_rsn_cd as bi_dstr_rsn_cd,-- confirmed by Alex
           |COALESCE(dstr_rslr_rprtBMT.bi_rslr_party_id,PPA.intrm_prty_ptnr_asscn_prty_id) as bi_rslr_prty_id,
           |COALESCE(dstr_rslr_rprtBMT.bi_rslr_br_typ,PPA.mdcp_bsn_rshp_typ_nm) as BI_rslr_BR_typ_nm,
           |COALESCE(dstr_rslr_rprtBMT.bi_rslr_lgcy_pro_id,AMS.bi_rslr_lgcy_pro_id) as bi_rslr_lgcy_pro_id,
           |bi_rslr_rsn_cd as bi_rslr_rsn_cd,-- confirmed by Alex
           |COALESCE(dstr_rslr_rprtBMT.bi_rslr_ctry_cd,AMS.bi_rslr_ctry_cd) as bi_rslr_ctry_cd,
           |AMS.bi_rslr_lgcy_hq_ctry_cd,--not mapped
           |COALESCE(end_cust.bi_end_cust_party_id,AMS.bi_end_cust_prty_id,PAD.prty_asscn_prty_id) as bi_end_cust_prty_id,
           |COALESCE(end_cust.bi_end_customer_legacy_opsi_id,AMS.bi_end_cust_lgcy_opsi_id) as bi_end_cust_lgcy_opsi_id,
           |AMS.dy_dt,
           |AMS.byr_sb_region2_cd,--not mapped
           |AMS.byr_ctry_nm,--not mapped
           |AMS.slr_hq_rowid_id,--not mapped
           |AMS.slr_site_rowid_id,--not mapped
           |AMS.byr_hq_rowid_id,--not mapped
           |AMS.byr_site_rowid_id,--not mapped
           |AMS.prod_nr,
           |AMS.drvd_big_deal_no_cd,--not mapped
           |AMS.inv_nr,
           |AMS.vol_sku_flg_cd,
           |AMS.uf_big_deal_id,
           |AMS.be_big_deal_id,
           |AMS.prcg_prgm_cd,--not mapped
           |AMS.prcg_prgm_grp_cd,--not mapped
           |AMS.drvd_eu_id,--not mapped
           |AMS.drvd_src_nm,--not mapped
           |AMS.eu_opsi_id,--not mapped
           |AMS.drvd_eclipse_eu_opsi_id,--not mapped
           |AMS.tcm_deal_nt_usd_cd,
           |NULL as src_sytm_nm,
           |AMS.src_sys_upd_ts,
           |AMS.src_sys_ky,
           |AMS.lgcl_dlt_ind,
           |AMS.ins_gmt_ts,
           |AMS.upd_gmt_ts,
           |AMS.src_sys_extrc_gmt_ts,
           |AMS.src_sys_btch_nr,
           |AMS.fl_nm,
           |AMS.ld_jb_nr
           | from ea_common.csis_ams_sim_nt_bmt_ref AMS left join prtyPtnrAsscnDF PPA ON AMS.bi_rslr_lgcy_pro_id=PPA.sbl_prm_id
           |left join ea_common.bmt_bi_distri_reseller_reporter_dmnsn dstr_rslr_rprtBMT ON AMS.trsn_id = dstr_rslr_rprtBMT.e2open_trsn_id
           |LEFT JOIN (select A.prty_asscn_prty_id,A.pos_prty_crss_xrf_ky from
           |(
           |select prty_asscn_prty_id,pos_prty_crss_xrf_ky ,row_number() over (partition by pos_prty_crss_xrf_ky order by ins_gmt_ts desc,last_upd_dt desc ) as rn
           |from ea_common.prty_asscn_dmnsn where upper(trim(prty_asscn_asscn_vl_typ_cd))='OPSI' and upper(trim(pos_prty_crss_xrf_ky))<>'OPSI'
           |) A where rn=1) PAD ON upper(trim(PAD.pos_prty_crss_xrf_ky))=upper(trim(AMS.bi_end_cust_lgcy_opsi_id))
           |left join bmt_bi_end_customer_entitled_dmnsn end_cust on trim(end_cust.ope_mf_trsn_ide2open_trsn_id)=AMS.trsn_id
           |)A
           |LEFT JOIN chnlptnr_common_prty_dmnsn rslrPrty ON trim(A.bi_rslr_prty_id)=trim(rslrPrty.mdm_id)
           |LEFT JOIN chnlptnr_common_prty_dmnsn dstrPrty ON trim(A.bi_dstr_prty_id)=trim(dstrPrty.mdm_id)
           |LEFT JOIN chnlptnr_common_prty_dmnsn endPrty  ON trim(A.bi_end_cust_prty_id)=trim(endPrty.mdm_id)
           |""".stripMargin)

      //csisAmsSimNtBmtRef.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("${dbNameSrcTbl}.csisAmsSimNtBmtRef")
      csisAmsSimNtBmtRef.createOrReplaceTempView("csisAmsSimNtBmtRef")

      //val AmsCount = csisAmsSimNtBmtRef.count().toInt
      logger.info("****Ams History count**** : ")


      val PsDmnsnTRSNSet = spark.sql(
        s"""
           |select  trsn_i_1_id , reporter_i_2_id, ptnr_inv_nr, end_usr_rw_addr_id, end_usr_rptd_id, end_usr_ky_cd,upfrnt_deal_1_cleansed_cd,
           |backend_deal_1_cleansed_cd,NULL as bi_end_customer_legacy_opsi_id,trs_1_dt,enttld_rptd_id,enttld_rw_addr_id,enttld_ky_cd
           |from ea_common.ps_dmnsn WHERE iud_fl_1_cd <> 'D'
           |UNION
           |SELECT CASE WHEN trans_id_zyme_trans_id  not like '%-%' THEN cast(trans_id_zyme_trans_id as bigint)
           |WHEN trans_id_zyme_trans_id like '%-%' THEN Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint) END as trsn_i_1_id,
           |co_id_seller_co_id as reporter_i_2_id ,txn_doc_id as ptnr_inv_nr,NULL as end_usr_rw_addr_id,NULL as end_usr_rptd_id,NULL as end_usr_ky_cd,
           |cleansed_upfront_deal_id_1_id as upfrnt_deal_1_cleansed_cd,cleansed_back_end_deal_1_cd as backend_deal_1_cleansed_cd,
           |NULL as bi_end_customer_legacy_opsi_id,coverage_end_date_dt as trs_1_dt, NULL as enttld_rptd_id,NULL as enttld_rw_addr_id,
           |NULL as enttld_ky_cd
           |from ea_common.dwtf_sl_thru_ref D LEFT JOIN PsDmnsnSubSet ps ON Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint)=ps.trsn_i_1_id
           |where ps.trsn_i_1_id IS NULL and Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint) IS NOT NULL
           |""".stripMargin).repartition($"trsn_i_1_id").persist(StorageLevel.MEMORY_AND_DISK_SER)

      PsDmnsnTRSNSet.createOrReplaceTempView("PsDmnsnTRSNSet")

      val OpePsDmnsnDF = spark.sql(
        s"""
           |select OPE.trsn_ky, OPE.trsn_i_1_id, OPE.hpe_prod_i_1_id, OPE.reporter_i_2_id, OPE.trs_1_dt, OPE.backend_deal_1_cleansed_cd,
           |OPE.backend_deal_2_cleansed_cd, OPE.backend_deal_3_cleansed_cd, OPE.backend_deal_4_cleansed_cd, OPE.upfrnt_deal_1_cleansed_cd,
           |OPE.upfrnt_deal_2_cleansed_cd, OPE.upfrnt_deal_3_cleansed_cd, OPE.deal_id, OPE.end_usr_rw_addr_id, OPE.end_usr_ky_cd, OPE.sld_to_rw_addr_id,
           |OPE.sld_to_ky_cd, OPE.enttld_rw_addr_id, OPE.enttld_ky_cd, OPE.sld_to_rptd_id, OPE.bll_to_rptd_id, OPE.shipto_rptd_id, OPE.end_usr_rptd_id,
           |OPE.prod_ln_i_1_id, OPE.ptnr_inv_nr, OPE.prod_orgn_cd, OPE.sale_qty_cd, OPE.cldr_yr_mth_cd, OPE.trsn_yr_ope, OPE.trsn_mnt_ope, OPE.src_rtm,
           |OPE.trsn_stts_ope, OPE.reporter_i_1_id, OPE.ptnr_pro_i_1_id, OPE.mdcp_br_ty_1_cd, OPE.ctry_cd, OPE.reporter_prty_id, OPE.ent_mdm_br_ty_1_cd,
           |OPE.rgn_cd, OPE.end_cust_lgcy_prty_id_ope, OPE.end_cust_lgcy_br_typ_ope, OPE.end_cust_prty_id_ope, OPE.end_cust_br_typ_ope, OPE.sldt_lgcy_prty_id_ope,
           |OPE.sldt_lgcy_br_typ_ope, OPE.sldt_prty_id_ope, OPE.sldt_br_typ_ope, OPE.sldt_sbl_prm_id_ope, OPE.deal_nt_usd_ext_cd, OPE.end_cust_emdm_prty_id_ope,
           |OPE.sldt_emdm_prty_id_ope, OPE.reporter_emdm_prty_id_ope, OPE.end_cust_new_br_typ_ope, OPE.sldt_new_br_typ_ope, OPE.reporter_new_br_typ_ope,
           |OPE.reporter_br_typ_rshp_ope, OPE.sldt_br_typ_rshp_ope, OPE.ope_deal_tr_2_rslr_prty_id, OPE.ope_deal_tr_2_rslr_br_typ, OPE.ope_deal_end_cust_prty_id,
           |OPE.ope_deal_end_cust_br_typ, OPE.ope_bmt_dstr_prty_id, OPE.ope_bmt_dstr_br_typ, OPE.ope_bmt_dstr_sls_comp_rlvnt, OPE.ope_bmt_rslr_prty_id,
           |OPE.ope_bmt_rslr_br_typ, OPE.ope_bmt_rslr_sls_comp_rlvnt, OPE.ope_bmt_end_cust_prty_id, OPE.ope_bmt_end_cust_br_typ, OPE.ope_bmt_end_cust_sls_comp_rlvnt,
           |OPE.ope_dstr_prty_id, OPE.ope_dstr_br_typ, OPE.ope_dstr_rsn_cd, OPE.ope_rslr_prty_id, OPE.ope_rslr_br_typ, OPE.ope_rslr_rsn_cd, OPE.ope_end_cust_prty_id,
           |OPE.ope_end_cust_br_typ, OPE.ope_end_cust_rsn_cd, OPE.ope_lvl_1_prty_id, OPE.ope_lvl_1_br_typ, OPE.ope_lvl_1_rshp, OPE.ope_lvl_2_prty_id, OPE.ope_lvl_2_br_typ,
           |OPE.ope_lvl_2_rshp, OPE.ope_lvl_3_prty_id, OPE.ope_lvl_3_br_typ, OPE.ope_lvl_3_rshp, OPE.ope_sldt_prty_id, OPE.ope_sldt_br_typ, OPE.ope_sldt_ctry, OPE.ope_enttld_prty_id,
           |OPE.ope_enttld_prty_ctry, OPE.ope_src_end_cust_prty_id, OPE.ope_src_end_cust_ctry, OPE.ope_ctry_cd, OPE.ope_ctry_cd_src, OPE.ope_rgn, OPE.ope_sb_rgn_lvl_1,
           |OPE.ope_sb_rgn_lvl_2, OPE.ope_sb_rgn_lvl_3, OPE.ope_rtm, OPE.ope_drp_shp, OPE.ope_crss_srcd, OPE.ope_big_deal_xclsn, OPE.ope_fdrl_bsn, OPE.ope_oem_rslr,
           |OPE.ope_oem_dstr, OPE.ope_oem_sldt, OPE.ope_crss_brdr, OPE.ope_revenuable, OPE.ope_qta_countable, OPE.pft_cntr_cd_ope, OPE.srv_ln_itm_ope, OPE.emb_srv_ln_itms_ope,
           |OPE.end_cust_sls_rep_rptg_dstr_ope, OPE.rslr_rptg_dstr_ope, OPE.dstr_rptg_dstr_ope, OPE.pbm_rptg_dstr_ope, OPE.dbm_rptg_dstr_ope,
           |OPE.end_cust_sls_rep_rptg_dstr_to_rptg_dstr_ope, OPE.dstr_rptg_dstr_to_rptg_dstr_ope, OPE.flipped_end_cust_vw_flg_ope, OPE.flipped_dstr_vw_flg_ope,
           |OPE.flipped_dbm_vw_flg_ope, OPE.flipped_rslr_vw_flg_ope, OPE.flipped_pbm_vw_flg_ope, OPE.ope_end_cust_sls_rep_bookings_vw, OPE.ope_dstr_bookings_vw,
           |OPE.ope_dbm_bookings_vw, OPE.ope_rslr_bookings_vw, OPE.ope_pbm_bookings_vw, OPE.ope_end_cust_sls_rep_comp_vw, OPE.ope_dstr_comp_vw, OPE.ope_dbm_comp_vw,
           |OPE.ope_rslr_comp_vw, OPE.ope_pbm_comp_vw, OPE.ope_mrgn, OPE.ope_mrgn_err_flg, OPE.src_sys_upd_ts, OPE.src_sys_ky, OPE.lgcl_dlt_ind, OPE.ins_gmt_ts,
           |OPE.upd_gmt_ts, OPE.src_sys_extrc_gmt_ts, OPE.src_sys_btch_nr, OPE.fl_nm, OPE.ld_jb_nr, OPE.ins_ts FROM ea_common.ope_ps_dmnsn_empty OPE
           |INNER JOIN psDmnsnInc PS ON OPE.trsn_i_1_id=PS.trsn_i_1_id
           |""".stripMargin)
      OpePsDmnsnDF.createOrReplaceTempView("OpePsDmnsnDF")

      var ps_prs_dmnsnDF = spark.emptyDataFrame

      if (!data_history_check) {
        ps_prs_dmnsnDF = spark.sql(
          s"""
             |select PRS.ps_prs_ky, PRS.trsn_i_3_id, PRS.prnt_trsn_i_4_id, PRS.prv_trsn_i_5_id, PRS.fl_i_6_id, PRS.reporter_i_7_id,
             |PRS.iud_fl_8_cd, PRS.valtn_trsn_i_1_id, PRS.valtn_wrnn_1_cd, PRS.lst_prc_lcy_ex_1_cd, PRS.lst_prc_lcy_un_1_cd, PRS.lst_prc_usd_ex_1_cd,
             |PRS.lst_prc_usd_un_1_cd, PRS.ndp_lcy_ex_1_cd, PRS.ndp_lcy_un_1_cd, PRS.ndp_usd_ex_1_cd, PRS.ndp_usd_un_1_cd, PRS.p_1_nr, PRS.pa_dscn_1_cd,
             |PRS.pa_dscnt_lcy_un_1_cd, PRS.pa_dscnt_usd_un_1_cd, PRS.deal_nt_lcy_ext_cd, PRS.deal_nt_lcy_un_1_cd, PRS.deal_nt_usd_ext_cd, PRS.deal_nt_usd_un_1_cd,
             |PRS.ndp_coefficien_1_cd, PRS.ndp_estmd_lcy_ext_cd, PRS.ndp_estmd_lcy_unt_cd, PRS.ndp_estmd_usd_ext_cd, PRS.ndp_estmd_usd_unt_cd, PRS.asp_coefficien_1_cd,
             |PRS.asp_lcy_ex_1_cd, PRS.asp_lcy_un_1_cd, PRS.asp_usd_ex_1_cd, PRS.asp_usd_un_1_cd, PRS.cust_app_2_cd, PRS.deal_geo_cd, PRS.dscnt_geo_cd, PRS.used_prc_ds_1_cd,
             |PRS.used_cur_1_cd, PRS.upfrnt_deal_1__1_cd, PRS.upfrnt_deal_1_ln_it_1_cd, PRS.upfrnt_deal_1_vrs_1_cd, PRS.upfrnt_deal_1_prc_lcy_un_1_cd,
             |PRS.upfrnt_deal_1_prc_usd_un_1_cd, PRS.upfrnt_deal_1_dscnt_lcy_ex_1_cd, PRS.upfrnt_deal_1_dscnt_lcy_un_1_cd, PRS.upfrnt_deal_1_dscnt_usd_ex_1_cd,
             |PRS.upfrnt_deal_1_dscnt_usd_un_1_cd, PRS.upfrnt_deal_2_m_cd, PRS.upfrnt_deal_2_ln_itm_cd, PRS.upfrnt_deal_2_vrsn_cd, PRS.upfrnt_deal_2_prc_lcy_unt_cd,
             |PRS.upfrnt_deal_2_prc_usd_unt_cd, PRS.upfrnt_deal_2_dscnt_lcy_ext_cd, PRS.upfrnt_deal_2_dscnt_lcy_unt_cd, PRS.upfrnt_deal_2_dscnt_usd_ext_cd,
             |PRS.upfrnt_deal_2_dscnt_usd_unt_cd, PRS.upfrnt_deal_3_m_cd, PRS.upfrnt_deal_3_ln_itm_cd, PRS.upfrnt_deal_3_vrsn_cd, PRS.upfrnt_deal_3_prc_lcy_unt_cd,
             |PRS.upfrnt_deal_3_prc_usd_unt_cd, PRS.upfrnt_deal_3_dscnt_lcy_ext_cd, PRS.upfrnt_deal_3_dscnt_lcy_unt_cd, PRS.upfrnt_deal_3_dscnt_usd_ext_cd,
             |PRS.upfrnt_deal_3_dscnt_usd_unt_cd, PRS.backend_deal_1_m_cd, PRS.backend_deal_1_ln_itm_cd, PRS.backend_deal_1_vrsn_cd, PRS.backend_deal_1_prc_lcy_unt_cd,
             |PRS.backend_deal_1_prc_usd_unt_cd, PRS.backend_deal_1_dscnt_lcy_ext_cd, PRS.backend_deal_1_dscnt_lcy_unt_cd, PRS.backend_deal_1_dscnt_usd_ext_cd,
             |PRS.backend_deal_1_dscnt_usd_unt_cd, PRS.backend_deal_2_m_cd, PRS.backend_deal_2_ln_itm_cd, PRS.backend_deal_2_vrsn_cd, PRS.backend_deal_2_prc_lcy_unt_cd,
             |PRS.backend_deal_2_prc_usd_unt_cd, PRS.backend_deal_2_dscnt_lcy_ext_cd, PRS.backend_deal_2_dscnt_lcy_unt_cd, PRS.backend_deal_2_dscnt_usd_ext_cd,
             |PRS.backend_deal_2_dscnt_usd_unt_cd, PRS.backend_deal_3_m_cd, PRS.backend_deal_3_ln_itm_cd, PRS.backend_deal_3_vrsn_cd, PRS.backend_deal_3_prc_lcy_unt_cd,
             |PRS.backend_deal_3_prc_usd_unt_cd, PRS.backend_deal_3_dscnt_lcy_ext_cd, PRS.backend_deal_3_dscnt_lcy_unt_cd, PRS.backend_deal_3_dscnt_usd_ext_cd,
             |PRS.backend_deal_3_dscnt_usd_unt_cd, PRS.backend_deal_4_m_cd, PRS.backend_deal_4_ln_itm_cd, PRS.backend_deal_4_vrsn_cd, PRS.backend_deal_4_prc_lcy_unt_cd,
             |PRS.backend_deal_4_prc_usd_unt_cd, PRS.backend_deal_4_dscnt_lcy_ext_cd, PRS.backend_deal_4_dscnt_lcy_unt_cd, PRS.backend_deal_4_dscnt_usd_ext_cd,
             |PRS.backend_deal_4_dscnt_usd_unt_cd, PRS.backend_rbt_1_lcy_unt_cd, PRS.backend_rbt_1_usd_unt_cd, PRS.backend_rbt_2_lcy_unt_cd, PRS.backend_rbt_2_usd_unt_cd,
             |PRS.backend_rbt_3_lcy_unt_cd, PRS.backend_rbt_3_usd_unt_cd, PRS.backend_rbt_4_lcy_unt_cd, PRS.backend_rbt_4_usd_unt_cd, PRS.exch_rate_lcy_to_us_1_cd,
             |PRS.exch_rate_pcy_to_usd_cd, PRS.hpe_ord_id, PRS.hpe_ord_itm_mcc_cd, PRS.hpe_ord_itm_nr, PRS.hpe_ord_dt, PRS.hpe_ord_itm_disc_lcy_ext_cd,
             |PRS.hpe_ord_itm_disc_usd_ext_cd, PRS.hpe_upfrnt_deal_id, PRS.hpe_ord_itm_cleansed_deal_id, PRS.hpe_upfrnt_deal_typ_cd, PRS.hpe_ord_itm_orig_deal_id,
             |PRS.hpe_upfrnt_disc_lcy_ext_cd, PRS.hpe_upfrnt_disc_usd_ext_cd, PRS.hpe_ord_end_usr_addr_id, PRS.hpe_upfrnt_deal_end_usr_addr_id, PRS.upfrnt_deal_1_typ_cd,
             |PRS.upfrnt_deal_1_end_usr_addr_id, PRS.backend_deal_1_typ_cd, PRS.backend_deal_1_end_usr_addr_id, PRS.ptnr_prch_prc_lcy_ex_1_cd, PRS.ptnr_prch_prc_lcy_un_2_cd,
             |PRS.ptnr_prch_prc_usd_ex_1_cd, PRS.ptnr_prch_prc_usd_un_1_cd, PRS.nt_cst_aftr_rbt_lcy_ex_2_cd, PRS.nt_cst_aftr_rbt_usd_ext_cd, PRS.nt_cst_curr_cd,
             |PRS.rbt_amt_lcy_ext_cd, PRS.rbt_amt_lcy_unt_cd, PRS.rbt_amt_usd_ext_cd, PRS.rbt_amt_usd_unt_cd, PRS.ptnr_sl_prc_lcy_ext_cd, PRS.ptnr_sl_prc_lcy_un_1_cd,
             |PRS.ptnr_sl_prc_usd_ext_cd, PRS.ptnr_sl_prc_usd_un_2_cd, PRS.avrg_deal_nt_lcy_unt_nm, PRS.avrg_deal_nt_usd_unt_nm, PRS.avrg_deal_nt_lcy_ext_nm,
             |PRS.avrg_deal_nt_usd_ext_nm, PRS.avrg_deal_nt_coefficient_nm, PRS.valtn_curr_cd, PRS.ex_rate_vcy_to_usd_nm, PRS.lst_prc_vcy_unt_nm, PRS.lst_prc_vcy_ext_nm,
             |PRS.ndp_vcy_unt_nm, PRS.ndp_vcy_ext_nm, PRS.deal_nt_vcy_unt_nm, PRS.deal_nt_vcy_ext_nm, PRS.avrg_deal_nt_vcy_unt_nm, PRS.avrg_deal_nt_vcy_ext_nm,
             |PRS.rsrv_fld_1_nm, PRS.rsrv_fld_2_nm, PRS.rsrv_fld_3_nm, PRS.rsrv_fld_4_nm, PRS.rsrv_fld_5_nm, PRS.rsrv_fld_6_nm, PRS.rsrv_fld_7_nm, PRS.rsrv_fld_8_nm,
             |PRS.rsrv_fld_9_nm, PRS.rsrv_fld_10_nm, PRS.src_sys_upd_ts, PRS.src_sys_ky, PRS.lgcl_dlt_ind, PRS.ins_gmt_ts, PRS.upd_gmt_ts, PRS.src_sys_extrc_gmt_ts,
             |PRS.src_sys_btch_nr, PRS.fl_nm, PRS.ld_jb_nr, PRS.ins_ts from ea_common.ps_prs_dmnsn PRS
             |INNER JOIN psDmnsnIncTrsn PS ON PRS.trsn_i_3_id=PS.trsn_i_1_id
             |""".stripMargin).repartition($"trsn_i_3_id").persist(StorageLevel.MEMORY_AND_DISK_SER)

        logger.info(" PS_prs_Incremental_Data_Read")

      } else {

        var psPrsDmnsnIncDF = spark.sql(
          s"""
             |select PRS.ps_prs_ky, PRS.trsn_i_3_id, PRS.prnt_trsn_i_4_id, PRS.prv_trsn_i_5_id, PRS.fl_i_6_id, PRS.reporter_i_7_id, PRS.iud_fl_8_cd, PRS.valtn_trsn_i_1_id,
             |PRS.valtn_wrnn_1_cd, PRS.lst_prc_lcy_ex_1_cd, PRS.lst_prc_lcy_un_1_cd, PRS.lst_prc_usd_ex_1_cd, PRS.lst_prc_usd_un_1_cd, PRS.ndp_lcy_ex_1_cd, PRS.ndp_lcy_un_1_cd,
             |PRS.ndp_usd_ex_1_cd, PRS.ndp_usd_un_1_cd, PRS.p_1_nr, PRS.pa_dscn_1_cd, PRS.pa_dscnt_lcy_un_1_cd, PRS.pa_dscnt_usd_un_1_cd, PRS.deal_nt_lcy_ext_cd, PRS.deal_nt_lcy_un_1_cd,
             |PRS.deal_nt_usd_ext_cd, PRS.deal_nt_usd_un_1_cd, PRS.ndp_coefficien_1_cd, PRS.ndp_estmd_lcy_ext_cd, PRS.ndp_estmd_lcy_unt_cd, PRS.ndp_estmd_usd_ext_cd,
             |PRS.ndp_estmd_usd_unt_cd, PRS.asp_coefficien_1_cd, PRS.asp_lcy_ex_1_cd, PRS.asp_lcy_un_1_cd, PRS.asp_usd_ex_1_cd, PRS.asp_usd_un_1_cd, PRS.cust_app_2_cd,
             |PRS.deal_geo_cd, PRS.dscnt_geo_cd, PRS.used_prc_ds_1_cd, PRS.used_cur_1_cd, PRS.upfrnt_deal_1__1_cd, PRS.upfrnt_deal_1_ln_it_1_cd, PRS.upfrnt_deal_1_vrs_1_cd,
             |PRS.upfrnt_deal_1_prc_lcy_un_1_cd, PRS.upfrnt_deal_1_prc_usd_un_1_cd, PRS.upfrnt_deal_1_dscnt_lcy_ex_1_cd, PRS.upfrnt_deal_1_dscnt_lcy_un_1_cd,
             |PRS.upfrnt_deal_1_dscnt_usd_ex_1_cd, PRS.upfrnt_deal_1_dscnt_usd_un_1_cd, PRS.upfrnt_deal_2_m_cd, PRS.upfrnt_deal_2_ln_itm_cd, PRS.upfrnt_deal_2_vrsn_cd,
             |PRS.upfrnt_deal_2_prc_lcy_unt_cd, PRS.upfrnt_deal_2_prc_usd_unt_cd, PRS.upfrnt_deal_2_dscnt_lcy_ext_cd, PRS.upfrnt_deal_2_dscnt_lcy_unt_cd,
             |PRS.upfrnt_deal_2_dscnt_usd_ext_cd, PRS.upfrnt_deal_2_dscnt_usd_unt_cd, PRS.upfrnt_deal_3_m_cd, PRS.upfrnt_deal_3_ln_itm_cd, PRS.upfrnt_deal_3_vrsn_cd,
             |PRS.upfrnt_deal_3_prc_lcy_unt_cd, PRS.upfrnt_deal_3_prc_usd_unt_cd, PRS.upfrnt_deal_3_dscnt_lcy_ext_cd, PRS.upfrnt_deal_3_dscnt_lcy_unt_cd,
             |PRS.upfrnt_deal_3_dscnt_usd_ext_cd, PRS.upfrnt_deal_3_dscnt_usd_unt_cd, PRS.backend_deal_1_m_cd, PRS.backend_deal_1_ln_itm_cd, PRS.backend_deal_1_vrsn_cd,
             |PRS.backend_deal_1_prc_lcy_unt_cd, PRS.backend_deal_1_prc_usd_unt_cd, PRS.backend_deal_1_dscnt_lcy_ext_cd, PRS.backend_deal_1_dscnt_lcy_unt_cd,
             |PRS.backend_deal_1_dscnt_usd_ext_cd, PRS.backend_deal_1_dscnt_usd_unt_cd, PRS.backend_deal_2_m_cd, PRS.backend_deal_2_ln_itm_cd, PRS.backend_deal_2_vrsn_cd,
             |PRS.backend_deal_2_prc_lcy_unt_cd, PRS.backend_deal_2_prc_usd_unt_cd, PRS.backend_deal_2_dscnt_lcy_ext_cd, PRS.backend_deal_2_dscnt_lcy_unt_cd,
             |PRS.backend_deal_2_dscnt_usd_ext_cd, PRS.backend_deal_2_dscnt_usd_unt_cd, PRS.backend_deal_3_m_cd, PRS.backend_deal_3_ln_itm_cd, PRS.backend_deal_3_vrsn_cd,
             |PRS.backend_deal_3_prc_lcy_unt_cd, PRS.backend_deal_3_prc_usd_unt_cd, PRS.backend_deal_3_dscnt_lcy_ext_cd, PRS.backend_deal_3_dscnt_lcy_unt_cd,
             |PRS.backend_deal_3_dscnt_usd_ext_cd, PRS.backend_deal_3_dscnt_usd_unt_cd, PRS.backend_deal_4_m_cd, PRS.backend_deal_4_ln_itm_cd, PRS.backend_deal_4_vrsn_cd,
             |PRS.backend_deal_4_prc_lcy_unt_cd, PRS.backend_deal_4_prc_usd_unt_cd, PRS.backend_deal_4_dscnt_lcy_ext_cd, PRS.backend_deal_4_dscnt_lcy_unt_cd,
             |PRS.backend_deal_4_dscnt_usd_ext_cd, PRS.backend_deal_4_dscnt_usd_unt_cd, PRS.backend_rbt_1_lcy_unt_cd, PRS.backend_rbt_1_usd_unt_cd, PRS.backend_rbt_2_lcy_unt_cd,
             |PRS.backend_rbt_2_usd_unt_cd, PRS.backend_rbt_3_lcy_unt_cd, PRS.backend_rbt_3_usd_unt_cd, PRS.backend_rbt_4_lcy_unt_cd, PRS.backend_rbt_4_usd_unt_cd,
             |PRS.exch_rate_lcy_to_us_1_cd, PRS.exch_rate_pcy_to_usd_cd, PRS.hpe_ord_id, PRS.hpe_ord_itm_mcc_cd, PRS.hpe_ord_itm_nr, PRS.hpe_ord_dt, PRS.hpe_ord_itm_disc_lcy_ext_cd,
             |PRS.hpe_ord_itm_disc_usd_ext_cd, PRS.hpe_upfrnt_deal_id, PRS.hpe_ord_itm_cleansed_deal_id, PRS.hpe_upfrnt_deal_typ_cd, PRS.hpe_ord_itm_orig_deal_id,
             |PRS.hpe_upfrnt_disc_lcy_ext_cd, PRS.hpe_upfrnt_disc_usd_ext_cd, PRS.hpe_ord_end_usr_addr_id, PRS.hpe_upfrnt_deal_end_usr_addr_id, PRS.upfrnt_deal_1_typ_cd,
             |PRS.upfrnt_deal_1_end_usr_addr_id, PRS.backend_deal_1_typ_cd, PRS.backend_deal_1_end_usr_addr_id, PRS.ptnr_prch_prc_lcy_ex_1_cd, PRS.ptnr_prch_prc_lcy_un_2_cd,
             |PRS.ptnr_prch_prc_usd_ex_1_cd, PRS.ptnr_prch_prc_usd_un_1_cd, PRS.nt_cst_aftr_rbt_lcy_ex_2_cd, PRS.nt_cst_aftr_rbt_usd_ext_cd, PRS.nt_cst_curr_cd,
             |PRS.rbt_amt_lcy_ext_cd, PRS.rbt_amt_lcy_unt_cd, PRS.rbt_amt_usd_ext_cd, PRS.rbt_amt_usd_unt_cd, PRS.ptnr_sl_prc_lcy_ext_cd, PRS.ptnr_sl_prc_lcy_un_1_cd,
             |PRS.ptnr_sl_prc_usd_ext_cd, PRS.ptnr_sl_prc_usd_un_2_cd, PRS.avrg_deal_nt_lcy_unt_nm, PRS.avrg_deal_nt_usd_unt_nm, PRS.avrg_deal_nt_lcy_ext_nm, PRS.avrg_deal_nt_usd_ext_nm,
             |PRS.avrg_deal_nt_coefficient_nm, PRS.valtn_curr_cd, PRS.ex_rate_vcy_to_usd_nm, PRS.lst_prc_vcy_unt_nm, PRS.lst_prc_vcy_ext_nm, PRS.ndp_vcy_unt_nm, PRS.ndp_vcy_ext_nm,
             |PRS.deal_nt_vcy_unt_nm, PRS.deal_nt_vcy_ext_nm, PRS.avrg_deal_nt_vcy_unt_nm, PRS.avrg_deal_nt_vcy_ext_nm, PRS.rsrv_fld_1_nm, PRS.rsrv_fld_2_nm, PRS.rsrv_fld_3_nm,
             |PRS.rsrv_fld_4_nm, PRS.rsrv_fld_5_nm, PRS.rsrv_fld_6_nm, PRS.rsrv_fld_7_nm, PRS.rsrv_fld_8_nm, PRS.rsrv_fld_9_nm, PRS.rsrv_fld_10_nm, PRS.src_sys_upd_ts, PRS.src_sys_ky,
             |PRS.lgcl_dlt_ind, PRS.ins_gmt_ts, PRS.upd_gmt_ts, PRS.src_sys_extrc_gmt_ts, PRS.src_sys_btch_nr, PRS.fl_nm, PRS.ld_jb_nr, PRS.ins_ts
             |from ea_common.ps_prs_dmnsn PRS
             |""".stripMargin)

        var psPrsHistDF = spark.sql(
          s"""
             |select cast(NULL as bigint) as ps_prs_ky, CASE WHEN trans_id_zyme_trans_id  not like '%-%' THEN cast(trans_id_zyme_trans_id as bigint)
             |WHEN trans_id_zyme_trans_id like '%-%' THEN Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint) END as trsn_i_3_id, NULL as prnt_trsn_i_4_id,
             |NULL as prv_trsn_i_5_id, NULL as fl_i_6_id, co_id_seller_co_id as reporter_i_7_id, NULL as iud_fl_8_cd, NULL as valtn_trsn_i_1_id, NULL as valtn_wrnn_1_cd,
             |cast(shipped_lp_seller_lc_cd as decimal(23,2)) as lst_prc_lcy_ex_1_cd, NULL as lst_prc_lcy_un_1_cd, cast(shipped_lp_usd_cd as decimal(23,2)) as lst_prc_usd_ex_1_cd,
             |NULL as lst_prc_usd_un_1_cd, cast(shipped_ndp_seller_lc_cd as decimal(23,2)) as ndp_lcy_ex_1_cd, NULL as ndp_lcy_un_1_cd,
             |cast(shipped_ndp_usd_cd as decimal(23,2)) as ndp_usd_ex_1_cd, NULL as ndp_usd_un_1_cd, purchase_agreement_no_cd as p_1_nr,
             |cast(purchase_agrmt_disc_percent_cd as decimal(23,2)) as pa_dscn_1_cd, NULL as pa_dscnt_lcy_un_1_cd, NULL as pa_dscnt_usd_un_1_cd,
             |cast(value_dealnet_lc_cd as decimal(23,2)) as deal_nt_lcy_ext_cd, NULL as deal_nt_lcy_un_1_cd, cast(value_dealnet_usd_cd as decimal(23,2)) as deal_nt_usd_ext_cd,
             |NULL as deal_nt_usd_un_1_cd, NULL as ndp_coefficien_1_cd, NULL as ndp_estmd_lcy_ext_cd, NULL as ndp_estmd_lcy_unt_cd, NULL as ndp_estmd_usd_ext_cd,
             |NULL as ndp_estmd_usd_unt_cd, cast(asp_coefficient_cd as decimal(23,2)) as asp_coefficien_1_cd, cast(shipped_asp_seller_lc_cd as decimal(23,2)) as asp_lcy_ex_1_cd,
             |NULL as asp_lcy_un_1_cd, cast(shipped_asp_usd_cd as decimal(23,2)) as asp_usd_ex_1_cd, NULL as asp_usd_un_1_cd, NULL as cust_app_2_cd, deal_geo_cd as deal_geo_cd,
             |discount_geo_cd as dscnt_geo_cd, used_price_descriptor_cd as used_prc_ds_1_cd, NULL as used_cur_1_cd, upfront_deal_m_code_1_cd as upfrnt_deal_1__1_cd,
             |cast(upfront_deal_line_item_1_cd as varchar(100)) as upfrnt_deal_1_ln_it_1_cd, upfront_deal1_version_number_nr as upfrnt_deal_1_vrs_1_cd,
             |NULL as upfrnt_deal_1_prc_lcy_un_1_cd, NULL as upfrnt_deal_1_prc_usd_un_1_cd, cast(value_upfront_deal1_disc_lc_cd as decimal(23,2)) as upfrnt_deal_1_dscnt_lcy_ex_1_cd,
             |NULL as upfrnt_deal_1_dscnt_lcy_un_1_cd, cast(value_upfront_deal1_disc_usd_cd as decimal(23,2)) as upfrnt_deal_1_dscnt_usd_ex_1_cd, NULL as upfrnt_deal_1_dscnt_usd_un_1_cd,
             |upfront_deal_m_code_2_cd as upfrnt_deal_2_m_cd, cast(upfront_deal_line_item_2_cd as varchar(100)) as upfrnt_deal_2_ln_itm_cd,
             |upfront_deal2_version_number_nr as upfrnt_deal_2_vrsn_cd, NULL as upfrnt_deal_2_prc_lcy_unt_cd, NULL as upfrnt_deal_2_prc_usd_unt_cd,
             |cast(value_upfront_deal2_disc_lc_cd as decimal(23,2)) as upfrnt_deal_2_dscnt_lcy_ext_cd, NULL as upfrnt_deal_2_dscnt_lcy_unt_cd,
             |cast(value_upfront_deal2_disc_usd_cd as decimal(23,2)) as upfrnt_deal_2_dscnt_usd_ext_cd, NULL as upfrnt_deal_2_dscnt_usd_unt_cd,
             |upfront_deal_m_code_3_cd as upfrnt_deal_3_m_cd, cast(upfront_deal_line_item_3_cd as varchar(100)) as upfrnt_deal_3_ln_itm_cd,
             |upfront_deal3_version_number_nr as upfrnt_deal_3_vrsn_cd, NULL as upfrnt_deal_3_prc_lcy_unt_cd, NULL as upfrnt_deal_3_prc_usd_unt_cd,
             |cast(value_upfront_deal3_disc_lc_cd as decimal(23,2)) as upfrnt_deal_3_dscnt_lcy_ext_cd, NULL as upfrnt_deal_3_dscnt_lcy_unt_cd,
             |cast(value_upfront_deal3_disc_usd_cd as decimal(23,2))  as upfrnt_deal_3_dscnt_usd_ext_cd, NULL as upfrnt_deal_3_dscnt_usd_unt_cd,
             |back_end_deal_m_code_1_cd as backend_deal_1_m_cd, cast(back_end_deal_line_item_1_cd as varchar(100)) as backend_deal_1_ln_itm_cd,
             |backend_deal1_version_number_nr as backend_deal_1_vrsn_cd, NULL as backend_deal_1_prc_lcy_unt_cd, NULL as backend_deal_1_prc_usd_unt_cd,
             |cast(value_backend_deal1_disc_lc_cd as decimal(23,2)) as backend_deal_1_dscnt_lcy_ext_cd, NULL as backend_deal_1_dscnt_lcy_unt_cd,
             |cast(value_backend_deal1_disc_usd_cd as decimal(23,2)) as backend_deal_1_dscnt_usd_ext_cd, NULL as backend_deal_1_dscnt_usd_unt_cd,
             |back_end_deal_m_code_2_cd as backend_deal_2_m_cd,cast(back_end_deal_line_item_2_cd as varchar(100) ) as backend_deal_2_ln_itm_cd,
             |backend_deal2_version_number_nr as backend_deal_2_vrsn_cd, NULL as backend_deal_2_prc_lcy_unt_cd, NULL as backend_deal_2_prc_usd_unt_cd,
             |cast(value_backend_deal2_disc_lc_cd as decimal(23,2)) as backend_deal_2_dscnt_lcy_ext_cd, NULL as backend_deal_2_dscnt_lcy_unt_cd,
             |cast(value_backend_deal2_disc_usd_cd as decimal(23,2)) as backend_deal_2_dscnt_usd_ext_cd, NULL as backend_deal_2_dscnt_usd_unt_cd,
             |back_end_deal_m_code_3_cd as backend_deal_3_m_cd,cast(back_end_deal_line_item_3_cd as varchar(100)) as backend_deal_3_ln_itm_cd,
             |backend_deal3_version_number_nr as backend_deal_3_vrsn_cd, NULL as backend_deal_3_prc_lcy_unt_cd, NULL as backend_deal_3_prc_usd_unt_cd,
             |cast(value_backend_deal3_disc_lc_cd as decimal(23,2)) as backend_deal_3_dscnt_lcy_ext_cd, NULL as backend_deal_3_dscnt_lcy_unt_cd,
             |cast(value_backend_deal3_disc_usd_cd as decimal(23,2)) as backend_deal_3_dscnt_usd_ext_cd, NULL as backend_deal_3_dscnt_usd_unt_cd,
             |back_end_deal_m_code_4_cd as backend_deal_4_m_cd,cast(back_end_deal_line_item_4_cd as varchar(100)) as backend_deal_4_ln_itm_cd,
             |backend_deal4_version_number_nr as backend_deal_4_vrsn_cd, NULL as backend_deal_4_prc_lcy_unt_cd, NULL as backend_deal_4_prc_usd_unt_cd,
             |cast(value_backend_deal4_disc_lc_cd as decimal(23,2)) as backend_deal_4_dscnt_lcy_ext_cd, NULL as backend_deal_4_dscnt_lcy_unt_cd,
             |cast(value_backend_deal4_disc_usd_cd as decimal(23,2)) as backend_deal_4_dscnt_usd_ext_cd, NULL as backend_deal_4_dscnt_usd_unt_cd,
             |NULL as backend_rbt_1_lcy_unt_cd, NULL as backend_rbt_1_usd_unt_cd, NULL as backend_rbt_2_lcy_unt_cd, NULL as backend_rbt_2_usd_unt_cd,
             |NULL as backend_rbt_3_lcy_unt_cd, NULL as backend_rbt_3_usd_unt_cd, NULL as backend_rbt_4_lcy_unt_cd, NULL as backend_rbt_4_usd_unt_cd,
             |exchange_rate_lcy_usd_cd as exch_rate_lcy_to_us_1_cd, exchange_rate_pcy_usd_cd as exch_rate_pcy_to_usd_cd, NULL as hpe_ord_id, NULL as hpe_ord_itm_mcc_cd,
             |NULL as hpe_ord_itm_nr, NULL as hpe_ord_dt, NULL as hpe_ord_itm_disc_lcy_ext_cd, NULL as hpe_ord_itm_disc_usd_ext_cd, NULL as hpe_upfrnt_deal_id,
             |NULL as hpe_ord_itm_cleansed_deal_id, NULL as hpe_upfrnt_deal_typ_cd, NULL as hpe_ord_itm_orig_deal_id, NULL as hpe_upfrnt_disc_lcy_ext_cd,
             |NULL as hpe_upfrnt_disc_usd_ext_cd, NULL as hpe_ord_end_usr_addr_id, NULL as hpe_upfrnt_deal_end_usr_addr_id, NULL as upfrnt_deal_1_typ_cd,
             |NULL as upfrnt_deal_1_end_usr_addr_id, NULL as backend_deal_1_typ_cd, NULL as backend_deal_1_end_usr_addr_id,
             |cast(value_ptnr_purch_price_pc_cd as decimal(23,2)) as ptnr_prch_prc_lcy_ex_1_cd, NULL as ptnr_prch_prc_lcy_un_2_cd,
             |cast(value_ptnr_purch_price_usd_cd as decimal(23,2)) as ptnr_prch_prc_usd_ex_1_cd, NULL as ptnr_prch_prc_usd_un_1_cd,
             |ext_net_cost_after_rebate_pc_cd as nt_cst_aftr_rbt_lcy_ex_2_cd, cast(ext_net_cost_after_rebate_usd_cd as decimal(23,2)) as nt_cst_aftr_rbt_usd_ext_cd,
             |NULL as nt_cst_curr_cd,cast(value_rebate_amount_pc_cd as decimal(23,2)) as rbt_amt_lcy_ext_cd, NULL as rbt_amt_lcy_unt_cd,
             |cast(value_rebate_amount_usd_cd as decimal(23,2)) as rbt_amt_usd_ext_cd, NULL as rbt_amt_usd_unt_cd, ptnr_rpt_price_lc_cd as ptnr_sl_prc_lcy_ext_cd,
             |NULL as ptnr_sl_prc_lcy_un_1_cd, ptnr_rpt_price_usd_cd as ptnr_sl_prc_usd_ext_cd, NULL as ptnr_sl_prc_usd_un_2_cd, NULL as avrg_deal_nt_lcy_unt_nm,
             |NULL as avrg_deal_nt_usd_unt_nm, NULL as avrg_deal_nt_lcy_ext_nm, NULL as avrg_deal_nt_usd_ext_nm, NULL as avrg_deal_nt_coefficient_nm, NULL as valtn_curr_cd,
             |NULL as ex_rate_vcy_to_usd_nm, NULL as lst_prc_vcy_unt_nm, NULL as lst_prc_vcy_ext_nm, NULL as ndp_vcy_unt_nm, NULL as ndp_vcy_ext_nm, NULL as deal_nt_vcy_unt_nm,
             |NULL as deal_nt_vcy_ext_nm, NULL as avrg_deal_nt_vcy_unt_nm, NULL as avrg_deal_nt_vcy_ext_nm, NULL as rsrv_fld_1_nm, NULL as rsrv_fld_2_nm, NULL as rsrv_fld_3_nm,
             |NULL as rsrv_fld_4_nm, NULL as rsrv_fld_5_nm, NULL as rsrv_fld_6_nm, NULL as rsrv_fld_7_nm, NULL as rsrv_fld_8_nm, NULL as rsrv_fld_9_nm, NULL as rsrv_fld_10_nm,
             |src_sys_upd_ts as src_sys_upd_ts, src_sys_ky as src_sys_ky, lgcl_dlt_ind as lgcl_dlt_ind, ins_gmt_dt as ins_gmt_ts, upd_gmt_ts as upd_gmt_ts,
             |src_sys_extrc_gmt_ts as src_sys_extrc_gmt_ts, src_sys_btch_nr as src_sys_btch_nr, fl_nm as fl_nm, ld_jb_nr as ld_jb_nr, NULL as ins_ts
             |from ea_common.dwtf_sl_thru_ref  D LEFT JOIN psDmnsnIncTrsn ps ON Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint)=ps.trsn_i_1_id
             |where ps.trsn_i_1_id IS NULL and Cast(regexp_replace(trans_id_zyme_trans_id,'-','') as bigint) IS NOT NULL AND pdw_src_sys_ky_cd='ASPEN'
             |""".stripMargin)
          
        ps_prs_dmnsnDF = psPrsDmnsnIncDF.union(psPrsHistDF).repartition($"trsn_i_3_id")
        logger.info("PS_prs_History_Data_Read")
      }
      ps_prs_dmnsnDF.createOrReplaceTempView("ps_prs_dmnsnDF")

      //**** BMT Transaction Update*****
      val bmtTrsnCnt = spark.sql(s"""select * from ea_common.bmt_bi_transaction_update_dmnsn """)
      bmtTrsnCnt.createOrReplaceTempView("bmtTrsnCnt")
      val bmtTrsnCount = bmtTrsnCnt.count().toInt
      logger.info("****bmtTrsnCount****"+bmtTrsnCount)
      
      if(bmtTrsnCount > 0)
      {
        val bmtTrnsnDF = spark.sql(
          s"""
             |SELECT /*+ BROADCAST(trsnBMT) */ PS.trsn_i_1_id, bi_trsn_xclsn_flg, trsnBMT.bi_trsn_xclsn_rsn_cd
             |FROM PsDmnsnTRSNSet PS INNER JOIN ea_common.bmt_bi_transaction_update_dmnsn trsnBMT
             |ON PS.trsn_i_1_id = trsn_id where (UPPER(trim(trsnBMT.rec_src))='POS' or trsnBMT.rec_src is null
             |or trsnBMT.rec_src='' or UPPER(trim(trsnBMT.rec_src))='E2OPEN')
             |""".stripMargin)
        // val bmtTrnsnDF_load = bmtTrnsnDF.repartition(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "bmtTrnsnDF")
        logger.info("BI Transaction BMT Dimension DF created")
      }
      else
      {
        logger.info("****NO_DATA_IN_BMTTRSN****"+bmtTrsnCount)
      }

      //****bmt_bi_distri_reseller_reporter_dmnsn****
      val bmt_bi_distri_reseller_reporter_dmnsn = spark.sql(
        s"""
           |select bmt_bi_distri_reseller_reporter_ky,
           |eff_upd_strt_dt,
           |eff_upd_end_dt,
           |reporter_id,
           |reporter_party_id,
           |reporter_br_type,
           |reporting_type,
           |e2open_trsn_id,
           |rec_src,
           |bi_dstr_party_id,
           |bi_dstr_ctry_lvl_grp_id,
           |bi_dstr_br_typ,
           |bi_dstr_ctry_cd,
           |bi_dstr_lgcy_hq_pro_id,
           |bi_dstr_lgcy_pro_id,
           |bi_dstr_aruba_stck_flg,
           |bi_dstr_ww_mbrshp,
           |bi_dstr_prnt_prty_id,
           |bi_dstr_prnt_prty_nm,
           |bi_dstr_affiliate_prty_id,
           |bi_dstr_affiliate_prty_nm,
           |bi_rslr_party_id,
           |bi_rslr_ctry_lvl_grp_id,
           |bi_rslr_br_typ,
           |bi_rslr_ctry_cd,
           |bi_rslr_lgcy_pro_id,
           |bi_rslr_lgcy_hq_pro_id,
           |bi_rslr_aruba_stck_flg,
           |bi_rslr_ww_mbrshp,
           |bi_rslr_prnt_prty_id,
           |bi_rslr_prnt_prty_nm,
           |bi_rslr_affiliate_prty_id,
           |bi_rslr_affiliate_prty_nm,
           |bi_rslr_ctry_lvl_mbrshp_aruba,
           |bi_rslr_ctry_lvl_mbrshp_hybrid_it,
           |bi_rslr_ctry_lvl_mbrshp_pointnext,
           |bi_rslr_lgcy_mbrshp_edge,
           |bi_rslr_lgcy_mbrshp_hybrid_it,
           |bi_rslr_lgcy_ww_mbrshp,
           |reserved_fld_1,
           |reserved_fld_2,
           |reserved_fld_3,
           |reserved_fld_4,
           |reserved_fld_5,
           |reserved_fld_6,
           |reserved_fld_7,
           |reserved_fld_8,
           |reserved_fld_9,
           |reserved_fld_10,
           |src_sys_upd_ts,
           |src_sys_ky,
           |lgcl_dlt_ind,
           |ins_gmt_ts,
           |upd_gmt_ts,
           |src_sys_extrc_gmt_ts,
           |src_sys_btch_nr,
           |fl_nm,
           |ld_jb_nr,
           |ins_ts from ea_common.bmt_bi_distri_reseller_reporter_dmnsn where (UPPER(rec_src)='POS' or UPPER(rec_src)='ALL' or rec_src is null or rec_src=''
           |or UPPER(rec_src)='E2OPEN')
           |""".stripMargin)
      bmt_bi_distri_reseller_reporter_dmnsn.createOrReplaceTempView("bmt_bi_distri_reseller_reporter_dmnsn")

      val rptg_ptnr_mstr_dmnsn_Unique_DF = spark.sql(
        s"""
           |select rptg_ptnr_mstr_ky,reporter_i_1_id,reporter_nm,rgn_cd,sb_rgn_cd,rptg_frq_cd,ctry_cd,father_reporter_id,father_prty_id,father_ent_mdm_br_typ_cd,
           |ptnr_typ_cd,reporter_strt_dt,reporter_end_dt,typ_tx_cd,top_vl_flg_cd,mdcp_br_ty_1_cd,dun_1_cd,nat_flg_cd,ptnr_pro_i_1_id,drct_flg_cd,actv_yn_cd,
           |not_cntct_flg_cd,strt_actv_dt,last_actv_dt,chnl_sgm_id,byng_slng_mtn_typ_cd,prch_agrmnt_id_1_id,prch_agrmnt_id_2_id,prch_agrmnt_id_3_id,cust_app_1_cd,
           |reporter_mdcp_id,reporter_mdcp_org_id,reporter_opsi_id,rptd_bri_id,reporter_prty_id,ent_mdm_br_ty_1_cd,rptg_typ_cd,rsrv_fld_1_cd,rsrv_fld_2_cd,rsrv_fld_3_cd,
           |rsrv_fld_4_cd,rsrv_fld_5_cd,src_sys_upd_ts,src_sys_ky,lgcl_dlt_ind,ins_gmt_ts,upd_gmt_ts,src_sys_extrc_gmt_ts,src_sys_btch_nr,fl_nm,ld_jb_nr,ins_ts,period
           |from (select rptg_ptnr_mstr_ky,reporter_i_1_id,reporter_nm,rgn_cd,sb_rgn_cd,rptg_frq_cd,ctry_cd,father_reporter_id,father_prty_id,father_ent_mdm_br_typ_cd,ptnr_typ_cd,
           |reporter_strt_dt,reporter_end_dt,typ_tx_cd,top_vl_flg_cd,mdcp_br_ty_1_cd,dun_1_cd,nat_flg_cd,ptnr_pro_i_1_id,drct_flg_cd,actv_yn_cd,not_cntct_flg_cd,strt_actv_dt,
           |last_actv_dt,chnl_sgm_id,byng_slng_mtn_typ_cd,prch_agrmnt_id_1_id,prch_agrmnt_id_2_id,prch_agrmnt_id_3_id,cust_app_1_cd,reporter_mdcp_id,reporter_mdcp_org_id,
           |reporter_opsi_id,rptd_bri_id,reporter_prty_id,ent_mdm_br_ty_1_cd,rptg_typ_cd,rsrv_fld_1_cd,rsrv_fld_2_cd,rsrv_fld_3_cd,rsrv_fld_4_cd,rsrv_fld_5_cd,src_sys_upd_ts,
           |src_sys_ky,lgcl_dlt_ind,ins_gmt_ts,upd_gmt_ts,src_sys_extrc_gmt_ts,src_sys_btch_nr,fl_nm,ld_jb_nr,ins_ts,period, row_number()over(partition by reporter_i_1_id
           |order by reporter_i_1_id)as ranking from ea_common.rptg_ptnr_mstr_dmnsn) t where t.ranking==1
           |""".stripMargin).drop("ranking")
      rptg_ptnr_mstr_dmnsn_Unique_DF.createOrReplaceTempView("rptg_ptnr_mstr_dmnsn_tmp");

      val bmtdisreserepoDF = spark.sql(
        s"""
           |select dup.* from (select rslr_repo.*,row_number() over(partition by trsn_i_1_id order by trsn_i_1_id desc )rnk
           |from (SELECT /*+ BROADCAST(dstr_rslr_rprtBMT_rptr,dstr_rslr_rprtBMT1) */
           | distinct ps.trsn_i_1_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_party_id , dstr_rslr_rprtBMT1.bi_dstr_party_id ) as bi_dstr_party_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_ctry_lvl_grp_id , dstr_rslr_rprtBMT1.bi_dstr_ctry_lvl_grp_id ) as bi_dstr_ctry_lvl_grp_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_br_typ , dstr_rslr_rprtBMT1.bi_dstr_br_typ ) as bi_dstr_br_typ,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_ctry_cd , dstr_rslr_rprtBMT1.bi_dstr_ctry_cd ) as bi_dstr_ctry_cd,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_lgcy_hq_pro_id , dstr_rslr_rprtBMT1.bi_dstr_lgcy_hq_pro_id ) as bi_dstr_lgcy_hq_pro_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_lgcy_pro_id , dstr_rslr_rprtBMT1.bi_dstr_lgcy_pro_id ) as bi_dstr_lgcy_pro_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_aruba_stck_flg , dstr_rslr_rprtBMT1.bi_dstr_aruba_stck_flg ) as bi_dstr_aruba_stck_flg,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_ww_mbrshp , dstr_rslr_rprtBMT1.bi_dstr_ww_mbrshp ) as bi_dstr_ww_mbrshp,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_party_id , dstr_rslr_rprtBMT1.bi_rslr_party_id ) as bi_rslr_party_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_ctry_lvl_grp_id , dstr_rslr_rprtBMT1.bi_rslr_ctry_lvl_grp_id ) as bi_rslr_ctry_lvl_grp_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_br_typ , dstr_rslr_rprtBMT1.bi_rslr_br_typ ) as bi_rslr_br_typ,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_ctry_cd , dstr_rslr_rprtBMT1.bi_rslr_ctry_cd ) as bi_rslr_ctry_cd,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_lgcy_pro_id , dstr_rslr_rprtBMT1.bi_rslr_lgcy_pro_id ) as bi_rslr_lgcy_pro_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_lgcy_hq_pro_id , dstr_rslr_rprtBMT1.bi_rslr_lgcy_hq_pro_id ) as bi_rslr_lgcy_hq_pro_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_aruba_stck_flg , dstr_rslr_rprtBMT1.bi_rslr_aruba_stck_flg ) as bi_rslr_aruba_stck_flg,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_ww_mbrshp , dstr_rslr_rprtBMT1.bi_rslr_ww_mbrshp) as bi_rslr_ww_mbrshp,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_affiliate_prty_id , dstr_rslr_rprtBMT1.bi_dstr_affiliate_prty_id) as bi_dstr_affiliate_prty_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_dstr_affiliate_prty_nm , dstr_rslr_rprtBMT1.bi_dstr_affiliate_prty_nm) as bi_dstr_affiliate_prty_nm,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_affiliate_prty_id , dstr_rslr_rprtBMT1.bi_rslr_affiliate_prty_id) as bi_rslr_affiliate_prty_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_affiliate_prty_nm , dstr_rslr_rprtBMT1.bi_rslr_affiliate_prty_nm) as bi_rslr_affiliate_prty_nm,
           | coalesce(dstr_rslr_rprtBMT_rptr.e2open_trsn_id , dstr_rslr_rprtBMT1.e2open_trsn_id) as e2open_trsn_id,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_ctry_lvl_mbrshp_aruba , dstr_rslr_rprtBMT1.bi_rslr_ctry_lvl_mbrshp_aruba) as bi_rslr_ctry_lvl_mbrshp_aruba,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_ctry_lvl_mbrshp_hybrid_it , dstr_rslr_rprtBMT1.bi_rslr_ctry_lvl_mbrshp_hybrid_it) as bi_rslr_ctry_lvl_mbrshp_hybrid_it,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_ctry_lvl_mbrshp_pointnext , dstr_rslr_rprtBMT1.bi_rslr_ctry_lvl_mbrshp_pointnext) as bi_rslr_ctry_lvl_mbrshp_pointnext,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_lgcy_mbrshp_edge , dstr_rslr_rprtBMT1.bi_rslr_lgcy_mbrshp_edge) as bi_rslr_lgcy_mbrshp_edge,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_lgcy_mbrshp_hybrid_it , dstr_rslr_rprtBMT1.bi_rslr_lgcy_mbrshp_hybrid_it) as bi_rslr_lgcy_mbrshp_hybrid_it,
           | coalesce(dstr_rslr_rprtBMT_rptr.bi_rslr_lgcy_ww_mbrshp , dstr_rslr_rprtBMT1.bi_rslr_lgcy_ww_mbrshp) as bi_rslr_lgcy_ww_mbrshp
           |FROM PsDmnsnSubSetTrsn ps
           |INNER JOIN rptg_ptnr_mstr_dmnsn_tmp rpm ON ps.reporter_i_2_id = rpm.reporter_i_1_id
           |Left JOIN bmt_bi_distri_reseller_reporter_dmnsn dstr_rslr_rprtBMT_rptr
           |ON rpm.reporter_i_1_id = dstr_rslr_rprtBMT_rptr.reporter_id
           |and (ps.trs_1_dt >= COALESCE(to_date(from_unixtime(unix_timestamp(dstr_rslr_rprtBMT_rptr.eff_upd_strt_dt,'YYYY-MM-DD'))),'1900-01-01')
           |AND ps.trs_1_dt <= COALESCE(to_date(from_unixtime(unix_timestamp(dstr_rslr_rprtBMT_rptr.eff_upd_end_dt,'YYYY-MM-DD'))),'9999-12-31') )
           |Left JOIN bmt_bi_distri_reseller_reporter_dmnsn dstr_rslr_rprtBMT1
           |ON rpm.reporter_prty_id = dstr_rslr_rprtBMT1.reporter_party_id
           |AND rpm.rptg_typ_cd = dstr_rslr_rprtBMT1.reporting_type
           |and (coalesce(upper(trim(rpm.ent_mdm_br_ty_1_cd)),'NA') = upper(trim(dstr_rslr_rprtBMT1.reporter_br_type)) or upper(trim(dstr_rslr_rprtBMT1.reporter_br_type)) IS NULL)
           |and (ps.trs_1_dt >= COALESCE(to_date(from_unixtime(unix_timestamp(dstr_rslr_rprtBMT1.eff_upd_strt_dt,'YYYY-MM-DD'))),'1900-01-01')
           |AND ps.trs_1_dt <= COALESCE(to_date(from_unixtime(unix_timestamp(dstr_rslr_rprtBMT1.eff_upd_end_dt,'YYYY-MM-DD'))),'9999-12-31') )
           |UNION
           |SELECT /*+ BROADCAST(dstr_rslr_rprtBMT2) */
           |distinct ps.trsn_i_1_id,
           |dstr_rslr_rprtBMT2.bi_dstr_party_id,
           | dstr_rslr_rprtBMT2.bi_dstr_ctry_lvl_grp_id,
           | dstr_rslr_rprtBMT2.bi_dstr_br_typ,
           | dstr_rslr_rprtBMT2.bi_dstr_ctry_cd,
           | dstr_rslr_rprtBMT2.bi_dstr_lgcy_hq_pro_id,
           | dstr_rslr_rprtBMT2.bi_dstr_lgcy_pro_id,
           | dstr_rslr_rprtBMT2.bi_dstr_aruba_stck_flg,
           | dstr_rslr_rprtBMT2.bi_dstr_ww_mbrshp,
           | dstr_rslr_rprtBMT2.bi_rslr_party_id,
           | dstr_rslr_rprtBMT2.bi_rslr_ctry_lvl_grp_id,
           | dstr_rslr_rprtBMT2.bi_rslr_br_typ,
           | dstr_rslr_rprtBMT2.bi_rslr_ctry_cd,
           | dstr_rslr_rprtBMT2.bi_rslr_lgcy_pro_id,
           | dstr_rslr_rprtBMT2.bi_rslr_lgcy_hq_pro_id,
           | dstr_rslr_rprtBMT2.bi_rslr_aruba_stck_flg,
           | dstr_rslr_rprtBMT2.bi_rslr_ww_mbrshp,
           | dstr_rslr_rprtBMT2.bi_dstr_affiliate_prty_id,
           | dstr_rslr_rprtBMT2.bi_dstr_affiliate_prty_nm,
           | dstr_rslr_rprtBMT2.bi_rslr_affiliate_prty_id,
           | dstr_rslr_rprtBMT2.bi_rslr_affiliate_prty_nm,
           | dstr_rslr_rprtBMT2.e2open_trsn_id ,
           |dstr_rslr_rprtBMT2.bi_rslr_ctry_lvl_mbrshp_aruba      ,
           |dstr_rslr_rprtBMT2.bi_rslr_ctry_lvl_mbrshp_hybrid_it ,
           |dstr_rslr_rprtBMT2.bi_rslr_ctry_lvl_mbrshp_pointnext ,
           |dstr_rslr_rprtBMT2.bi_rslr_lgcy_mbrshp_edge          ,
           |dstr_rslr_rprtBMT2.bi_rslr_lgcy_mbrshp_hybrid_it     ,
           |dstr_rslr_rprtBMT2.bi_rslr_lgcy_ww_mbrshp
           |FROM PsDmnsnSubSetTrsn ps
           |INNER JOIN bmt_bi_distri_reseller_reporter_dmnsn dstr_rslr_rprtBMT2
           |ON ps.trsn_i_1_id = dstr_rslr_rprtBMT2.e2open_trsn_id) rslr_repo)dup where rnk=1 AND (bi_dstr_party_id IS NOT NULL OR bi_dstr_ctry_lvl_grp_id IS NOT NULL
           |OR bi_dstr_br_typ IS NOT NULL OR bi_dstr_ctry_cd IS NOT NULL OR bi_dstr_lgcy_hq_pro_id IS NOT NULL OR bi_dstr_lgcy_pro_id IS NOT NULL OR
           |bi_dstr_aruba_stck_flg IS NOT NULL OR bi_dstr_ww_mbrshp IS NOT NULL OR bi_rslr_party_id IS NOT NULL OR bi_rslr_ctry_lvl_grp_id IS NOT NULL OR
           |bi_rslr_br_typ IS NOT NULL OR bi_rslr_ctry_cd IS NOT NULL OR bi_rslr_lgcy_pro_id IS NOT NULL OR bi_rslr_lgcy_hq_pro_id IS NOT NULL OR bi_rslr_aruba_stck_flg IS NOT NULL
           |OR bi_rslr_ww_mbrshp IS NOT NULL OR bi_dstr_affiliate_prty_id IS NOT NULL OR bi_dstr_affiliate_prty_nm IS NOT NULL OR bi_rslr_affiliate_prty_id IS NOT NULL
           |OR bi_rslr_affiliate_prty_nm IS NOT NULL OR e2open_trsn_id IS NOT NULL OR bi_rslr_ctry_lvl_mbrshp_aruba IS NOT NULL OR bi_rslr_ctry_lvl_mbrshp_hybrid_it IS NOT NULL
           |OR bi_rslr_ctry_lvl_mbrshp_pointnext IS NOT NULL OR bi_rslr_lgcy_mbrshp_edge IS NOT NULL OR bi_rslr_lgcy_mbrshp_hybrid_it IS NOT NULL
           |OR bi_rslr_lgcy_ww_mbrshp IS NOT NULL)
           |""".stripMargin).drop("rnk")
      //bmtdisreserepoDF.createOrReplaceTempView("bmtdisreserepoDF")
      // val bmtdisreserepoDF_load = bmtdisreserepoDF.repartition(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "bmtdisreserepoDF")
      logger.info("Distributor/Reseller reporter bmt DF created")
      val distributorTrsn = spark.sql(
        s"""
           |select /*+ BROADCAST(dstrBmt) */
           |trsn_i_1_id,ptnr_inv_nr,BI_dstr_prty_id,BI_dstr_rsn_cd,dstr_lgcy_pro_id,dstr_lgcy_pro_hqid,BI_Dstr_BR_typ_nm,coalesce(BI_dstr_ctr_cd,PRTY.prty_ctry_cd) as BI_dstr_ctr_cd
           |from (select distinct
           |ps.trsn_i_1_id,ps.ptnr_inv_nr,
           | CASE WHEN dstrBmt.bi_dstr_party_id IS NOT NULL THEN dstrBmt.bi_dstr_party_id
           |WHEN (OPE.ope_dstr_prty_id IS NOT NULL AND UPPER(OPE.ope_dstr_prty_id) <> 'NO DISTRIBUTOR') THEN OPE.ope_dstr_prty_id
           |WHEN (OPE.ope_dstr_prty_id IS NOT NULL AND UPPER(OPE.ope_dstr_prty_id) = 'NO DISTRIBUTOR') THEN 'NO DISTRIBUTOR'
           |END AS BI_dstr_prty_id,
           |CASE WHEN dstrBmt.bi_dstr_party_id IS NOT NULL  or dstrBmt.bi_dstr_br_typ is not null or dstrBmt.bi_dstr_ctry_cd is not null  THEN 'BI Distributor REPORTER BMT'
           |WHEN (ope.ope_dstr_prty_id IS NOT NULL AND UPPER(OPE.ope_dstr_prty_id) <> 'NO DISTRIBUTOR') OR (ope.ope_sldt_prty_id IS NOT NULL AND UPPER(OPE.ope_sldt_prty_id) <> 'NO DISTRIBUTOR' AND OPE.ope_sldt_prty_id <> '') THEN OPE.ope_dstr_rsn_cd ELSE 'NO DISTRIBUTOR'
           |END AS BI_dstr_rsn_cd,
           |CASE WHEN dstrBmt.bi_dstr_lgcy_pro_id is not null THEN dstrBmt.bi_dstr_lgcy_pro_id
           | WHEN ope.ope_dstr_rsn_cd ='REPORTER' THEN RPM.ptnr_pro_i_1_id  ELSE NULL  END AS dstr_lgcy_pro_id,
           |CASE
           |WHEN dstrBmt.bi_dstr_lgcy_pro_id is not null THEN dstrBmt.bi_dstr_lgcy_hq_pro_id
           |WHEN OPE.ope_dstr_rsn_cd ='REPORTER' THEN DstrSblHrchy.BI_lgcy_hqid ELSE NULL  END AS dstr_lgcy_pro_hqid,
           |CASE WHEN dstrBmt.bi_dstr_br_typ IS NOT NULL THEN dstrBmt.bi_dstr_br_typ ELSE OPE.ope_dstr_br_typ END AS BI_Dstr_BR_typ_nm,
           |dstrBmt.bi_dstr_ctry_cd AS BI_dstr_ctr_cd
           |FROM PsDmnsnTRSNSet ps
           |LEFT JOIN OpePsDmnsnSubSet ope ON ps.trsn_i_1_id=OPE.trsn_i_1_id
           |LEFT JOIN rptg_ptnr_mstr_dmnsn_tmp rpm ON ps.reporter_i_2_id = rpm.reporter_i_1_id
           |LEFT JOIN ea_support_temp_an.bmtdisreserepoDF dstrBmt ON ps.trsn_i_1_id = dstrBmt.trsn_i_1_id
           |LEFT JOIN ea_common.chnlptnr_legacy DstrSblHrchy
           |ON RPM.ptnr_pro_i_1_id  = DstrSblHrchy.ptnr_pfl_row_id)A
           |LEFT JOIN chnlptnr_common_prty_dmnsn prty ON prty.mdm_id=A.bi_dstr_prty_id where BI_dstr_prty_id IS NOT NULL OR dstr_lgcy_pro_id IS NOT NULL or dstr_lgcy_pro_hqid is not null
           |""".stripMargin).repartition($"trsn_i_1_id").persist(StorageLevel.MEMORY_AND_DISK_SER)
      distributorTrsn.createOrReplaceTempView("distributorTrsn")
      logger.info("distributorTrsn View Created")

      val distributorDF = spark.sql(
        s"""
           |select
           |BI_dstr_ctry_Lvl_grp_id,
           |BI_dstr_ctr_cd,
           |BI_dstr_prty_id,
           |BI_Dstr_BR_typ_nm,
           |BI_dstr_Aruba_STCK_flg_cd,
           |bi_dstr_affiliate_prty_id,
           |bi_dstr_affiliate_prty_nm
           |from
           |(select BI_dstr_ctry_Lvl_grp_id,BI_dstr_ctr_cd,BI_dstr_prty_id,BI_Dstr_BR_typ_nm,
           |BI_dstr_Aruba_STCK_flg_cd,bi_dstr_affiliate_prty_id,bi_dstr_affiliate_prty_nm,
           |row_number() over (partition by BI_dstr_prty_id,BI_Dstr_BR_typ_nm order by BI_dstr_prty_id desc,BI_Dstr_BR_typ_nm desc) rk
           |from
           |(select
           |CASE WHEN dstrBmt.bi_dstr_ctry_lvl_grp_id IS NOT NULL THEN dstrBmt.bi_dstr_ctry_lvl_grp_id
           |WHEN cast(prty.ctry_lvl_grp_id as string) IS NOT NULL THEN  cast(prty.ctry_lvl_grp_id as string)
           |END AS BI_dstr_ctry_Lvl_grp_id,
           |CASE WHEN dstrBmt.bi_dstr_ctry_cd IS NOT NULL THEN dstrBmt.bi_dstr_ctry_cd
           |when PRTY.prty_ctry_cd IS NOT NULL THEN PRTY.prty_ctry_cd
           |END AS BI_dstr_ctr_cd ,
           |dst.bi_dstr_prty_id,
           |dst.BI_Dstr_BR_typ_nm,
           |CASE WHEN dstrBmt.bi_dstr_party_id IS NOT NULL OR (ope.ope_dstr_prty_id IS NOT NULL AND UPPER(OPE.ope_dstr_prty_id) <> 'NO DISTRIBUTOR')
           |OR (ope.ope_sldt_prty_id IS NOT NULL AND UPPER(OPE.ope_sldt_prty_id) <> 'NO DISTRIBUTOR' AND OPE.ope_sldt_prty_id <> '')
           |THEN dstrBmt.bi_dstr_aruba_stck_flg END AS BI_dstr_Aruba_STCK_flg_cd,
           |dstrBmt.bi_dstr_affiliate_prty_id,
           |dstrBmt.bi_dstr_affiliate_prty_nm
           |FROM distributorTrsn dst
           |LEFT JOIN OpePsDmnsnSubSet ope ON dst.trsn_i_1_id=OPE.trsn_i_1_id
           |LEFT JOIN chnlptnr_common_prty_dmnsn prty ON prty.mdm_id=dst.bi_dstr_prty_id
           |LEFT JOIN ea_support_temp_an.bmtdisreserepoDF dstrBmt ON dst.trsn_i_1_id = dstrBmt.trsn_i_1_id)A )B where B.rk=1
           |""".stripMargin)

      distributorDF.createOrReplaceTempView("distributorDF")
      logger.info("distributorDF View Created")

      val distributorDfFull = spark.sql(
        s"""
           |select
           |BI_dstr_prty_id,
           |BI_Dstr_BR_typ_nm,
           |BI_dstr_Aruba_STCK_flg_cd,
           |bi_dstr_affiliate_prty_id,
           |bi_dstr_affiliate_prty_nm,
           |bi_dstr_prty_eng_name,
           |BI_dstr_ctry_Lvl_grp_id,
           |BI_dstr_ctr_cd,
           |bi_dstr_ctry_lvl_grp_nm,
           |bi_dstr_ctry_lvl_lcl_languague_grp_nm,
           |bi_dstr_gbl_lvl_grp_id,
           |bi_dstr_gbl_lvl_grp_nm,
           |BI_dstr_gbl_Lvl_lcl_Languague_grp_nm,
           |BI_dstr_pfl_Lvl_grp_id,
           |BI_dstr_pfl_Lvl_grp_nm,
           |BI_dstr_EMDMH_ctry_nm,
           |BI_dstr_EMDMH_ctry_cd,
           |BI_dstr_cmpt_ind,
           |BI_dstr_cust_ind,
           |BI_dstr_hq_ind,
           |BI_dstr_indy_vtcl_nm,
           |BI_dstr_indy_vtcl_sgm_nm,
           |BI_dstr_lgl_ctr_nm,
           |BI_dstr_ptnr_ind,
           |BI_dstr_prty_ctry_nm,
           |BI_dstr_prty_lcl_nm,
           |BI_dstr_prty_pstl_cd,
           |BI_dstr_prty_stts_nm,
           |BI_dstr_prty_typ_cd,
           |BI_dstr_prty_typ_nm,
           |BI_dstr_vndr_ind,
           |BI_dstr_cty,
           |BI_dstr_addr_1,
           |BI_dstr_addr_2,
           |BI_dstr_st_prvc,
           |BI_dstr_st_prvc_nm,
           |BI_dstr_dnb_out_bsn_ind,
           |bi_dstr_ctry_nm,
           |current_timestamp() as ins_ts
           |from
           |(select
           |DBF.BI_dstr_prty_id,
           |DBF.BI_Dstr_BR_typ_nm,
           |DBF.BI_dstr_Aruba_STCK_flg_cd,
           |DBF.bi_dstr_affiliate_prty_id,
           |DBF.bi_dstr_affiliate_prty_nm,
           |NULL AS bi_dstr_prty_eng_name,
           |DBF.BI_dstr_ctry_Lvl_grp_id as BI_dstr_ctry_Lvl_grp_id,
           |DBF.BI_dstr_ctr_cd,
           |dstrPrty.ctry_lvl_grp_nm AS bi_dstr_ctry_lvl_grp_nm,
           |dstrPrty.ctry_lvl_lcl_languague_grp_nm AS bi_dstr_ctry_lvl_lcl_languague_grp_nm,
           |dstrPrty.gbl_lvl_grp_id AS bi_dstr_gbl_lvl_grp_id,
           |dstrPrty.gbl_lvl_grp_nm AS bi_dstr_gbl_lvl_grp_nm,
           |dstrPrty.gbl_lvl_lcl_languague_grp_nm AS BI_dstr_gbl_Lvl_lcl_Languague_grp_nm ,
           |dstrPrty.cy_profile_lvl_grp_id_rslr_dstr AS BI_dstr_pfl_Lvl_grp_id ,
           |dstrPrty.cy_profile_lvl_grp_nm_rslr_dstr AS BI_dstr_pfl_Lvl_grp_nm ,
           |dstrPrty.emdmh_ctry_nm AS BI_dstr_EMDMH_ctry_nm ,
           |dstrPrty.emdmh_ctry_cd AS BI_dstr_EMDMH_ctry_cd,
           |dstrPrty.cmpt_ind_cd AS BI_dstr_cmpt_ind,
           |dstrPrty.cust_ind_cd AS BI_dstr_cust_ind,
           |dstrPrty.hq_ind_cd AS BI_dstr_hq_ind ,
           |dstrPrty.indy_vtcl_nm AS BI_dstr_indy_vtcl_nm ,
           |dstrPrty.indy_vtcl_sgm_nm AS BI_dstr_indy_vtcl_sgm_nm,
           |dstrPrty.lgl_ctry_nm AS BI_dstr_lgl_ctr_nm ,
           |dstrPrty.ptnr_ind_cd AS BI_dstr_ptnr_ind ,
           |dstrPrty.prty_ctry_nm AS BI_dstr_prty_ctry_nm ,
           |dstrPrty.prty_lcl_nm AS BI_dstr_prty_lcl_nm ,
           |dstrPrty.prty_pstl_cd AS BI_dstr_prty_pstl_cd ,
           |dstrPrty.prty_stts_nm AS BI_dstr_prty_stts_nm ,
           |dstrPrty.prty_typ_cd AS BI_dstr_prty_typ_cd ,
           |dstrPrty.prty_typ_nm AS BI_dstr_prty_typ_nm ,
           |dstrPrty.vndr_ind_cd AS BI_dstr_vndr_ind ,
           |dstrPrty.cty_cd AS BI_dstr_cty,
           |dstrPrty.addr_1_cd AS BI_dstr_addr_1,
           |dstrPrty.addr_2_cd AS BI_dstr_addr_2,
           |dstrPrty.st_prvc_cd AS BI_dstr_st_prvc ,
           |dstrPrty.st_prvc_nm AS BI_dstr_st_prvc_nm,
           |dstrPrty.out_of_business_ind AS BI_dstr_dnb_out_bsn_ind,
           |dstrPrty.prty_ctry_nm as bi_dstr_ctry_nm,
           |row_number() over (partition by BI_dstr_prty_id,DBF.BI_Dstr_BR_typ_nm order by BI_dstr_prty_id desc,DBF.BI_Dstr_BR_typ_nm desc) rk
           |from
           |distributorDF DBF
           |LEFT JOIN chnlptnr_common_prty_dmnsn dstrPrty ON DBF.BI_dstr_prty_id = dstrPrty.mdm_id where DBF.BI_dstr_prty_id is not null)A where A.rk=1
           |""".stripMargin)

      // val distributorDF_load = distributorDfFull.repartition(5).write.mode("overwrite").format("orc").insertInto("ea_common_an"+ "." + "chnlptnr_ps_dstr_dmnsn")
      logger.info("distributorDF Loaded")

      //****bmt_bi_distri_reseller_nonreporter_dmnsn****
      val bmt_bi_distri_reseller_nonreporter_dmnsn = spark.sql(
        s"""
           |select bmt_bi_distri_reseller_nonreporter_ky,
           |eff_upd_strt_dt,
           |eff_upd_end_dt,
           |reporter_id,
           |reporter_party_id,
           |reporter_br_type,
           |reporting_type,
           |sold_to_rawaddr_id,
           |sold_to_rawaddr_key,
           |sold_to_reported_id,
           |partner_invoice_numbersales_ord_nr,
           |ope_mf_trsn_ide2open_trsn_id,
           |rec_src,
           |bi_dstr_party_id,
           |bi_dstr_ctry_lvl_grp_id,
           |bi_dstr_br_typ,
           |bi_dstr_ctry_cd,
           |bi_dstr_lgcy_hq_pro_id,
           |bi_dstr_lgcy_pro_id,
           |bi_dstr_ww_mbrshp,
           |bi_dstr_prnt_prty_id,
           |bi_dstr_aruba_stck_flg,
           |bi_dstr_affiliate_prty_id,
           |bi_dstr_affiliate_prty_nm,
           |bi_rslr_party_id,
           |bi_rslr_ctry_lvl_grp_id,
           |bi_rslr_br_typ,
           |bi_rslr_ctry_cd,
           |bi_rslr_lgcy_pro_id,
           |bi_rslr_lgcy_hq_pro_id,
           |bi_rslr_ww_mbrshp,
           |bi_rslr_prnt_prty_id,
           |bi_rslr_aruba_stck_flg,
           |bi_rslr_ctry_lvl_mbrshp_aruba,
           |bi_rslr_ctry_lvl_mbrshp_hybrid_it,
           |bi_rslr_ctry_lvl_mbrshp_pointnext,
           |bi_rslr_affiliate_prty_id,
           |bi_rslr_affiliate_prty_nm,
           |bi_rslr_lgcy_mbrshp_edge,
           |bi_rslr_lgcy_mbrshp_hybrid_it,
           |bi_rslr_lgcy_ww_mbrshp,
           |reserved_fld_1,
           |reserved_fld_2,
           |reserved_fld_3,
           |reserved_fld_4,
           |reserved_fld_5,
           |reserved_fld_6,
           |reserved_fld_7,
           |reserved_fld_8,
           |reserved_fld_9,
           |reserved_fld_10,
           |src_sys_upd_ts,
           |src_sys_ky,
           |lgcl_dlt_ind,
           |ins_gmt_ts,
           |upd_gmt_ts,
           |src_sys_extrc_gmt_ts,
           |src_sys_btch_nr,
           |fl_nm,
           |ld_jb_nr,
           |ins_ts from ea_common.bmt_bi_distri_reseller_nonreporter_dmnsn
           |where (UPPER(rec_src)='POS' or UPPER(rec_src)='ALL' or rec_src is null or rec_src='' or UPPER(rec_src)='E2OPEN')
           |""".stripMargin)
      bmt_bi_distri_reseller_nonreporter_dmnsn.createOrReplaceTempView("bmt_bi_distri_reseller_nonreporter_dmnsn")

      val bmtdisresenonrepoDF = spark.sql(
        s"""
           |select dup.* from (SELECT  /*+ BROADCAST(trsn,Ptnr_INV,rslr_addr,rslr_prty_addr) */
           |ps.trsn_i_1_id,
           |COALESCE(trsn.bi_rslr_party_id,Ptnr_INV.bi_rslr_party_id,rslr_addr.bi_rslr_party_id,rslr_prty_addr.bi_rslr_party_id) AS bi_rslr_party_id,
           |COALESCE(trsn.bi_rslr_ctry_lvl_grp_id,Ptnr_INV.bi_rslr_ctry_lvl_grp_id,rslr_addr.bi_rslr_ctry_lvl_grp_id,rslr_prty_addr.bi_rslr_ctry_lvl_grp_id) AS bi_rslr_ctry_lvl_grp_id,
           |COALESCE(trsn.bi_rslr_br_typ,Ptnr_INV.bi_rslr_br_typ,rslr_addr.bi_rslr_br_typ,rslr_prty_addr.bi_rslr_br_typ) AS bi_rslr_br_typ,
           |COALESCE(trsn.bi_rslr_ctry_cd,Ptnr_INV.bi_rslr_ctry_cd,rslr_addr.bi_rslr_ctry_cd,rslr_prty_addr.bi_rslr_ctry_cd) AS bi_rslr_ctry_cd,
           |COALESCE(trsn.bi_rslr_lgcy_pro_id,Ptnr_INV.bi_rslr_lgcy_pro_id,rslr_addr.bi_rslr_lgcy_pro_id,rslr_prty_addr.bi_rslr_lgcy_pro_id) AS bi_rslr_lgcy_pro_id,
           |COALESCE(trsn.bi_rslr_lgcy_hq_pro_id,Ptnr_INV.bi_rslr_lgcy_hq_pro_id,rslr_addr.bi_rslr_lgcy_hq_pro_id,rslr_prty_addr.bi_rslr_lgcy_hq_pro_id) AS bi_rslr_lgcy_hq_pro_id,
           |COALESCE(trsn.bi_rslr_aruba_stck_flg,Ptnr_INV.bi_rslr_aruba_stck_flg,rslr_addr.bi_rslr_aruba_stck_flg,rslr_prty_addr.bi_rslr_aruba_stck_flg) as bi_rslr_aruba_stck_flg,
           |COALESCE(trsn.bi_rslr_ww_mbrshp,Ptnr_INV.bi_rslr_ww_mbrshp,rslr_addr.bi_rslr_ww_mbrshp,rslr_prty_addr.bi_rslr_ww_mbrshp) as bi_rslr_ww_mbrshp,
           |COALESCE(trsn.bi_dstr_affiliate_prty_id,Ptnr_INV.bi_dstr_affiliate_prty_id,rslr_addr.bi_dstr_affiliate_prty_id,rslr_prty_addr.bi_dstr_affiliate_prty_id) as bi_dstr_affiliate_prty_id,
           |COALESCE(trsn.bi_dstr_affiliate_prty_nm,Ptnr_INV.bi_dstr_affiliate_prty_nm,rslr_addr.bi_dstr_affiliate_prty_nm,rslr_prty_addr.bi_dstr_affiliate_prty_nm) as bi_dstr_affiliate_prty_nm,
           |COALESCE(trsn.bi_rslr_affiliate_prty_id,Ptnr_INV.bi_rslr_affiliate_prty_id,rslr_addr.bi_rslr_affiliate_prty_id,rslr_prty_addr.bi_rslr_affiliate_prty_id) as bi_rslr_affiliate_prty_id,
           |COALESCE(trsn.bi_rslr_affiliate_prty_nm,Ptnr_INV.bi_rslr_affiliate_prty_nm,rslr_addr.bi_rslr_affiliate_prty_nm,rslr_prty_addr.bi_rslr_affiliate_prty_nm) as bi_rslr_affiliate_prty_nm,
           |trsn.ope_mf_trsn_ide2open_trsn_id,
           |COALESCE(trsn.bi_rslr_ctry_lvl_mbrshp_aruba,Ptnr_INV.bi_rslr_ctry_lvl_mbrshp_aruba,rslr_addr.bi_rslr_ctry_lvl_mbrshp_aruba,rslr_prty_addr.bi_rslr_ctry_lvl_mbrshp_aruba) as bi_rslr_ctry_lvl_mbrshp_aruba,
           |COALESCE(trsn.bi_rslr_ctry_lvl_mbrshp_hybrid_it,Ptnr_INV.bi_rslr_ctry_lvl_mbrshp_hybrid_it,rslr_addr.bi_rslr_ctry_lvl_mbrshp_hybrid_it,rslr_prty_addr.bi_rslr_ctry_lvl_mbrshp_hybrid_it) as bi_rslr_ctry_lvl_mbrshp_hybrid_it,
           |COALESCE(trsn.bi_rslr_ctry_lvl_mbrshp_pointnext,Ptnr_INV.bi_rslr_ctry_lvl_mbrshp_pointnext,rslr_addr.bi_rslr_ctry_lvl_mbrshp_pointnext,rslr_prty_addr.bi_rslr_ctry_lvl_mbrshp_pointnext) as bi_rslr_ctry_lvl_mbrshp_pointnext,
           |COALESCE(trsn.bi_rslr_lgcy_mbrshp_edge,Ptnr_INV.bi_rslr_lgcy_mbrshp_edge,rslr_addr.bi_rslr_lgcy_mbrshp_edge,rslr_prty_addr.bi_rslr_lgcy_mbrshp_edge) as bi_rslr_lgcy_mbrshp_edge,
           |COALESCE(trsn.bi_rslr_lgcy_mbrshp_hybrid_it,Ptnr_INV.bi_rslr_lgcy_mbrshp_hybrid_it,rslr_addr.bi_rslr_lgcy_mbrshp_hybrid_it,rslr_prty_addr.bi_rslr_lgcy_mbrshp_hybrid_it) as bi_rslr_lgcy_mbrshp_hybrid_it,
           |COALESCE(trsn.bi_rslr_lgcy_ww_mbrshp,Ptnr_INV.bi_rslr_lgcy_ww_mbrshp,rslr_addr.bi_rslr_lgcy_ww_mbrshp,rslr_prty_addr.bi_rslr_lgcy_ww_mbrshp) as bi_rslr_lgcy_ww_mbrshp,
           |row_number() over(partition by ps.trsn_i_1_id order by ps.trsn_i_1_id desc)rnk
           |FROM PsDmnsnSubSet ps
           |LEFT JOIN rptg_ptnr_mstr_dmnsn_tmp rpm ON ps.reporter_i_2_id = rpm.reporter_i_1_id
           |LEFT JOIN bmt_bi_distri_reseller_nonreporter_dmnsn trsn
           |ON ps.trsn_i_1_id = trsn.ope_mf_trsn_ide2open_trsn_id
           |LEFT JOIN distributorTrsn tr ON ps.trsn_i_1_id=tr.trsn_i_1_id
           |LEFT JOIN bmt_bi_distri_reseller_nonreporter_dmnsn Ptnr_INV
           |on
           |ps.reporter_i_2_id=Ptnr_INV.reporter_id
           |AND ps.ptnr_inv_nr =Ptnr_INV.partner_invoice_numbersales_ord_nr
           |lEFT JOIN bmt_bi_distri_reseller_nonreporter_dmnsn rslr_addr
           |on
           |ps.reporter_i_2_id=rslr_addr.reporter_id
           |AND ps.sld_to_rptd_id = rslr_addr.sold_to_reported_id
           |and (ps.trs_1_dt >= COALESCE(to_date(from_unixtime(unix_timestamp(rslr_addr.eff_upd_strt_dt,'YYYY-MM-DD'))),'1900-01-01')
           |AND ps.trs_1_dt <= COALESCE(to_date(from_unixtime(unix_timestamp(rslr_addr.eff_upd_end_dt,'YYYY-MM-DD'))),'9999-12-31') )
           |lEFT JOIN bmt_bi_distri_reseller_nonreporter_dmnsn rslr_prty_addr
           |on
           |ps.reporter_i_2_id=rslr_prty_addr.reporter_id
           |AND ps.sld_to_rw_addr_id = rslr_prty_addr.sold_to_rawaddr_id
           |AND coalesce(ps.sld_to_ky_cd,'unknown') = coalesce(rslr_prty_addr.sold_to_rawaddr_key,'unknown')
           |and (ps.trs_1_dt >= COALESCE(to_date(from_unixtime(unix_timestamp(rslr_prty_addr.eff_upd_strt_dt,'YYYY-MM-DD'))),'1900-01-01')
           |AND ps.trs_1_dt <= COALESCE(to_date(from_unixtime(unix_timestamp(rslr_prty_addr.eff_upd_end_dt,'YYYY-MM-DD'))),'9999-12-31') )
           |)dup where rnk=1 and ((bi_rslr_party_id is not null or  bi_rslr_ctry_lvl_grp_id is not null or  bi_rslr_br_typ is not null or
           |bi_rslr_ctry_cd is not null or  bi_rslr_lgcy_pro_id is not null or  bi_rslr_lgcy_hq_pro_id is not null or
           |bi_rslr_aruba_stck_flg is not null or  bi_rslr_ww_mbrshp is not null or  bi_dstr_affiliate_prty_id is not null or
           |bi_dstr_affiliate_prty_nm is not null or  bi_rslr_affiliate_prty_id is not null or  bi_rslr_affiliate_prty_nm is not null or
           |ope_mf_trsn_ide2open_trsn_id is not null or  bi_rslr_ctry_lvl_mbrshp_aruba is not null or  bi_rslr_ctry_lvl_mbrshp_hybrid_it is not null
           |or  bi_rslr_ctry_lvl_mbrshp_pointnext is not null or  bi_rslr_lgcy_mbrshp_edge is not null or  bi_rslr_lgcy_mbrshp_hybrid_it is not null
           |or  bi_rslr_lgcy_ww_mbrshp is not null) )
           |""".stripMargin).drop("rnk")
      // val bmtdisresenonrepoDF_load = bmtdisresenonrepoDF.coalesce(20).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "bmtdisresenonrepoDF")

      // bmtdisresenonrepoDF.createOrReplaceTempView("bmtdisresenonrepoDF")
      logger.info("Distributor/Reseller non reporter bmt DF created")

      //Creating DF and loading reseller related information****
      val resellerTrsn = spark.sql(
        s"""
           |SELECT trsn_i_1_id,ptnr_inv_nr,BI_rslr_prty_id,BI_rslr_rsn_cd,BI_rslr_BR_typ_nm,bi_rslr_lgcy_pro_id,bi_rslr_lgcy_hq_pro_id,
           |COALESCE(BI_rslr_ctr_cd,PRTY.prty_ctry_cd) as BI_rslr_ctr_cd,ope_rslr_prty_id,ope_sldt_prty_id   FROM (select distinct
           |pS.trsn_i_1_id,ps.ptnr_inv_nr,
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_party_id IS NOT NULL THEN Rslr_rpt_Bmt.bi_rslr_party_id
           |WHEN Rslr_non_rpt_Bmt.bi_rslr_party_id IS NOT NULL THEN Rslr_non_rpt_Bmt.bi_rslr_party_id
           |WHEN OPE.ope_rslr_prty_id IS NOT NULL AND UPPER(OPE.ope_rslr_prty_id) <> 'NO RESELLER' THEN OPE.ope_rslr_prty_id
           |WHEN OPE.ope_rslr_prty_id IS NOT NULL AND UPPER(OPE.ope_rslr_prty_id) = 'NO RESELLER' AND
           |OPE.ope_sldt_prty_id IS NOT NULL AND UPPER(OPE.ope_sldt_prty_id) = 'UNMATCHED'
           |THEN CONCAT('UNMATCHED','_',OPE.ope_sldt_ctry ) ELSE OPE.ope_rslr_prty_id END AS BI_rslr_prty_id,
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_party_id IS NOT NULL THEN 'BI RESELLER REPORTER BMT'
           |WHEN Rslr_non_rpt_Bmt.bi_rslr_party_id IS NOT NULL THEN 'BI RESELLER NON-REPORTER BMT'
           |WHEN (ope.ope_rslr_prty_id IS NOT NULL AND UPPER(OPE.ope_rslr_prty_id) <> 'NO RESELLER') OR (ope.ope_sldt_prty_id IS NOT NULL
           |AND UPPER(OPE.ope_sldt_prty_id) <> 'NO RESELLER' AND OPE.ope_sldt_prty_id <> '') THEN OPE.ope_rslr_rsn_cd
           |WHEN ope.ope_sldt_prty_id IS NULL OR ope.ope_sldt_prty_id = '' OR ope.ope_sldt_prty_id='UNMATCHED' THEN 'UNMATCHED' ELSE 'NO RESELLER'
           |END AS BI_rslr_rsn_cd,
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_br_typ IS NOT NULL THEN Rslr_rpt_Bmt.bi_rslr_br_typ
           |WHEN Rslr_non_rpt_Bmt.bi_rslr_br_typ IS NOT NULL THEN Rslr_non_rpt_Bmt.bi_rslr_br_typ
           |WHEN OPE.ope_rslr_br_typ IS NOT NULL AND UPPER(OPE.ope_rslr_br_typ) <> 'NO RESELLER' THEN OPE.ope_rslr_br_typ
           |WHEN OPE.ope_rslr_br_typ IS NOT NULL AND UPPER(OPE.ope_rslr_br_typ) = 'NO RESELLER' AND
           |OPE.ope_sldt_br_typ IS NOT NULL AND UPPER(OPE.ope_sldt_br_typ) = 'UNMATCHED' and UPPER(OPE.ope_sldt_prty_id)= 'UNMATCHED'
           |then CONCAT('UNMATCHED','_',OPE.ope_sldt_ctry )
           |ELSE OPE.ope_rslr_br_typ END AS BI_rslr_BR_typ_nm,
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_lgcy_pro_id IS NOT NULL THEN Rslr_rpt_Bmt.bi_rslr_lgcy_pro_id
           |WHEN Rslr_non_rpt_Bmt.bi_rslr_lgcy_pro_id IS NOT NULL THEN Rslr_non_rpt_Bmt.bi_rslr_lgcy_pro_id
           |WHEN upper(trim(OPE.ope_rslr_rsn_cd))='REPORTER' THEN rpm.ptnr_pro_i_1_id
           |WHEN upper(trim(OPE.ope_rslr_rsn_cd))='SOLD TO' THEN OPE.sldt_sbl_prm_id_ope END AS bi_rslr_lgcy_pro_id,
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_lgcy_hq_pro_id IS NOT NULL THEN Rslr_rpt_Bmt.bi_rslr_lgcy_hq_pro_id
           |WHEN Rslr_non_rpt_Bmt.bi_rslr_lgcy_hq_pro_id IS NOT NULL THEN Rslr_non_rpt_Bmt.bi_rslr_lgcy_hq_pro_id
           |WHEN upper(trim(OPE.ope_rslr_rsn_cd))='REPORTER'  THEN rslrSblHrchy.BI_lgcy_hqid
           |WHEN upper(trim(OPE.ope_rslr_rsn_cd))='SOLD TO' THEN rslrSblHrchy_op.BI_lgcy_hqid END AS bi_rslr_lgcy_hq_pro_id,
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_ctry_cd IS NOT NULL THEN Rslr_rpt_Bmt.bi_rslr_ctry_cd
           |WHEN Rslr_non_rpt_Bmt.bi_rslr_ctry_cd IS NOT NULL THEN Rslr_non_rpt_Bmt.bi_rslr_ctry_cd ELSE NULL END AS BI_rslr_ctr_cd,
           |ope.ope_rslr_prty_id,ope.ope_sldt_prty_id
           |FROM PsDmnsnTRSNSet ps
           |LEFT JOIN OpePsDmnsnSuperSet ope ON ps.trsn_i_1_id=OPE.trsn_i_1_id
           |LEFT JOIN rptg_ptnr_mstr_dmnsn_tmp rpm ON ps.reporter_i_2_id = rpm.reporter_i_1_id
           |LEFT JOIN ea_support_temp_an.bmtdisresenonrepoDF Rslr_non_rpt_Bmt ON ps.trsn_i_1_id=Rslr_non_rpt_Bmt.trsn_i_1_id
           |LEFT JOIN ea_support_temp_an.bmtdisreserepoDF Rslr_rpt_Bmt ON ps.trsn_i_1_id = Rslr_rpt_Bmt.trsn_i_1_id
           |LEFT JOIN ea_common.chnlptnr_legacy rslrSblHrchy
           |ON RPM.ptnr_pro_i_1_id  = rslrSblHrchy.ptnr_pfl_row_id
           |LEFT JOIN ea_common.chnlptnr_legacy rslrSblHrchy_op
           |ON ope.sldt_sbl_prm_id_ope  = rslrSblHrchy_op.ptnr_pfl_row_id)A
           |LEFT JOIN chnlptnr_common_prty_dmnsn prty ON prty.mdm_id=A.BI_rslr_prty_id WHERE
           | A.BI_rslr_prty_id IS NOT NULL OR A.bi_rslr_lgcy_pro_id IS NOT NULL
           |or A.bi_rslr_lgcy_hq_pro_id is not null
           |or A.BI_rslr_BR_typ_nm is not null
           |or A.BI_rslr_ctr_cd is not null
           |""".stripMargin).repartition($"trsn_i_1_id")

      resellerTrsn.createOrReplaceTempView("resellerTrsn")
      logger.info("resellerTrsn view created")

      val resellerDF = spark.sql(
        s"""
           |SELECT BI_rslr_ctry_Lvl_grp_id,
           |BI_rslr_ctr_cd,
           |BI_rslr_prty_id,
           |BI_rslr_BR_typ_nm,
           |BI_rslr_Aruba_STCK_flg_cd,
           |bi_rslr_ww_mbrshp,
           |bi_rslr_affiliate_prty_id,
           |bi_rslr_affiliate_prty_nm,
           |bi_rslr_ctry_lvl_mbrshp_aruba,
           | bi_rslr_ctry_lvl_mbrshp_hybrid_it ,
           | bi_rslr_ctry_lvl_mbrshp_pointnext,
           | bi_rslr_lgcy_mbrshp_edge,
           | bi_rslr_lgcy_mbrshp_hybrid_it,
           | bi_rslr_lgcy_ww_mbrshp
           |FROM
           |(select
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_ctry_lvl_grp_id IS NOT NULL THEN Rslr_rpt_Bmt.bi_rslr_ctry_lvl_grp_id
           |WHEN Rslr_non_rpt_Bmt.bi_rslr_ctry_lvl_grp_id IS NOT NULL THEN  Rslr_non_rpt_Bmt.bi_rslr_ctry_lvl_grp_id
           |WHEN cast(prty.ctry_lvl_grp_id as string) IS NOT NULL THEN  cast(prty.ctry_lvl_grp_id as String) END AS BI_rslr_ctry_Lvl_grp_id,
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_ctry_cd IS NOT NULL THEN Rslr_rpt_Bmt.bi_rslr_ctry_cd
           |WHEN Rslr_non_rpt_Bmt.bi_rslr_ctry_cd IS NOT NULL THEN Rslr_non_rpt_Bmt.bi_rslr_ctry_cd
           |WHEN PRTY.prty_ctry_cd IS NOT NULL THEN PRTY.prty_ctry_cd END AS BI_rslr_ctr_cd,
           |ps.BI_rslr_prty_id AS BI_rslr_prty_id,
           |ps.BI_rslr_BR_typ_nm,
           |CASE WHEN Rslr_rpt_Bmt.bi_rslr_party_id IS NOT NULL OR Rslr_non_rpt_Bmt.bi_rslr_party_id IS NOT NULL
           |OR(ps.ope_rslr_prty_id IS NOT NULL AND UPPER(ps.ope_rslr_prty_id) <> 'NO RESELLER') OR (ps.ope_sldt_prty_id IS NOT NULL AND UPPER(ps.ope_sldt_prty_id) <> 'NO RESELLER' AND ps.ope_sldt_prty_id <> '') THEN Rslr_rpt_Bmt.bi_rslr_aruba_stck_flg END AS BI_rslr_Aruba_STCK_flg_cd,
           |CASE WHEN  Rslr_rpt_Bmt.bi_rslr_ww_mbrshp IS NOT NULL THEN  Rslr_rpt_Bmt.bi_rslr_ww_mbrshp ELSE Rslr_non_rpt_Bmt.bi_rslr_ww_mbrshp END AS bi_rslr_ww_mbrshp,
           |CASE WHEN  Rslr_rpt_Bmt.bi_rslr_affiliate_prty_id IS NOT NULL THEN  Rslr_rpt_Bmt.bi_rslr_affiliate_prty_id ELSE Rslr_non_rpt_Bmt.bi_rslr_affiliate_prty_id END AS bi_rslr_affiliate_prty_id,
           |CASE WHEN  Rslr_rpt_Bmt.bi_rslr_affiliate_prty_nm IS NOT NULL THEN  Rslr_rpt_Bmt.bi_rslr_affiliate_prty_nm ELSE Rslr_non_rpt_Bmt.bi_rslr_affiliate_prty_nm END AS bi_rslr_affiliate_prty_nm,
           |Rslr_rpt_Bmt.bi_rslr_party_id as RRB_bi_rslr_party_id,
           |Rslr_non_rpt_Bmt.bi_rslr_party_id as RNRB_bi_rslr_party_id,
           |coalesce(Rslr_rpt_Bmt.bi_rslr_ctry_lvl_mbrshp_aruba,Rslr_non_rpt_Bmt.bi_rslr_ctry_lvl_mbrshp_aruba) as bi_rslr_ctry_lvl_mbrshp_aruba,
           |coalesce(Rslr_rpt_Bmt.bi_rslr_ctry_lvl_mbrshp_hybrid_it,Rslr_non_rpt_Bmt.bi_rslr_ctry_lvl_mbrshp_hybrid_it) as bi_rslr_ctry_lvl_mbrshp_hybrid_it,
           |coalesce(Rslr_rpt_Bmt.bi_rslr_ctry_lvl_mbrshp_pointnext,Rslr_non_rpt_Bmt.bi_rslr_ctry_lvl_mbrshp_pointnext) as bi_rslr_ctry_lvl_mbrshp_pointnext,
           |coalesce(Rslr_rpt_Bmt.bi_rslr_lgcy_mbrshp_edge,Rslr_non_rpt_Bmt.bi_rslr_lgcy_mbrshp_edge) as bi_rslr_lgcy_mbrshp_edge,
           |coalesce(Rslr_rpt_Bmt.bi_rslr_lgcy_mbrshp_hybrid_it,Rslr_non_rpt_Bmt.bi_rslr_lgcy_mbrshp_hybrid_it) as bi_rslr_lgcy_mbrshp_hybrid_it,
           |coalesce(Rslr_rpt_Bmt.bi_rslr_lgcy_ww_mbrshp,Rslr_non_rpt_Bmt.bi_rslr_lgcy_ww_mbrshp) as bi_rslr_lgcy_ww_mbrshp,
           |row_number() over(partition by BI_rslr_prty_id,BI_rslr_BR_typ_nm order by Rslr_rpt_Bmt.bi_rslr_party_id desc,BI_rslr_BR_typ_nm desc) as rk
           |FROM resellerTrsn ps
           |LEFT JOIN chnlptnr_common_prty_dmnsn prty ON prty.mdm_id=ps.BI_rslr_prty_id
           |LEFT JOIN ea_support_temp_an.bmtdisresenonrepoDF Rslr_non_rpt_Bmt ON ps.trsn_i_1_id = Rslr_non_rpt_Bmt.trsn_i_1_id
           |LEFT JOIN ea_support_temp_an.bmtdisreserepoDF Rslr_rpt_Bmt ON ps.trsn_i_1_id = Rslr_rpt_Bmt.trsn_i_1_id)A where A.rk=1
           |""".stripMargin)
      //resellerDF.createOrReplaceTempView("resellerDF")
      // val resellerD_load = resellerDF.write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "resellerDF_tmp")
      logger.info("resellerDF Table Loaded")

      val resellerDfFull = spark.sql(
        s"""
           |SELECT
           |BI_rslr_prty_id,
           |BI_rslr_BR_typ_nm,
           |BI_rslr_Aruba_STCK_flg_cd,
           |bi_rslr_affiliate_prty_id,
           |bi_rslr_affiliate_prty_nm,
           |bi_rslr_prty_eng_name,
           |BI_rslr_CY_ctry_Lvl_grp_id,
           |BI_rslr_ctr_cd,
           |BI_rslr_CY_ctry_Lvl_grp_nm,
           |BI_rslr_CY_ctry_Lvl_lcl_Languague_grp_nm,
           |BI_rslr_CY_gbl_Lvl_grp_id,
           |BI_rslr_CY_gbl_Lvl_grp_nm,
           |BI_rslr_CY_gbl_Lvl_lcl_Languague_grp_nm,
           |BI_rslr_pfl_Lvl_grp_id,
           |BI_rslr_pfl_Lvl_grp_nm,
           |BI_rslr_EMDMH_ctry_cd,
           |BI_rslr_EMDMH_ctry_nm,
           |BI_rslr_cmpt_ind,
           |BI_rslr_cust_ind,
           |BI_rslr_hq_ind,
           |BI_rslr_indy_vtcl_nm,
           |BI_rslr_indy_vtcl_sgm_nm,
           |BI_rslr_ptnr_ind,
           |BI_rslr_prty_ctry_nm,
           |BI_rslr_prty_lcl_nm,
           |BI_rslr_pstl_cd,
           |BI_rslr_prty_stts_nm,
           |BI_rslr_prty_typ_cd,
           |BI_rslr_prty_typ_nm,
           |BI_rslr_vndr_ind,
           |BI_rslr_cty,
           |BI_rslr_addr_1,
           |BI_rslr_addr_2,
           |BI_rslr_st_prvc,
           |BI_rslr_st_prvc_nm,
           |rslr_out_of_business_ind,
           |bi_rslr_lgl_ctr_nm,
           |bi_rslr_ctry_lvl_mbrshp_aruba     ,
           |bi_rslr_ctry_lvl_mbrshp_hybrid_it ,
           |bi_rslr_ctry_lvl_mbrshp_pointnext ,
           |bi_rslr_lgcy_mbrshp_edge          ,
           |bi_rslr_lgcy_mbrshp_hybrid_it     ,
           |bi_rslr_lgcy_ww_mbrshp            ,
           |bi_reseller_cy_country_entity_aruba_rad_name,
           |bi_reseller_cy_country_entity_eg_rad_name,
           |bi_reseller_cy_global_entity_aruba_rad_name,
           |bi_reseller_cy_global_entity_eg_rad_name,
           |bi_rslr_ww_mbrshp,
           |ins_ts
           |FROM
           |(select
           |RDF.BI_rslr_prty_id,
           |RDF.BI_rslr_BR_typ_nm,
           |CASE WHEN RDF.BI_rslr_prty_id LIKE '%UNMATCHED%' THEN NULL ELSE RDF.BI_rslr_Aruba_STCK_flg_cd END AS BI_rslr_Aruba_STCK_flg_cd,
           |RDF.bi_rslr_affiliate_prty_id,
           |RDF.bi_rslr_affiliate_prty_nm,
           |NULL AS bi_rslr_prty_eng_name,
           |RDF.BI_rslr_ctry_Lvl_grp_id AS BI_rslr_CY_ctry_Lvl_grp_id,
           |case when UPPER(RDF.BI_rslr_prty_id)  like '%UNMATCHED%' THEN SUBSTR(RDF.BI_rslr_prty_id,11,LENGTH(RDF.BI_rslr_prty_id)) ELSE RDF.BI_rslr_ctr_cd end as BI_rslr_ctr_cd,
           |rslrPrty.ctry_lvl_grp_nm AS BI_rslr_CY_ctry_Lvl_grp_nm,
           |rslrPrty.ctry_lvl_lcl_languague_grp_nm AS BI_rslr_CY_ctry_Lvl_lcl_Languague_grp_nm,
           |rslrPrty.gbl_lvl_grp_id AS BI_rslr_CY_gbl_Lvl_grp_id,
           |rslrPrty.gbl_lvl_grp_nm AS BI_rslr_CY_gbl_Lvl_grp_nm,
           |rslrPrty.gbl_lvl_lcl_languague_grp_nm AS BI_rslr_CY_gbl_Lvl_lcl_Languague_grp_nm ,
           |rslrPrty.cy_profile_lvl_grp_id_rslr_dstr AS BI_rslr_pfl_Lvl_grp_id,
           |rslrPrty.cy_profile_lvl_grp_nm_rslr_dstr AS BI_rslr_pfl_Lvl_grp_nm,
           |rslrPrty.emdmh_ctry_cd AS BI_rslr_EMDMH_ctry_cd ,
           |rslrPrty.emdmh_ctry_nm AS BI_rslr_EMDMH_ctry_nm,
           |rslrPrty.cmpt_ind_cd AS BI_rslr_cmpt_ind,
           |rslrPrty.cust_ind_cd AS BI_rslr_cust_ind,
           |rslrPrty.hq_ind_cd AS BI_rslr_hq_ind,
           |rslrPrty.indy_vtcl_nm AS BI_rslr_indy_vtcl_nm,
           |rslrPrty.indy_vtcl_sgm_nm AS BI_rslr_indy_vtcl_sgm_nm,
           |rslrPrty.lgl_ctry_nm AS BI_rslr_lgl_ctry_cd,
           |rslrPrty.ptnr_ind_cd AS BI_rslr_ptnr_ind,
           |rslrPrty.prty_ctry_nm AS BI_rslr_prty_ctry_nm,
           |rslrPrty.prty_lcl_nm AS BI_rslr_prty_lcl_nm,
           |rslrPrty.prty_pstl_cd AS BI_rslr_pstl_cd,
           |rslrPrty.prty_stts_nm AS BI_rslr_prty_stts_nm,
           |rslrPrty.prty_typ_cd AS BI_rslr_prty_typ_cd,
           |rslrPrty.prty_typ_nm AS BI_rslr_prty_typ_nm,
           |rslrPrty.vndr_ind_cd AS BI_rslr_vndr_ind,
           |rslrPrty.cty_cd AS BI_rslr_cty ,
           |rslrPrty.addr_1_cd AS BI_rslr_addr_1,
           |rslrPrty.addr_2_cd AS BI_rslr_addr_2,
           |rslrPrty.st_prvc_cd AS BI_rslr_st_prvc,
           |rslrPrty.st_prvc_nm AS BI_rslr_st_prvc_nm,
           |rslrPrty.out_of_business_ind AS rslr_out_of_business_ind,
           |rslrPrty.lgl_ctry_nm as bi_rslr_lgl_ctr_nm,
           |RDF.bi_rslr_ctry_lvl_mbrshp_aruba     ,
           |RDF.bi_rslr_ctry_lvl_mbrshp_hybrid_it ,
           |RDF.bi_rslr_ctry_lvl_mbrshp_pointnext ,
           |RDF.bi_rslr_lgcy_mbrshp_edge          ,
           |RDF.bi_rslr_lgcy_mbrshp_hybrid_it     ,
           |RDF.bi_rslr_lgcy_ww_mbrshp            ,
           |rslrPrty.cy_country_entity_aruba_rad_name as bi_reseller_cy_country_entity_aruba_rad_name,
           |rslrPrty.cy_country_entity_eg_rad_name as bi_reseller_cy_country_entity_eg_rad_name,
           |rslrPrty.cy_global_entity_aruba_rad_name as bi_reseller_cy_global_entity_aruba_rad_name,
           |rslrPrty.cy_global_entity_eg_rad_name as bi_reseller_cy_global_entity_eg_rad_name,
           |RDF.bi_rslr_ww_mbrshp,
           |current_timestamp() as ins_ts,
           |ROW_NUMBER() OVER(partition by BI_rslr_prty_id,BI_rslr_BR_typ_nm order by BI_rslr_prty_id desc,BI_rslr_BR_typ_nm desc) rk
           |from ea_support_temp_an.resellerDF_tmp RDF
           |LEFT JOIN chnlptnr_common_prty_dmnsn rslrPrty ON RDF.BI_rslr_prty_id = rslrPrty.mdm_id
           |WHERE RDF.BI_rslr_prty_id IS NOT NULL)A where A.rk=1
           |""".stripMargin)

      // val resellerDF_load = resellerDfFull.write.mode("overwrite").format("orc").insertInto("ea_common_an"+ "." + "chnlptnr_ps_rseller_dmnsn")
      logger.info("chnlptnr_ps_rseller_dmnsn Loaded")

      val LgcyPro = spark.sql(
        s"""
           |SELECT bi_rslr_lgcy_pro_id AS lgcy_pro_id,NULL AS lgcy_pro_hqid,ptnr_inv_nr FROM resellerTrsn WHERE bi_rslr_lgcy_pro_id IS NOT NULL
           |UNION
           |SELECT dstr_lgcy_pro_id AS lgcy_pro_id,NULL AS lgcy_pro_hqid,ptnr_inv_nr FROM distributorTrsn WHERE dstr_lgcy_pro_id IS NOT NULL
           |UNION
           |select BI_dstr_lgcy_pro_id AS lgcy_pro_id,NULL AS lgcy_pro_hqid,Null as ptnr_inv_nr from ea_common.Dstr_Lgcy_pro where BI_dstr_lgcy_pro_id is not null
           |UNION
           |select BI_rslr_lgcy_pro_id AS lgcy_pro_id,NULL AS lgcy_pro_hqid,Null as ptnr_inv_nr from ea_common.Rslr_Lgcy_pro where BI_rslr_lgcy_pro_id is not null
           |UNION
           |select psGrpgSoldTo.ptnr_pro_i_2_id as sld_to_ptnr_pro_id , NULL AS lgcy_pro_hqid,ptnr_inv_nr FROM PsDmnsnSubSet ps LEFT JOIN ea_common.ps_grpg_dmnsn psGrpgSoldTo
           |ON ps.sld_to_rw_addr_id = psGrpgSoldTo.rawaddr_id AND coalesce(ps.sld_to_ky_cd,'unknown') = coalesce(psGrpgSoldTo.rawaddr_ky_cd,'unknown')
           |AND psGrpgSoldTo.addr_typ_cd ='Sold To' WHERE psGrpgSoldTo.ptnr_pro_i_2_id IS NOT NULL
           |""".stripMargin)

      LgcyPro.createOrReplaceTempView("LgcyPro")

      val lgcyProDf = spark.sql(
        s""" select lgcy_pro_id,lgcy_pro_hqid,ptnr_inv_nr from (select lgcy_pro_id,lgcy_pro_hqid,ptnr_inv_nr,
           |row_number() over (partition by lgcy_pro_id order by ptnr_inv_nr desc )rk from LgcyPro)A where rk=1""".stripMargin)
      // val LGCY_PRO_load = lgcyProDf.coalesce(20).write.mode("overwrite").format("orc").insertInto("ea_common_an"+ "." + "chnlptnr_LGCY_PRO")
      logger.info("chnlptnr_LGCY_PRO table loaded")

      val lgcyPro_ReporterDf = spark.sql(
        s"""
           |select  distinct bi_dstr_lgcy_pro_id,bi_dstr_lgcy_hq_pro_id,bi_rslr_lgcy_pro_id,bi_rslr_lgcy_hq_pro_id,bi_rslr_lgcy_mbrshp_edge,
           |bi_rslr_lgcy_mbrshp_hybrid_it,bi_rslr_lgcy_ww_mbrshp
           |from ea_common.bmt_bi_distri_reseller_reporter_dmnsn where bi_dstr_lgcy_pro_id is not null or bi_rslr_lgcy_pro_id is not null
           |""".stripMargin)
 
      lgcyPro_ReporterDf.createOrReplaceTempView("lgcyPro_ReporterDf")

      val lgcyPro_NonReporterDf = spark.sql(
        s"""
           |select  distinct bi_dstr_lgcy_pro_id,bi_dstr_lgcy_hq_pro_id,bi_rslr_lgcy_pro_id,bi_rslr_lgcy_hq_pro_id,bi_rslr_lgcy_mbrshp_edge,bi_rslr_lgcy_mbrshp_hybrid_it,
           |bi_rslr_lgcy_ww_mbrshp from ea_common.bmt_bi_distri_reseller_nonreporter_dmnsn where bi_dstr_lgcy_pro_id is not null or bi_rslr_lgcy_pro_id is not null
           |""".stripMargin)
      lgcyPro_NonReporterDf.createOrReplaceTempView("lgcyPro_NonReporterDf")

	 //****Creating Legacy Pro Dimension table****
   val lgcyProDmnsn = spark.sql(
     s"""
        |SELECT
        |pro.lgcy_pro_id,
        |CASE WHEN COALESCE(lgcy_reporter_dist.bi_dstr_lgcy_hq_pro_id,lgcy_reporter_reseller.bi_rslr_lgcy_hq_pro_id,
        |lgcy_Nonreporter_dist.bi_dstr_lgcy_hq_pro_id,lgcy_Nonreporter_reseller.bi_rslr_lgcy_hq_pro_id,PRO.lgcy_pro_hqid,
        |LE.BI_lgcy_hqid) is not NULL THEN COALESCE(lgcy_reporter_dist.bi_dstr_lgcy_hq_pro_id,lgcy_reporter_reseller.bi_rslr_lgcy_hq_pro_id,
        |lgcy_Nonreporter_dist.bi_dstr_lgcy_hq_pro_id,lgcy_Nonreporter_reseller.bi_rslr_lgcy_hq_pro_id,PRO.lgcy_pro_hqid,LE.BI_lgcy_hqid)
        |WHEN DSTR_HST_UP.bi_dstr_lgcy_hq_pro_id_id is not NULL THEN DSTR_HST_UP.bi_dstr_lgcy_hq_pro_id_id ELSE
        |coalesce(RSLR_HST_UP.bi_rslr_lgcy_hq_pro_id,pro.lgcy_pro_id) END AS lgcy_pro_hqid,
        |CASE WHEN LE.bi_lgcy_hq_ctrynm IS NOT NULL THEN LE.bi_lgcy_hq_ctrynm WHEN DSTR_HST_UP.bi_dstr_lgcy_hq_ctr_nm is NOT NULL THEN
        |DSTR_HST_UP.bi_dstr_lgcy_hq_ctr_nm  WHEN RSLR_HST_UP.bi_rslr_lgcy_hq_ctr_nm IS NULL THEN RSLR_LGCY_HST_UP.byr_ctry_nm ELSE
        |RSLR_HST_UP.bi_rslr_lgcy_hq_ctr_nm END AS bi_lgcy_hq_ctrynm,
        |CASE WHEN LE.bi_lgcy_hqnm IS NOT NULL THEN LE.bi_lgcy_hqnm WHEN DSTR_HST_UP.bi_dstr_lgcy_h_1_nm is NOT NULL THEN DSTR_HST_UP.bi_dstr_lgcy_h_1_nm ELSE RSLR_HST_UP.bi_rslr_lgcy_hq_nm END AS bi_lgcy_hqnm,
        |CASE WHEN LE.bi_lgcy_rgnl_hq_id IS NOT NULL THEN LE.bi_lgcy_rgnl_hq_id WHEN DSTR_HST_UP.bi_dstr_lgcy_rgnl_hq_i_1_id is NOT NULL THEN DSTR_HST_UP.bi_dstr_lgcy_rgnl_hq_i_1_id ELSE RSLR_HST_UP.bi_rslr_lgcy_rgnl_hq_id END AS bi_lgcy_rgnl_hq_id,
        |CASE WHEN LE.bi_lgcy_rgnl_hq_nm IS NOT NULL THEN LE.bi_lgcy_rgnl_hq_nm WHEN DSTR_HST_UP.bi_dstr_lgcy_rgnl_h_1_nm is NOT NULL THEN DSTR_HST_UP.bi_dstr_lgcy_rgnl_h_1_nm ELSE RSLR_HST_UP.bi_rslr_lgcy_rgnl_hq_nm END AS bi_lgcy_rgnl_hq_nm,
        |CASE WHEN
        |coalesce(lgcy_reporter_reseller.bi_rslr_lgcy_mbrshp_edge,lgcy_Nonreporter_reseller.bi_rslr_lgcy_mbrshp_edge) is not null then
        |coalesce(lgcy_reporter_reseller.bi_rslr_lgcy_mbrshp_edge,lgcy_Nonreporter_reseller.bi_rslr_lgcy_mbrshp_edge)
        |WHEN DSTR_HST_UP.bi_ditributor_lgcy_mbrshp_edge_cd is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_mbrshp_edge_cd ELSE DSTR_HST_UP.bi_ditributor_lgcy_mbrshp_edge_cd END AS bi_rslr_lgcy_mbrshp_edge,
        |CASE WHEN
        |coalesce(lgcy_reporter_reseller.bi_rslr_lgcy_mbrshp_hybrid_it,lgcy_Nonreporter_reseller.bi_rslr_lgcy_mbrshp_hybrid_it) is not null then
        |coalesce(lgcy_reporter_reseller.bi_rslr_lgcy_mbrshp_hybrid_it,lgcy_Nonreporter_reseller.bi_rslr_lgcy_mbrshp_hybrid_it)
        |WHEN DSTR_HST_UP.bi_ditributor_lgcy_mbrshp_hybrid_it_id is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_mbrshp_hybrid_it_id ELSE DSTR_HST_UP.bi_ditributor_lgcy_mbrshp_hybrid_it_id END AS bi_rslr_lgcy_mbrshp_hybrid_it,
        |CASE WHEN
        |coalesce(lgcy_reporter_reseller.bi_rslr_lgcy_ww_mbrshp,lgcy_Nonreporter_reseller.bi_rslr_lgcy_ww_mbrshp) is not null then
        |coalesce(lgcy_reporter_reseller.bi_rslr_lgcy_ww_mbrshp,lgcy_Nonreporter_reseller.bi_rslr_lgcy_ww_mbrshp)
        |WHEN DSTR_HST_UP.bi_ditributor_lgcy_ww_mbrshp_cd is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_ww_mbrshp_cd ELSE DSTR_HST_UP.bi_ditributor_lgcy_ww_mbrshp_cd END AS bi_rslr_lgcy_ww_mbrshp,
        |CASE WHEN DSTR_HST_UP.bi_dstr_lgcy_hq_ctr_cd is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_hq_ctr_cd ELSE DSTR_HST_UP.bi_dstr_lgcy_hq_ctr_cd END AS bi_lgcy_hq_ctr_cd,
        |CASE WHEN DSTR_HST_UP.bi_ditributor_lgcy_ww_mbrshp_grp_cd is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_ww_mbrshp_grp_cd ELSE DSTR_HST_UP.bi_ditributor_lgcy_ww_mbrshp_grp_cd END AS bi_lgcy_ww_mbrshp_grp_cd,
        |CASE WHEN DSTR_HST_UP.bi_ditributor_lgcy_nm is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_nm ELSE DSTR_HST_UP.bi_ditributor_lgcy_nm END AS bi_lgcy_nm,
        |RSLR_HST_UP.bi_rslr_lgcy_ts_cntrctl_stts_cd AS bi_lgcy_ts_cntrctl_stts_cd,
        |RSLR_HST_UP.bi_rslr_lgcy_ptnr_typ_cd AS bi_lgcy_ptnr_typ_cd,
        |CASE WHEN DSTR_HST_UP.bi_ditributor_lgcy_pro_id_hrchy_typ_id is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_hrchy_typ_cd ELSE DSTR_HST_UP.bi_ditributor_lgcy_pro_id_hrchy_typ_id END AS bi_lgcy_hrchy_typ_cd,
        |CASE WHEN DSTR_HST_UP.bi_ditributor_rgnl_lctn_id_id is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_rgnl_hq_lctn_id ELSE DSTR_HST_UP.bi_ditributor_rgnl_lctn_id_id END AS bi_lgcy_rgnl_hq_lctn_id,
        |CASE WHEN DSTR_HST_UP.bi_ditributor_hq_lctn_id_id is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_hq_lctn_id ELSE DSTR_HST_UP.bi_ditributor_hq_lctn_id_id END AS bi_lgcy_hq_lctn_id,
        |CASE WHEN DSTR_HST_UP.bi_ditributor_lgcy_chnl_sgm_cd is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_chnl_sgm_cd ELSE DSTR_HST_UP.bi_ditributor_lgcy_chnl_sgm_cd END AS bi_lgcy_chnl_sgm_cd,
        |CASE WHEN DSTR_HST_UP.bi_ditributor_lgcy_chnl_sb_sgm_cd is NULL THEN RSLR_HST_UP.bi_rslr_lgcy_subchannel_sgm_cd ELSE DSTR_HST_UP.bi_ditributor_lgcy_chnl_sb_sgm_cd END AS bi_lgcy_subchannel_sgm_cd,
        |DSTR_HST_UP.bi_dstr_lgcy_cntrct_typ_cd AS bi_lgcy_cntrct_typ_cd,
        |DSTR_HST_UP.ptnr_rdy_srv_dstr_flg_cd AS bi_lgcy_ptnr_rdy_srv_dstr_flg_cd,
        |DSTR_HST_UP.bi_ditributor_lgcy_hb_ppid_id AS bi_lgcy_hb_ppid_id,
        |DSTR_HST_UP.odp_flg_cd AS odp_flg_cd
        |from ea_common_an.chnlptnr_LGCY_PRO pro
        |LEFT JOIN lgcyPro_ReporterDf lgcy_reporter_dist on lgcy_reporter_dist.bi_dstr_lgcy_pro_id=pro.lgcy_pro_id
        |LEFT JOIN lgcyPro_ReporterDf lgcy_reporter_reseller on lgcy_reporter_reseller.bi_rslr_lgcy_pro_id=pro.lgcy_pro_id
        |LEFT JOIN lgcyPro_NonReporterDf lgcy_Nonreporter_dist on lgcy_Nonreporter_dist.bi_dstr_lgcy_pro_id=pro.lgcy_pro_id
        |LEFT JOIN lgcyPro_NonReporterDf lgcy_Nonreporter_reseller on lgcy_Nonreporter_reseller.bi_rslr_lgcy_pro_id=pro.lgcy_pro_id
        |LEFT JOIN ea_common.chnlptnr_legacy LE ON pro.lgcy_pro_id=LE.ptnr_pfl_row_id
        |LEFT JOIN ea_common.bi_dstr_hstl_upd_bmt_ref DSTR_HST_UP ON DSTR_HST_UP.bi_dstr_lgcy_pro_id_id=pro.lgcy_pro_id
        |LEFT JOIN ea_common.bi_rslr_hstl_upd_bmt_ref RSLR_HST_UP ON RSLR_HST_UP.bi_rslr_lgcy_pro_id=pro.lgcy_pro_id
        |LEFT JOIN ea_common.bi_rslr_lgcy_hq_ctry_bmt_ref RSLR_LGCY_HST_UP ON RSLR_LGCY_HST_UP.slr_site_rowid_id=pro.lgcy_pro_id
        |and RSLR_LGCY_HST_UP.inv_nr=pro.ptnr_inv_nr
        |""".stripMargin)

      // val LGCY_PRO_dmnsn_load = lgcyProDmnsn.coalesce(10).write.mode("overwrite").format("orc").insertInto("ea_common_an"+ "." + "chnlptnr_LGCY_PRO_dmnsn")
      logger.info("chnlptnr_LGCY_PRO_dmnsn table loaded")

      val PS_DMNSN_grp = spark.sql(
        s"""
           |select distinct
           |bll_to_ky_cd,
           |bll_to_rw_addr_id,
           |sl_frm_rw_addr_i_1_id,
           |sl_frm_ky_cd,
           |shp_to_rw_addr_id,
           |shp_to_ky_cd,
           |shp_frm_rw_addr_id,
           |shp_frm_ky_cd,
           |sld_to_rw_addr_id,
           |sld_to_ky_cd,
           |end_usr_rw_addr_id,
           |end_usr_ky_cd,
           |enttld_rw_addr_id,
           |enttld_ky_cd
           |from ps_dmnsnDF
           |""".stripMargin)
      PS_DMNSN_grp.createOrReplaceTempView("PS_DMNSN_grp")
      PS_DMNSN_grp.persist(StorageLevel.MEMORY_AND_DISK_SER)
      logger.info("PS_DMNSN_grp Loaded")

      val ps_grpg_dmnsn = spark.sql(
        s"""
           |select distinct
           |rawaddr_id,
           |rawaddr_ky_cd,
           |addr_typ_cd,
           |mdcp_org_id,
           |mdcp_loc_id,
           |mdcp_opsi_id,
           |mdcp_bri_id,
           |mdcp_br_ty_2_cd,
           |ptnr_pro_i_2_id,
           |ent_mdm_prty_id,
           |ent_mdm_br_ty_2_cd,
           |dun_2_cd,
           |rsrv_fld_1_nm,
           |rsrv_fld_2_nm,
           |rsrv_fld_3_nm,
           |rsrv_fld_4_nm,
           |rsrv_fld_5_nm,
           |substring(fl_nm,0,19) AS ps_grpg_E2Open_extrt_nm,
           |REGEXP_EXTRACT(fl_nm,'E2OPEN_HPE_GROUPING_(\\\\d+)',1) AS ps_grpg_E2Open_seq_ID,
           |REGEXP_EXTRACT(fl_nm,'E2OPEN_HPE_GROUPING_(\\\\d+)_(\\\\d+)_',2) AS ps_grpg_E2Open_extrt_dt,
           |REGEXP_EXTRACT(fl_nm,'E2OPEN_HPE_GROUPING_([^\\\\t]*)_(\\\\d+)',2) AS ps_grpg_E2Open_extrt_ts,
           |substring(ins_gmt_ts,0,10) AS ps_grpg_EAP_load_dt,
           |substring(ins_gmt_ts,12,18) AS ps_grpg_EAP_load_ts
           |from ea_common.ps_grpg_dmnsn
           |""".stripMargin).persist(StorageLevel.MEMORY_AND_DISK_SER)
      ps_grpg_dmnsn.createOrReplaceTempView("ps_grpg_dmnsn")
      logger.info("ps_grpg_dmnsn Loaded" + fl_nm)

      val psGrpgEndCust = spark.sql(
        s"""
           |select distinct psGrpgEndCust.* from PS_DMNSN_grp ps inner join ps_grpg_dmnsn psGrpgEndCust
           |ON ps.end_usr_rw_addr_id = psGrpgEndCust.rawaddr_id
           |AND coalesce(ps.end_usr_ky_cd,'unknown') = coalesce(psGrpgEndCust.rawaddr_ky_cd,'unknown')
           |where psGrpgEndCust.addr_typ_cd ='End User'
           |""".stripMargin)
      // val psGrpgEndCust_load = psGrpgEndCust.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psGrpgEndCust")
      logger.info("psGrpgEndCust Loaded")

      //**** Loading Entitled Dimension****
      val entitledPrtyDfTrsn = spark.sql(
        s"""
           |select trsn_i_1_id,
           |Entitled_Party_id,
           |BI_Enti_rsn_cd,
           |coalesce(a.bi_enttld_ctry_cd,prty.prty_ctry_cd) as bi_enttld_ctry_cd,bi_enttld_prty_br_typ from
           |(select distinct
           |ps.trsn_i_1_id,
           |CASE WHEN endCustEntldBMT1.bi_enttld_party_id IS NOT NULL THEN endCustEntldBMT1.bi_enttld_party_id WHEN ope.ope_enttld_prty_id IS NOT NULL AND
           |UPPER(ope.ope_enttld_prty_id) = 'UNMATCHED' THEN CONCAT('UNMATCHED','_',OPE.ope_enttld_prty_ctry ) WHEN ope.ope_enttld_prty_id IS NOT NULL and
           |UPPER(OPE.ope_enttld_prty_id) = 'UNREPORTED' THEN 'UNREPORTED' ELSE ope.ope_enttld_prty_id END AS Entitled_Party_id,
           |CASE WHEN endCustEntldBMT1.bi_enttld_party_id IS NOT NULL THEN 'BI Entitled Party BMT' WHEN (ope.ope_dstr_prty_id IS NOT NULL
           |AND ope.ope_dstr_prty_id <> 'NO DISTRIBUTOR') OR (ope.ope_enttld_prty_id IS NOT NULL AND ope.ope_enttld_prty_id <> '' ) THEN 'OPE'
           |ELSE 'NO ENTITLED PARTY' END AS BI_Enti_rsn_cd,
           |endCustEntldBMT1.bi_enttld_ctry_cd,
           |endCustEntldBMT1.bi_enttld_prty_br_typ
           |FROM PsDmnsnTRSNSet ps
           |LEFT JOIN OpePsDmnsnSubSet ope ON ps.trsn_i_1_id=OPE.trsn_i_1_id
           |LEFT JOIN ea_support_temp_an.Entitled_CUST_prty_DF_POS endCustEntldBMT1 ON ps.trsn_i_1_id = endCustEntldBMT1.ope_mf_trsn_ide2open_trsn_id)a
           |LEFT JOIN chnlptnr_common_prty_dmnsn prty ON prty.mdm_id=a.Entitled_Party_id where Entitled_Party_id IS NOT NULL
           |""".stripMargin).repartition($"trsn_i_1_id").persist(StorageLevel.MEMORY_AND_DISK_SER)
      entitledPrtyDfTrsn.createOrReplaceTempView("entitledPrtyDfTrsn")
      logger.info("entitledPrtyDfTrsn Loaded")

      val entitledPrtyDf = spark.sql(
        s"""
           |select distinct
           |ps.Entitled_Party_id as Entitled_Party_id,
           |CASE WHEN endCustEntldBMT1.bi_enttld_ctry_lvl_grp_id IS NOT NULL THEN endCustEntldBMT1.bi_enttld_ctry_lvl_grp_id
           |WHEN cast(prty.ctry_lvl_grp_id as string) IS NOT NULL THEN  cast(prty.ctry_lvl_grp_id as String) END AS BI_ENT_ctry_Lvl_grp_id,
           |CASE WHEN endCustEntldBMT1.bi_enttld_pfl_lvl_grp_id IS NOT NULL THEN endCustEntldBMT1.bi_enttld_pfl_lvl_grp_id
           |WHEN cast(prty.cy_profile_entity_id as string) IS NOT NULL THEN  cast(prty.cy_profile_entity_id as String) END AS BI_ENT_Profile_Lvl_grp_id,
           |CASE WHEN endCustEntldBMT1.bi_enttld_ctry_cd IS NOT NULL THEN endCustEntldBMT1.bi_enttld_ctry_cd
           |WHEN PRTY.prty_ctry_cd IS NOT NULL THEN PRTY.prty_ctry_cd  END AS bi_enttld_ctry_cd
           |FROM entitledPrtyDfTrsn ps
           |LEFT JOIN OpePsDmnsnSubSet ope ON ps.trsn_i_1_id=OPE.trsn_i_1_id
           |LEFT JOIN chnlptnr_common_prty_dmnsn prty ON prty.mdm_id=ps.Entitled_Party_id
           |LEFT JOIN ea_support_temp_an.Entitled_CUST_prty_DF_POS endCustEntldBMT1 ON ps.trsn_i_1_id = endCustEntldBMT1.ope_mf_trsn_ide2open_trsn_id
           |""".stripMargin)
      entitledPrtyDf.createOrReplaceTempView("Entitled_prty_DF_POS")
      logger.info("entitledPrtyDf Loaded")

      val entitledPrtyLoad = spark.sql(
        s"""
           |select
           |Entitled_Party_id,
           |BI_ENT_ctry_Lvl_grp_id,
           |BI_ENT_Profile_Lvl_grp_id,
           |BI_enttld_prty_addr_1,
           |BI_enttld_prty_addr_2,
           |BI_enttld_prty_cty,
           |BI_enttld_prty_ctry_prty_nm,
           |BI_enttld_prty_cmpt_ind,
           |BI_enttld_prty_cust_ind,
           |BI_enttld_prty_dnb_out_bsn_ind,
           |BI_enttld_prty_hq_ind,
           |BI_enttld_prty_indy_vtcl_nm,
           |BI_enttld_prty_indy_vtcl_sgm_nm,
           |BI_enttld_prty_lgl_ctry_nm,
           |BI_enttld_prty_lcl_nm,
           |BI_enttld_prty_ptnr_ind,
           |BI_enttld_prty_prty_stts_nm,
           |BI_enttld_prty_prty_typ_cd,
           |BI_enttld_prty_prty_typ_nm,
           |BI_enttld_prty_pstl_cd,
           |BI_enttld_prty_st_prvc,
           |BI_enttld_prty_st_prvc_nm,
           |BI_enttld_prty_vndr_ind,
           |BI_enttld_prty_EMDMH_ctry_cd,
           |BI_enttld_prty_EMDMH_ctry_nm,
           |BI_enttld_prty_pfl_Lvl_grp_nm,
           |BI_enttld_prty_CY_ctry_Lvl_grp_lcl_nm,
           |BI_enttld_prty_CY_gbl_lvl_grp_id,
           |BI_enttld_prty_CY_gbl_lvl_grp_nm,
           |BI_enttld_prty_ctry_Lvl_grp_nm
           |from
           |(select
           |DF_POS.Entitled_Party_id,
           |CASE WHEN DF_POS.Entitled_Party_id LIKE '%UNMATCHED%' OR DF_POS.Entitled_Party_id LIKE '%UNREPORTED%' THEN NULL ELSE
           |DF_POS.BI_ENT_ctry_Lvl_grp_id END AS BI_ENT_ctry_Lvl_grp_id,
           |CASE WHEN DF_POS.Entitled_Party_id LIKE '%UNMATCHED%' OR DF_POS.Entitled_Party_id LIKE '%UNREPORTED%' THEN NULL ELSE
           |DF_POS.BI_ENT_Profile_Lvl_grp_id END AS BI_ENT_Profile_Lvl_grp_id,
           |PRTY_ENT_PRTY.addr_1_cd AS BI_enttld_prty_addr_1,
           |PRTY_ENT_PRTY.addr_2_cd AS BI_enttld_prty_addr_2,
           |PRTY_ENT_PRTY.cty_cd AS BI_enttld_prty_cty,
           |PRTY_ENT_PRTY.prty_ctry_nm AS BI_enttld_prty_ctry_prty_nm,
           |PRTY_ENT_PRTY.cmpt_ind_cd AS BI_enttld_prty_cmpt_ind,
           |PRTY_ENT_PRTY.cust_ind_cd AS BI_enttld_prty_cust_ind,
           |PRTY_ENT_PRTY.out_of_business_ind AS BI_enttld_prty_dnb_out_bsn_ind,
           |PRTY_ENT_PRTY.hq_ind_cd AS BI_enttld_prty_hq_ind,
           |PRTY_ENT_PRTY.indy_vtcl_nm AS BI_enttld_prty_indy_vtcl_nm,
           |PRTY_ENT_PRTY.indy_vtcl_sgm_nm AS BI_enttld_prty_indy_vtcl_sgm_nm,
           |PRTY_ENT_PRTY.lgl_ctry_nm AS BI_enttld_prty_lgl_ctry_nm,
           |PRTY_ENT_PRTY.prty_lcl_nm AS BI_enttld_prty_lcl_nm,
           |PRTY_ENT_PRTY.ptnr_ind_cd AS BI_enttld_prty_ptnr_ind,
           |PRTY_ENT_PRTY.prty_stts_nm AS BI_enttld_prty_prty_stts_nm,
           |PRTY_ENT_PRTY.prty_typ_cd AS BI_enttld_prty_prty_typ_cd,
           |PRTY_ENT_PRTY.prty_typ_nm AS BI_enttld_prty_prty_typ_nm,
           |PRTY_ENT_PRTY.prty_pstl_cd AS BI_enttld_prty_pstl_cd,
           |PRTY_ENT_PRTY.st_prvc_cd AS BI_enttld_prty_st_prvc,
           |PRTY_ENT_PRTY.st_prvc_nm AS BI_enttld_prty_st_prvc_nm,
           |PRTY_ENT_PRTY.vndr_ind_cd AS BI_enttld_prty_vndr_ind,
           |PRTY_ENT_PRTY.emdmh_ctry_cd AS BI_enttld_prty_EMDMH_ctry_cd,
           |PRTY_ENT_PRTY.emdmh_ctry_nm AS BI_enttld_prty_EMDMH_ctry_nm,
           |PRTY_ENT_PRTY.cy_profile_entity_name AS BI_enttld_prty_pfl_Lvl_grp_nm,
           |PRTY_ENT_PRTY.ctry_lvl_lcl_languague_grp_nm AS BI_enttld_prty_CY_ctry_Lvl_grp_lcl_nm,
           |PRTY_ENT_PRTY.gbl_lvl_grp_id AS BI_enttld_prty_CY_gbl_lvl_grp_id,
           |PRTY_ENT_PRTY.gbl_lvl_grp_nm AS BI_enttld_prty_CY_gbl_lvl_grp_nm,
           |PRTY_ENT_PRTY.ctry_lvl_grp_nm AS BI_enttld_prty_ctry_Lvl_grp_nm,
           |row_number() over (partition by Entitled_Party_id order by Entitled_Party_id) rk
           |from Entitled_prty_DF_POS DF_POS
           |LEFT JOIN chnlptnr_common_prty_dmnsn PRTY_ENT_PRTY ON DF_POS.Entitled_Party_id = PRTY_ENT_PRTY.mdm_id
           |WHERE DF_POS.Entitled_Party_id IS NOT NULL)A where A.rk=1
           |""".stripMargin)

      // val Entitled_ps_prty_Load = entitledPrtyLoad.coalesce(20).write.mode("overwrite").format("orc").insertInto("ea_common_an"+ "." + "chnlptnr_ps_Entitled_dmnsn")
      logger.info("chnlptnr_ps_Entitled_dmnsn Loaded")

      //**** bmt_bi_deal_dmnsn LR1 implementation****
      val bmtDealDmnsnDf = spark.sql(
        s"""
           |select
           |eff_upd_strt_dt,
           |eff_upd_end_dt,
           |bs_prod_nr,
           |deal_id,
           |vol_sku,
           |mc_cd,
           |smb_ofr,
           |trsn_dt,
           |reporter_ctry,
           |sls_ord_nr,
           |reporter_id,
           |deal_dn_value,
           |rec_src,
           |prcg_prgm_nm,
           |prcg_prgm_grp,
           |mc_cd_dn,
           |deal_ind,
           |deal_ind_grp,
           |deal_ind_mc_pri_cd,
           |prcg_prgm_pri_cd,
           |prcg_prgm_deal_id
           |from ea_common.bmt_bi_deal_dmnsn
           |  where mc_cd<>''
           |""".stripMargin)
      bmtDealDmnsnDf.createOrReplaceTempView("bmtDealDmnsnDf")
      bmtDealDmnsnDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
      logger.info("bmtDealDmnsnDf View Created")
      //deal_ln_itm,

      val bmtDealDF = spark.sql(
        s"""
           |select trsn_i_1_id,MC_cd,mc_cd_dn,deal_id,deal_ind,deal_ind_grp,
           |upfrnt_deal_1_MC_CD,
           |upfrnt_deal_2_MC_CD,
           |upfrnt_deal_3_MC_CD,
           |backend_deal_1_MC_CD,
           |backend_deal_2_MC_CD,
           |backend_deal_3_MC_CD,
           |backend_deal_4_MC_CD,
           |upfrnt_deal_1_mc_dn_cd,
           |upfrnt_deal_2_mc_dn_cd,
           |upfrnt_deal_3_mc_dn_cd,
           |backend_deal_1_mc_dn_cd,
           |backend_deal_2_mc_dn_cd,
           |backend_deal_3_mc_dn_cd,
           |backend_deal_4_mc_dn_cd,
           |upfrnt_deal_1_deal_ind_mc_pri_cd,
           |upfrnt_deal_2_deal_ind_mc_pri_cd,
           |upfrnt_deal_3_deal_ind_mc_pri_cd,
           |backend_deal_1_deal_ind_mc_pri_cd,
           |backend_deal_2_deal_ind_mc_pri_cd,
           |backend_deal_3_deal_ind_mc_pri_cd,
           |backend_deal_4_deal_ind_mc_pri_cd from (select trsn_i_1_id,
           |CASE
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_1 THEN MC_cd_1
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_2 THEN MC_cd_2
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_3 THEN MC_cd_3
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_4 THEN MC_cd_4
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_5 THEN MC_cd_5
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_6 THEN MC_cd_6
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_7 THEN MC_cd_7 else NULL END AS MC_cd,
           |CASE
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_1 THEN mc_dn_cd_1
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_2 THEN mc_dn_cd_2
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_3 THEN mc_dn_cd_3
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_4 THEN mc_dn_cd_4
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_5 THEN mc_dn_cd_5
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_6 THEN mc_dn_cd_6
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_7 THEN mc_dn_cd_7 else NULL END AS mc_cd_dn,
           |CASE
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_1 THEN deal_id_1
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_2 THEN deal_id_2
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_3 THEN deal_id_3
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_4 THEN deal_id_4
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_5 THEN deal_id_5
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_6 THEN deal_id_6
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_7 THEN deal_id_7 else NULL END AS deal_id,
           |CASE
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_1 THEN deal_ind_1
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_2 THEN deal_ind_2
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_3 THEN deal_ind_3
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_4 THEN deal_ind_4
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_5 THEN deal_ind_5
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_6 THEN deal_ind_6
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_7 THEN deal_ind_7 else NULL END AS deal_ind,
           |CASE
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_1 THEN deal_ind_grp_1
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_2 THEN deal_ind_grp_2
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_3 THEN deal_ind_grp_3
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_4 THEN deal_ind_grp_4
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_5 THEN deal_ind_grp_5
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_6 THEN deal_ind_grp_6
           |WHEN MIN_Priority = deal_ind_mc_pri_cd_7 THEN deal_ind_grp_7 else NULL END AS deal_ind_grp,
           |MC_cd_1 as upfrnt_deal_1_MC_CD,
           |MC_cd_2 as upfrnt_deal_2_MC_CD,
           |MC_cd_3 as upfrnt_deal_3_MC_CD,
           |MC_cd_4 as backend_deal_1_MC_CD,
           |MC_cd_5 as backend_deal_2_MC_CD,
           |MC_cd_6 as backend_deal_3_MC_CD,
           |MC_cd_7 as backend_deal_4_MC_CD,
           |mc_dn_cd_1 as upfrnt_deal_1_mc_dn_cd,
           |mc_dn_cd_2 as upfrnt_deal_2_mc_dn_cd,
           |mc_dn_cd_3 as upfrnt_deal_3_mc_dn_cd,
           |mc_dn_cd_4 as backend_deal_1_mc_dn_cd,
           |mc_dn_cd_5 as backend_deal_2_mc_dn_cd,
           |mc_dn_cd_6 as backend_deal_3_mc_dn_cd,
           |mc_dn_cd_7 as backend_deal_4_mc_dn_cd,
           |deal_ind_mc_pri_cd_1 as upfrnt_deal_1_deal_ind_mc_pri_cd,
           |deal_ind_mc_pri_cd_2 as upfrnt_deal_2_deal_ind_mc_pri_cd,
           |deal_ind_mc_pri_cd_3 as upfrnt_deal_3_deal_ind_mc_pri_cd,
           |deal_ind_mc_pri_cd_4 as backend_deal_1_deal_ind_mc_pri_cd,
           |deal_ind_mc_pri_cd_5 as backend_deal_2_deal_ind_mc_pri_cd,
           |deal_ind_mc_pri_cd_6 as backend_deal_3_deal_ind_mc_pri_cd,
           |deal_ind_mc_pri_cd_7 as backend_deal_4_deal_ind_mc_pri_cd
           |FROM
           |(select PS.trsn_i_1_id,
           |dealBMTU1.MC_cd as MC_cd_1,
           |dealBMTU2.MC_cd as MC_cd_2,
           |dealBMTU3.MC_cd as MC_cd_3,
           |dealBMTB1.MC_cd as MC_cd_4,
           |dealBMTB2.MC_cd as MC_cd_5,
           |dealBMTB3.MC_cd as MC_cd_6,
           |dealBMTB4.MC_cd as MC_cd_7,
           |dealBMTU1.mc_cd_dn as mc_dn_cd_1,
           |dealBMTU2.mc_cd_dn as mc_dn_cd_2,
           |dealBMTU3.mc_cd_dn as mc_dn_cd_3,
           |dealBMTB1.mc_cd_dn as mc_dn_cd_4,
           |dealBMTB2.mc_cd_dn as mc_dn_cd_5,
           |dealBMTB3.mc_cd_dn as mc_dn_cd_6,
           |dealBMTB4.mc_cd_dn as mc_dn_cd_7,
           |dealBMTU1.deal_id as deal_id_1,
           |dealBMTU2.deal_id as deal_id_2,
           |dealBMTU3.deal_id as deal_id_3,
           |dealBMTB1.deal_id as deal_id_4,
           |dealBMTB2.deal_id as deal_id_5,
           |dealBMTB3.deal_id as deal_id_6,
           |dealBMTB4.deal_id as deal_id_7,
           |dealBMTU1.deal_ind as deal_ind_1,
           |dealBMTU2.deal_ind as deal_ind_2,
           |dealBMTU3.deal_ind as deal_ind_3,
           |dealBMTB1.deal_ind as deal_ind_4,
           |dealBMTB2.deal_ind as deal_ind_5,
           |dealBMTB3.deal_ind as deal_ind_6,
           |dealBMTB4.deal_ind as deal_ind_7,
           |dealBMTU1.deal_ind_grp as deal_ind_grp_1,
           |dealBMTU2.deal_ind_grp as deal_ind_grp_2,
           |dealBMTU3.deal_ind_grp as deal_ind_grp_3,
           |dealBMTB1.deal_ind_grp as deal_ind_grp_4,
           |dealBMTB2.deal_ind_grp as deal_ind_grp_5,
           |dealBMTB3.deal_ind_grp as deal_ind_grp_6,
           |dealBMTB4.deal_ind_grp as deal_ind_grp_7,
           |trim(dealBMTU1.deal_ind_mc_pri_cd) as deal_ind_mc_pri_cd_1,
           |dealBMTU2.deal_ind_mc_pri_cd as deal_ind_mc_pri_cd_2,
           |dealBMTU3.deal_ind_mc_pri_cd as deal_ind_mc_pri_cd_3,
           |dealBMTB1.deal_ind_mc_pri_cd as deal_ind_mc_pri_cd_4,
           |dealBMTB2.deal_ind_mc_pri_cd as deal_ind_mc_pri_cd_5,
           |dealBMTB3.deal_ind_mc_pri_cd as deal_ind_mc_pri_cd_6,
           |dealBMTB4.deal_ind_mc_pri_cd as deal_ind_mc_pri_cd_7,
           |least(coalesce(dealBMTU1.deal_ind_mc_pri_cd,'99999'),coalesce(dealBMTU2.deal_ind_mc_pri_cd,'99999'),coalesce(dealBMTU3.deal_ind_mc_pri_cd,'99999'),
           |coalesce(dealBMTB1.deal_ind_mc_pri_cd,'99999'),coalesce(dealBMTB2.deal_ind_mc_pri_cd,'99999'),coalesce(dealBMTB3.deal_ind_mc_pri_cd,'99999'),
           |coalesce(dealBMTB4.deal_ind_mc_pri_cd,'99999')) as MIN_Priority
           |from psDmnsnIncTrsn PS
           |LEFT JOIN ps_prs_dmnsnDF ps_prs ON PS.trsn_i_1_id=ps_prs.trsn_i_3_id
           |LEFT OUTER JOIN bmtDealDmnsnDf dealBMTU1 ON upper(trim(ps_prs.upfrnt_deal_1__1_cd)) = upper(trim(dealBMTU1.MC_cd))
           |LEFT OUTER JOIN bmtDealDmnsnDf dealBMTU2 ON upper(trim(ps_prs.upfrnt_deal_2_m_cd)) = upper(trim(dealBMTU2.MC_cd))
           |LEFT OUTER JOIN bmtDealDmnsnDf dealBMTU3 ON upper(trim(ps_prs.upfrnt_deal_3_m_cd)) = upper(trim(dealBMTU3.MC_cd))
           |LEFT OUTER JOIN bmtDealDmnsnDf dealBMTB1 ON upper(trim(ps_prs.backend_deal_1_m_cd)) = upper(trim(dealBMTB1.MC_cd))
           |LEFT OUTER JOIN bmtDealDmnsnDf dealBMTB2 ON upper(trim(ps_prs.backend_deal_2_m_cd)) = upper(trim(dealBMTB2.MC_cd))
           |LEFT OUTER JOIN bmtDealDmnsnDf dealBMTB3 ON upper(trim(ps_prs.backend_deal_3_m_cd)) = upper(trim(dealBMTB3.MC_cd))
           |LEFT OUTER JOIN bmtDealDmnsnDf dealBMTB4 ON upper(trim(ps_prs.backend_deal_4_m_cd)) = upper(trim(dealBMTB4.MC_cd))
           | )A )b where MC_cd IS NOT NULL OR mc_cd_dn IS NOT NULL OR deal_id IS NOT NULL OR deal_ind IS NOT NULL OR deal_ind_grp IS NOT NULL
           |""".stripMargin)

      // val bmtDealDF_load = bmtDealDF.write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "bmtDealDF")
      logger.info("bmtDealDF Loaded")

      //**** Reading Incremental Data for without SN POS****
      val ps_rw_addr_dmnsn = spark.sql(
        s"""
           |select distinct
           |rw_addr_i_1_id,
           |lct_1_nm,
           |lctn_addr_1_cd,
           |lctn_addr_2_cd,
           |lctn_ct_1_nm,
           |lctn_s_1_cd,
           |lctn_zi_1_cd,
           |normalized_lctn_counrty_cd,
           |substring(fl_nm,0,22) AS E2Open_rwaddr_extrt_nm,
           |REGEXP_EXTRACT(fl_nm,'E2OPEN_HPE_RAW_ADDRESS_(\\\\d+)',1) AS E2Open_rwaddr_seq_ID,
           |REGEXP_EXTRACT(fl_nm,'E2OPEN_HPE_RAW_ADDRESS_(\\\\d+)_(\\\\d+)_',2) AS E2Open_rwaddr_extrt_dt,
           |REGEXP_EXTRACT(fl_nm,'E2OPEN_HPE_RAW_ADDRESS_([^\\\\t]*)_(\\\\d+)',2) AS E2Open_rwaddr_extrt_ts,
           |substring(ins_gmt_ts,0,10) AS ps_rwaddr_eap_load_dt,
           |substring(ins_gmt_ts,12,18) AS ps_rwaddr_eap_load_ts
           |from ea_common.ps_rw_addr_dmnsn
           |""".stripMargin).persist(StorageLevel.MEMORY_AND_DISK_SER)

      ps_rw_addr_dmnsn.createOrReplaceTempView("ps_rw_addr_dmnsn")
      logger.info("ps_rw_addr_dmnsn Loaded" + fl_nm)

      val SPRM_ptnr_pfl_dmnsn = spark.sql(
        s"""
           |select
           |ptnr_pfl_row_id,
           |prnt_ptnr_pfl_row_id
           |from
           |(select ptnr_pfl_row_id,prnt_ptnr_pfl_row_id,row_number() over(partition by ptnr_pfl_row_id order by ins_gmt_ts desc)rk
           |from ea_common.SPRM_ptnr_pfl_dmnsn SP INNER JOIN (SELECT ptnr_pro_i_2_id FROM ps_grpg_dmnsn WHERE addr_typ_cd ='Sold To') PSG
           |ON PSG.ptnr_pro_i_2_id=SP.ptnr_pfl_row_id where prnt_ptnr_pfl_row_id is not null)a where rk=1
           |""".stripMargin)

      // val SPRM_ptnr_pfl_dmnsn_load = SPRM_ptnr_pfl_dmnsn.coalesce(10).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "SPRM_ptnr_pfl_dmnsn_DF")
      //SPRM_ptnr_pfl_dmnsn.createOrReplaceTempView("SPRM_ptnr_pfl_dmnsn")
      logger.info("SPRM_ptnr_pfl_dmnsn Loaded")

      val psPsPrsDmnsn = spark.sql(
        s"""
           |select ps.src_sys_nm,
           |ps.rec_src,
           |ps.backend_deal_1_cleansed_cd,
           |ps.backend_deal_1_rptd_cd,
           |ps.backend_deal_2_cleansed_cd,
           |ps.backend_deal_2_rptd_cd,
           |ps.backend_deal_3_cleansed_cd,
           |ps.backend_deal_3_rptd_cd,
           |ps.backend_deal_4_cleansed_cd,
           |ps.backend_deal_4_rptd_cd,
           |ps.bll_to_asian_addr_cd,
           |ps.bll_to_co_tx_id,
           |ps.bll_to_co_tx_id_enr_id,
           |ps.bll_to_cntct_nm,
           |ps.bll_to_ctry_rptd_cd,
           |ps.bll_to_ky_cd,
           |ps.bll_to_ph_nr,
           |ps.bll_to_rw_addr_id,
           |ps.bll_to_rptd_id,
           |ps.nmso_nm,
           |ps.nmso_dt,
           |ps.stndg_ofr_nr,
           |ps.deal_reg_1_cleansed_cd,
           |ps.deal_reg_1_rptd_cd,
           |ps.deal_reg_2_cleansed_cd,
           |ps.deal_reg_2_rptd_cd,
           |ps.nt_cst_aftr_rbt_lcy_ex_1_cd,
           |ps.ptnr_prch_prc_lcy_un_1_cd,
           |ps.ptnr_sl_prc_lcy_un_1_cd,
           |ps.ptnr_sl_prc_usd_un_1_cd,
           |ps.bndl_qt_1_cd,
           |ps.sale_qty_cd,
           |ps.iud_fl_1_cd,
           |ps.rsrv_fld_1_cd,
           |ps.rsrv_fld_2_cd,
           |ps.rptg_prd_strt_dt,
           |ps.rptg_prd_end_dt,
           |ps.rsrv_fld_5_cd,
           |ps.rsrv_fld_6_cd,
           |ps.rsrv_fld_7_cd,
           |ps.rsrv_fld_8_cd,
           |ps.rsrv_fld_9_cd,
           |ps.rsrv_fld_10_cd,
           |ps.bndl_prod_i_1_id,
           |ps.prod_ln_i_1_id,
           |ps.ptnr_bndl_prod_i_1_id,
           |ps.ptnr_inrn_sk_1_cd,
           |ps.hpe_prod_i_1_id,
           |ps.rptd_prod_i_1_id,
           |ps.vl_vol_fl_1_cd,
           |ps.reporter_i_2_id,
           |ps.sl_frm_cntc_1_nm,
           |ps.sl_frm_ctry_rptd_cd,
           |ps.sl_frm_ky_cd,
           |ps.sl_frm_p_1_nr,
           |ps.sl_frm_rw_addr_i_1_id,
           |ps.sl_frm_rptd_id,
           |ps.shp_frm_cntct_nm,
           |ps.shp_frm_ctry_rptd_cd,
           |ps.shp_frm_ky_cd,
           |ps.shp_frm_ph_nr,
           |ps.shp_frm_rw_addr_id,
           |ps.shp_frm_rptd_id,
           |ps.shp_to_asian_addr_cd,
           |ps.shp_to_co_tx_id,
           |ps.shp_to_co_tx_id_enr_id,
           |ps.shp_to_cntct_nm,
           |ps.shp_to_ctry_rptd_cd,
           |ps.shp_to_ky_cd,
           |ps.shp_to_ph_nr,
           |ps.shp_to_rw_addr_id,
           |ps.shipto_rptd_id,
           |ps.sld_to_asian_addr_cd,
           |ps.sld_to_co_tx_id,
           |ps.sld_to_co_tx_id_enr_id,
           |ps.sld_to_cntct_nm,
           |ps.sld_to_ctry_rptd_cd,
           |ps.sld_to_ky_cd,
           |ps.sld_to_ph_nr,
           |ps.sld_to_rw_addr_id,
           |ps.sld_to_rptd_id,
           |ps.end_usr_addr_src_rptd_cd,
           |ps.end_usr_asian_addr_cd,
           |ps.end_usr_co_tx_id,
           |ps.end_usr_co_tx_id_enr_id,
           |ps.end_usr_cntct_nm,
           |ps.end_usr_ctry_rptd_cd,
           |ps.end_usr_ky_cd,
           |ps.end_usr_ph_nr,
           |ps.end_usr_rw_addr_id,
           |ps.end_usr_rptd_id,
           |ps.enttld_asian_addr_cd,
           |ps.enttld_co_tx_id,
           |ps.enttld_co_tx_id_enr_id,
           |ps.enttld_cntct_nm,
           |ps.enttld_ctry_rptd_cd,
           |ps.enttld_ky_cd,
           |ps.enttld_ph_nr,
           |ps.enttld_rw_addr_id,
           |ps.enttld_rptd_id,
           |ps.agt_flg_cd,
           |ps.brm_err_flg_cd,
           |ps.crss_shp_flg_cd,
           |ps.cust_to_ptnr_po_nr,
           |ps.trs_1_dt,
           |ps.drp_shp_flg_cd,
           |ps.fl_i_1_id,
           |ps.fl_rcv_1_dt,
           |ps.hpe_inv_nr,
           |ps.nt_cst_aftr_rbt_curr_cd,
           |ps.org_sls_nm,
           |ps.orgl_hpe_asngd_trsn_id,
           |ps.orgl_trsn_i_1_id,
           |ps.prnt_trsn_i_1_id,
           |ps.ptnr_inrn_trsn_id,
           |ps.inv_dt,
           |ps.ptnr_inv_nr,
           |ps.ptnr_prch_curr_cd,
           |ps.ptnr_sls_rep_nm,
           |ps.ptnr_sl_prc_curr_cd,
           |ps.sm_dt,
           |ps.ptnr_to_hpe_po_nr,
           |ps.prv_trsn_i_1_id,
           |ps.prod_orgn_cd,
           |ps.prchg_src_ws_ind_cd,
           |ps.splyr_iso_ctry_cd,
           |ps.tty_mgr_cd,
           |ps.trsn_i_1_id,
           |ps.vldtn_wrnn_1_cd,
           |ps.upfrnt_deal_1_rptd_cd,
           |ps.upfrnt_deal_1_cleansed_cd,
           |ps.upfrnt_deal_2_rptd_cd,
           |ps.upfrnt_deal_2_cleansed_cd,
           |ps.upfrnt_deal_3_rptd_cd,
           |ps.upfrnt_deal_3_cleansed_cd,
           |ps.src_sys_upd_ts,
           |ps.src_sys_ky,
           |ps.lgcl_dlt_ind,
           |ps.ins_gmt_ts,
           |ps.upd_gmt_ts,
           |ps.src_sys_extrc_gmt_ts,
           |ps.src_sys_btch_nr,
           |ps.fl_nm,
           |ps.ld_jb_nr,
           |ps_prs.backend_deal_1_ln_itm_cd,
           |ps_prs.backend_deal_1_m_cd,
           |ps_prs.backend_deal_1_vrsn_cd,
           |ps_prs.backend_deal_2_ln_itm_cd,
           |ps_prs.backend_deal_2_m_cd,
           |ps_prs.backend_deal_2_vrsn_cd,
           |ps_prs.backend_deal_3_ln_itm_cd,
           |ps_prs.backend_deal_3_m_cd,
           |ps_prs.backend_deal_3_vrsn_cd,
           |ps_prs.backend_deal_4_ln_itm_cd,
           |ps_prs.backend_deal_4_m_cd,
           |ps_prs.backend_deal_4_vrsn_cd,
           |ps_prs.ndp_coefficien_1_cd,
           |ps_prs.ndp_estmd_lcy_ext_cd,
           |ps_prs.ndp_estmd_lcy_unt_cd,
           |ps_prs.ndp_estmd_usd_ext_cd,
           |ps_prs.ndp_estmd_usd_unt_cd,
           |ps_prs.hpe_ord_id,
           |ps_prs.hpe_ord_itm_mcc_cd,
           |ps_prs.hpe_ord_itm_nr,
           |ps_prs.hpe_ord_dt,
           |ps_prs.hpe_ord_itm_disc_lcy_ext_cd,
           |ps_prs.hpe_ord_itm_disc_usd_ext_cd,
           |ps_prs.hpe_upfrnt_deal_id,
           |ps_prs.hpe_ord_itm_cleansed_deal_id,
           |ps_prs.hpe_upfrnt_deal_typ_cd,
           |ps_prs.hpe_ord_itm_orig_deal_id,
           |ps_prs.hpe_upfrnt_disc_lcy_ext_cd,
           |ps_prs.hpe_upfrnt_disc_usd_ext_cd,
           |ps_prs.hpe_ord_end_usr_addr_id,
           |ps_prs.hpe_upfrnt_deal_end_usr_addr_id,
           |ps_prs.upfrnt_deal_1_typ_cd,
           |ps_prs.upfrnt_deal_1_end_usr_addr_id,
           |ps_prs.backend_deal_1_typ_cd,
           |ps_prs.backend_deal_1_end_usr_addr_id,
           |ps_prs.ptnr_prch_prc_lcy_ex_1_cd,
           |ps_prs.ptnr_prch_prc_lcy_un_2_cd,
           |ps_prs.ptnr_prch_prc_usd_ex_1_cd,
           |ps_prs.ptnr_prch_prc_usd_un_1_cd,
           |ps_prs.nt_cst_aftr_rbt_lcy_ex_2_cd,
           |ps_prs.nt_cst_aftr_rbt_usd_ext_cd,
           |ps_prs.nt_cst_curr_cd,
           |ps_prs.rbt_amt_lcy_ext_cd,
           |ps_prs.rbt_amt_lcy_unt_cd,
           |ps_prs.rbt_amt_usd_ext_cd,
           |ps_prs.rbt_amt_usd_unt_cd,
           |ps_prs.ptnr_sl_prc_lcy_ext_cd,
           |ps_prs.ptnr_sl_prc_lcy_un_1_cd as ps_prs_ptnr_sl_prc_lcy_un_1_cd,
           |ps_prs.ptnr_sl_prc_usd_ext_cd,
           |ps_prs.ptnr_sl_prc_usd_un_2_cd,
           |ps_prs.backend_deal_1_dscnt_lcy_ext_cd,
           |ps_prs.backend_deal_1_dscnt_lcy_unt_cd,
           |ps_prs.backend_deal_1_dscnt_usd_ext_cd,
           |ps_prs.backend_deal_1_dscnt_usd_unt_cd,
           |ps_prs.backend_deal_1_prc_lcy_unt_cd,
           |ps_prs.backend_deal_1_prc_usd_unt_cd,
           |ps_prs.backend_deal_2_dscnt_lcy_ext_cd,
           |ps_prs.backend_deal_2_dscnt_lcy_unt_cd,
           |ps_prs.backend_deal_2_dscnt_usd_ext_cd,
           |ps_prs.backend_deal_2_dscnt_usd_unt_cd,
           |ps_prs.backend_deal_2_prc_lcy_unt_cd,
           |ps_prs.backend_deal_2_prc_usd_unt_cd,
           |ps_prs.backend_deal_3_dscnt_lcy_ext_cd,
           |ps_prs.backend_deal_3_dscnt_lcy_unt_cd,
           |ps_prs.backend_deal_3_dscnt_usd_ext_cd,
           |ps_prs.backend_deal_3_dscnt_usd_unt_cd,
           |ps_prs.backend_deal_3_prc_lcy_unt_cd,
           |ps_prs.backend_deal_3_prc_usd_unt_cd,
           |ps_prs.backend_deal_4_dscnt_lcy_ext_cd,
           |ps_prs.backend_deal_4_dscnt_lcy_unt_cd,
           |ps_prs.backend_deal_4_dscnt_usd_ext_cd,
           |ps_prs.backend_deal_4_dscnt_usd_unt_cd,
           |ps_prs.backend_deal_4_prc_lcy_unt_cd,
           |ps_prs.backend_deal_4_prc_usd_unt_cd,
           |ps_prs.backend_rbt_1_lcy_unt_cd,
           |ps_prs.backend_rbt_1_usd_unt_cd,
           |ps_prs.backend_rbt_2_lcy_unt_cd,
           |ps_prs.backend_rbt_2_usd_unt_cd,
           |ps_prs.backend_rbt_3_lcy_unt_cd,
           |ps_prs.backend_rbt_3_usd_unt_cd,
           |ps_prs.backend_rbt_4_lcy_unt_cd,
           |ps_prs.backend_rbt_4_usd_unt_cd,
           |ps_prs.pa_dscnt_lcy_un_1_cd,
           |ps_prs.pa_dscnt_usd_un_1_cd,
           |ps_prs.asp_lcy_ex_1_cd,
           |ps_prs.asp_lcy_un_1_cd,
           |ps_prs.asp_usd_ex_1_cd,
           |ps_prs.asp_usd_un_1_cd,
           |ps_prs.AVRG_deal_nt_LCY_EXT_nm,
           |ps_prs.AVRG_deal_nt_LCY_unt_nm,
           |ps_prs.AVRG_deal_nt_usd_EXT_nm,
           |ps_prs.AVRG_deal_nt_usd_unt_nm,
           |ps_prs.AVRG_deal_nt_VCY_EXT_nm,
           |ps_prs.AVRG_deal_nt_VCY_unt_nm,
           |ps_prs.deal_nt_lcy_ext_cd,
           |ps_prs.deal_nt_lcy_un_1_cd,
           |ps_prs.deal_nt_usd_ext_cd,
           |ps_prs.deal_nt_usd_un_1_cd,
           |ps_prs.deal_nt_VCY_EXT_nm,
           |ps_prs.deal_nt_VCY_unt_nm,
           |ps_prs.lst_prc_lcy_ex_1_cd,
           |ps_prs.lst_prc_lcy_un_1_cd,
           |ps_prs.lst_prc_usd_ex_1_cd,
           |ps_prs.lst_prc_usd_un_1_cd,
           |ps_prs.lst_prc_VCY_EXT_nm,
           |ps_prs.lst_prc_VCY_unt_nm,
           |ps_prs.ndp_lcy_ex_1_cd,
           |ps_prs.ndp_lcy_un_1_cd,
           |ps_prs.ndp_usd_ex_1_cd,
           |ps_prs.ndp_usd_un_1_cd,
           |ps_prs.NDP_VCY_EXT_nm,
           |ps_prs.NDP_VCY_unt_nm,
           |ps_prs.upfrnt_deal_1_dscnt_lcy_ex_1_cd,
           |ps_prs.upfrnt_deal_1_dscnt_lcy_un_1_cd,
           |ps_prs.upfrnt_deal_1_dscnt_usd_ex_1_cd,
           |ps_prs.upfrnt_deal_1_dscnt_usd_un_1_cd,
           |ps_prs.upfrnt_deal_1_prc_lcy_un_1_cd,
           |ps_prs.upfrnt_deal_1_prc_usd_un_1_cd,
           |ps_prs.upfrnt_deal_2_dscnt_lcy_ext_cd,
           |ps_prs.upfrnt_deal_2_dscnt_lcy_unt_cd,
           |ps_prs.upfrnt_deal_2_dscnt_usd_ext_cd,
           |ps_prs.upfrnt_deal_2_dscnt_usd_unt_cd,
           |ps_prs.upfrnt_deal_2_prc_lcy_unt_cd,
           |ps_prs.upfrnt_deal_2_prc_usd_unt_cd,
           |ps_prs.upfrnt_deal_3_dscnt_lcy_ext_cd,
           |ps_prs.upfrnt_deal_3_dscnt_lcy_unt_cd,
           |ps_prs.upfrnt_deal_3_dscnt_usd_ext_cd,
           |ps_prs.upfrnt_deal_3_dscnt_usd_unt_cd,
           |ps_prs.upfrnt_deal_3_prc_lcy_unt_cd,
           |ps_prs.upfrnt_deal_3_prc_usd_unt_cd,
           |ps_prs.iud_fl_8_cd,
           |ps_prs.rsrv_fld_1_nm,
           |ps_prs.rsrv_fld_2_nm,
           |ps_prs.rsrv_fld_3_nm,
           |ps_prs.rsrv_fld_4_nm,
           |ps_prs.rsrv_fld_5_nm,
           |ps_prs.rsrv_fld_6_nm,
           |ps_prs.rsrv_fld_7_nm,
           |ps_prs.rsrv_fld_8_nm,
           |ps_prs.rsrv_fld_9_nm,
           |ps_prs.rsrv_fld_10_nm,
           |ps_prs.upfrnt_deal_1_ln_it_1_cd,
           |ps_prs.upfrnt_deal_1__1_cd,
           |ps_prs.upfrnt_deal_1_vrs_1_cd,
           |ps_prs.upfrnt_deal_2_ln_itm_cd,
           |ps_prs.upfrnt_deal_2_m_cd,
           |ps_prs.upfrnt_deal_2_vrsn_cd,
           |ps_prs.upfrnt_deal_3_ln_itm_cd,
           |ps_prs.upfrnt_deal_3_m_cd,
           |ps_prs.upfrnt_deal_3_vrsn_cd,
           |ps_prs.asp_coefficien_1_cd,
           |ps_prs.AVRG_deal_nt_COEFFICIENT_nm,
           |ps_prs.deal_geo_cd,
           |ps_prs.dscnt_geo_cd,
           |ps_prs.exch_rate_pcy_to_usd_cd,
           |ps_prs.exch_rate_lcy_to_us_1_cd,
           |ps_prs.EX_rate_VCY_to_usd_nm,
           |ps_prs.used_cur_1_cd,
           |ps_prs.valtn_curr_cd,
           |ps_prs.cust_app_2_cd,
           |ps_prs.pa_dscn_1_cd,
           |ps_prs.p_1_nr,
           |ps_prs.used_prc_ds_1_cd,
           |ps_prs.valtn_trsn_i_1_id,
           |ps_prs.valtn_wrnn_1_cd,
           |ps_prs.fl_i_6_id,
           |ps_prs.prnt_trsn_i_4_id,
           |ps_prs.prv_trsn_i_5_id,
           |ps_prs.reporter_i_7_id,
           |ps_prs.trsn_i_3_id,
           |substring(ps.fl_nm,0,14) AS ps_E2Open_extrt_nm,
           |REGEXP_EXTRACT(ps.fl_nm,'E2OPEN_HPE_POS_(\\\\d+)',1) AS ps_E2Open_seq_ID,
           |from_unixtime(unix_timestamp(split(regexp_replace(ps.fl_nm,".txt",""),"_")[4],"yyyyMMdd"),"yyyy-MM-dd") as ps_E2Open_extrt_dt,
           |from_unixtime(unix_timestamp(split(regexp_replace(ps.fl_nm,".txt",""),"_")[5],"HHmmss"),"HH:mm:ss") as ps_E2Open_extrt_ts,
           |substring(ps.ins_gmt_ts,0,10) AS ps_EAP_load_dt,
           |substring(ps.ins_gmt_ts,12,18) AS ps_EAP_load_ts,
           |substring(ps_prs.fl_nm,0,24) AS ps_prs_E2Open_extrt_nm,
           |REGEXP_EXTRACT(ps_prs.fl_nm,'E2OPEN_HPE_POS_VALUATION_(\\\\d+)',1) AS ps_prs_E2Open_seq_ID,
           |from_unixtime(unix_timestamp(split(regexp_replace(ps_prs.fl_nm,".txt",""),"_")[5],"yyyyMMdd"),"yyyy-MM-dd") as ps_prs_E2Open_extrt_dt,
           |from_unixtime(unix_timestamp(split(regexp_replace(ps_prs.fl_nm,".txt",""),"_")[6],"HHmmss"),"HH:mm:ss") as ps_prs_E2Open_extrt_ts,
           |substring(ps_prs.ins_gmt_ts,0,10) AS ps_prs_EAP_load_dt,
           |substring(ps_prs.ins_gmt_ts,12,18) AS ps_prs_EAP_load_ts
           |from ps_dmnsnDF ps
           |LEFT JOIN ps_prs_dmnsnDF ps_prs ON ps.trsn_i_1_id = ps_prs.trsn_i_3_id
           |""".stripMargin)

      // var ps_ps_prs_dmnsn_load = psPsPrsDmnsn.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "ps_ps_prsDF")
      logger.info("*******ps_ps_prsDF table loaded********" + fl_nm)

      val PS_DMNSN_rwaddr = spark.sql(
        s"""
           |select
           |distinct
           |bll_to_rw_addr_id,
           |sl_frm_rw_addr_i_1_id,
           |shp_to_rw_addr_id,
           |shp_frm_rw_addr_id,
           |sld_to_rw_addr_id,
           |end_usr_rw_addr_id,
           |enttld_rw_addr_id
           |from ps_dmnsnDF
           |""".stripMargin)
      PS_DMNSN_rwaddr.createOrReplaceTempView("PS_DMNSN_rwaddr")
      PS_DMNSN_rwaddr.persist(StorageLevel.MEMORY_AND_DISK_SER)
      logger.info("PS_DMNSN_rwaddr view created")

      val psRwAddrBillTo = spark.sql(s"""select distinct psRwAddrBillTo.* from PS_DMNSN_rwaddr ps INNER JOIN ps_rw_addr_dmnsn psRwAddrBillTo
      ON ps.bll_to_rw_addr_id = psRwAddrBillTo.rw_addr_i_1_id""")
      // val psRwAddrBillTo_load = psRwAddrBillTo.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psRwAddrBillTo")
      logger.info("psRwAddrBillTo table loaded")

      val psRwAddrSellFrm = spark.sql(s"""select distinct psRwAddrSellFrm.* from PS_DMNSN_rwaddr ps INNER JOIN ps_rw_addr_dmnsn psRwAddrSellFrm
      ON ps.sl_frm_rw_addr_i_1_id = psRwAddrSellFrm.rw_addr_i_1_id""")
      // val psRwAddrSellFrm_load = psRwAddrSellFrm.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psRwAddrSellFrm")
      logger.info("psRwAddrSellFrm table loaded")

      val psRwAddrShipTo = spark.sql(s"""select distinct psRwAddrShipTo.* from PS_DMNSN_rwaddr ps INNER JOIN ps_rw_addr_dmnsn psRwAddrShipTo
      ON ps.shp_to_rw_addr_id = psRwAddrShipTo.rw_addr_i_1_id""")
      // val psRwAddrShipTo_load = psRwAddrShipTo.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psRwAddrShipTo")
      logger.info("psRwAddrShipTo table loaded")

      val psRwAddrShipFrm = spark.sql(s"""select distinct psRwAddrShipFrm.* from PS_DMNSN_rwaddr ps INNER JOIN ps_rw_addr_dmnsn psRwAddrShipFrm
      ON ps.shp_frm_rw_addr_id = psRwAddrShipFrm.rw_addr_i_1_id""")
      // val psRwAddrShipFrm_load = psRwAddrShipFrm.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psRwAddrShipFrm")
      logger.info("psRwAddrShipFrm table loaded")

      val psRwAddrSoldTo = spark.sql(s"""select distinct psRwAddrSoldTo.* from PS_DMNSN_rwaddr ps INNER JOIN ps_rw_addr_dmnsn psRwAddrSoldTo
      ON ps.sld_to_rw_addr_id = psRwAddrSoldTo.rw_addr_i_1_id""")
      // val psRwAddrSoldTo_load = psRwAddrSoldTo.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psRwAddrSoldTo")
      logger.info("psRwAddrSoldTo table loaded")

      val psRwAddrEndCust = spark.sql(s"""select distinct psRwAddrEndCust.* from PS_DMNSN_rwaddr ps INNER JOIN ps_rw_addr_dmnsn psRwAddrEndCust
      ON ps.end_usr_rw_addr_id = psRwAddrEndCust.rw_addr_i_1_id""")
      // val psRwAddrEndCust_load = psRwAddrEndCust.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psRwAddrEndCust")
      logger.info("psRwAddrEndCust table loaded")

      val psRwAddrEntltd = spark.sql(s"""select distinct psRwAddrEntltd.* from PS_DMNSN_rwaddr ps INNER JOIN ps_rw_addr_dmnsn psRwAddrEntltd
      ON ps.enttld_rw_addr_id = psRwAddrEntltd.rw_addr_i_1_id""")
      // val psRwAddrEntltd_load = psRwAddrEntltd.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psRwAddrEntltd")
      logger.info("psRwAddrEntltd table loaded")

      val psGrpgBillTo = spark.sql(
        s"""
           |select distinct psGrpgBillTo.* from PS_DMNSN_grp ps inner join ps_grpg_dmnsn psGrpgBillTo
           |ON ps.bll_to_rw_addr_id = psGrpgBillTo.rawaddr_id
           |AND coalesce(ps.bll_to_ky_cd,'unknown') = coalesce(psGrpgBillTo.rawaddr_ky_cd,'unknown')
           |where psGrpgBillTo.addr_typ_cd ='Bill To'
           |""".stripMargin)
      // val psGrpgBillTo_load = psGrpgBillTo.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psGrpgBillTo")
      logger.info("psGrpgBillTo Loaded")

      val psGrpgSellFrm = spark.sql(
        s"""
           |select distinct psGrpgSellFrm.* from PS_DMNSN_grp ps inner join ps_grpg_dmnsn psGrpgSellFrm
           |ON ps.sl_frm_rw_addr_i_1_id = psGrpgSellFrm.rawaddr_id
           |AND coalesce(ps.sl_frm_ky_cd,'unknown') = coalesce(psGrpgSellFrm.rawaddr_ky_cd,'unknown')
           |where psGrpgSellFrm.addr_typ_cd ='Sell From'
           |""".stripMargin)
      // val psGrpgSellFrm_load = psGrpgSellFrm.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psGrpgSellFrm")
      logger.info("psGrpgSellFrm table Loaded")

      val psGrpgShipTo = spark.sql(
        s"""
           |select distinct psGrpgShipTo.* from PS_DMNSN_grp ps inner join ps_grpg_dmnsn psGrpgShipTo
           |ON ps.shp_to_rw_addr_id = psGrpgShipTo.rawaddr_id
           |AND coalesce(ps.shp_to_ky_cd,'unknown') = coalesce(psGrpgShipTo.rawaddr_ky_cd,'unknown')
           |where psGrpgShipTo.addr_typ_cd ='Ship To'
           |""".stripMargin)
      // val psGrpgShipTo_load = psGrpgShipTo.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psGrpgShipTo")
      logger.info("psGrpgShipTo table loaded")

      val psGrpgShipFrm = spark.sql(
        s"""
           |select distinct psGrpgShipFrm.* from PS_DMNSN_grp ps inner join ps_grpg_dmnsn psGrpgShipFrm
           |ON ps.shp_frm_rw_addr_id = psGrpgShipFrm.rawaddr_id
           |AND coalesce(ps.shp_frm_ky_cd,'unknown') = coalesce(psGrpgShipFrm.rawaddr_ky_cd,'unknown')
           |where psGrpgShipFrm.addr_typ_cd ='Ship From'
           |""".stripMargin)
      // val psGrpgShipFrm_load = psGrpgShipFrm.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psGrpgShipFrm")
      logger.info("psGrpgShipFrm Table Loaded")

      val psGrpgSoldTo = spark.sql(
        s"""
           |select distinct psGrpgSoldTo.* from PS_DMNSN_grp ps inner join ps_grpg_dmnsn psGrpgSoldTo
           |ON ps.sld_to_rw_addr_id = psGrpgSoldTo.rawaddr_id
           |AND coalesce(ps.sld_to_ky_cd,'unknown') = coalesce(psGrpgSoldTo.rawaddr_ky_cd,'unknown')
           |where psGrpgSoldTo.addr_typ_cd ='Sold To'
           |""".stripMargin)
      // val psGrpgSoldTo_load = psGrpgSoldTo.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psGrpgSoldTo")
      logger.info("psGrpgSoldTo Loaded")

      val psGrpgEntltd = spark.sql(
        s"""
           |select distinct psGrpgEntltd.* from PS_DMNSN_grp ps inner join ps_grpg_dmnsn psGrpgEntltd
           |ON ps.enttld_rw_addr_id = psGrpgEntltd.rawaddr_id
           |AND coalesce(ps.enttld_ky_cd,'unknown') = coalesce(psGrpgEntltd.rawaddr_ky_cd,'unknown')
           |where psGrpgEntltd.addr_typ_cd ='Entitled'
           |""".stripMargin)
      // val psGrpgEntltd_load = psGrpgEntltd.coalesce(5).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "psGrpgEntltd")
      logger.info("psGrpgEntltd Loaded")

      val chnlptnrTmpRwAddrDf = spark.sql(
        s"""
           |select ps.trsn_i_1_id as trsn_id,
           |psRwAddrBillTo.rw_addr_i_1_id as bll_to_raw_address_id ,
           |psRwAddrBillTo.normalized_lctn_counrty_cd as bll_to_lctn_ctry_cd ,
           |psRwAddrBillTo.lctn_addr_1_cd as bll_to_lctn_addr_1 ,
           |psRwAddrBillTo.lctn_addr_2_cd as bll_to_lctn_addr_2 ,
           |psRwAddrBillTo.lctn_ct_1_nm as bll_to_lctn_cty_nm ,
           |psRwAddrBillTo.lct_1_nm as bll_to_lctn_nm ,
           |psRwAddrBillTo.lctn_zi_1_cd as bll_to_lctn_pstl_cd ,
           |psRwAddrBillTo.lctn_s_1_cd as bll_to_lctn_st_cd ,
           |psRwAddrSellFrm.rw_addr_i_1_id as sell_from_raw_address_id ,
           |psRwAddrSellFrm.normalized_lctn_counrty_cd as sl_frm_lctn_ctry_cd ,
           |psRwAddrSellFrm.lctn_addr_1_cd as sl_frm_lctn_addr_1 ,
           |psRwAddrSellFrm.lctn_addr_2_cd as sl_frm_lctn_addr_2 ,
           |psRwAddrSellFrm.lctn_ct_1_nm as sl_frm_lctn_cty_nm ,
           |psRwAddrSellFrm.lct_1_nm as sl_frm_lctn_nm ,
           |psRwAddrSellFrm.lctn_zi_1_cd as sl_frm_lctn_pstl_cd ,
           |psRwAddrSellFrm.lctn_s_1_cd as sl_frm_lctn_st_cd ,
           |psRwAddrShipTo.rw_addr_i_1_id as ship_to_raw_address_id ,
           |psRwAddrShipTo.normalized_lctn_counrty_cd as shp_to_lctn_ctry_cd ,
           |psRwAddrShipTo.lctn_addr_1_cd as shp_to_lctn_addr_1 ,
           |psRwAddrShipTo.lctn_addr_2_cd as shp_to_lctn_addr_2 ,
           |psRwAddrShipTo.lctn_ct_1_nm as shp_to_lctn_cty_nm ,
           |psRwAddrShipTo.lct_1_nm as shp_to_lctn_nm ,
           |psRwAddrShipTo.lctn_zi_1_cd as shp_to_lctn_pstl_cd ,
           |psRwAddrShipTo.lctn_s_1_cd as shp_to_lctn_st_cd ,
           |psRwAddrShipFrm.rw_addr_i_1_id as ship_from_raw_address_id ,
           |psRwAddrShipFrm.normalized_lctn_counrty_cd as shp_frm_lctn_ctry_cd ,
           |psRwAddrShipFrm.lctn_addr_1_cd as shp_frm_lctn_addr_1 ,
           |psRwAddrShipFrm.lctn_addr_2_cd as shp_frm_lctn_addr_2 ,
           |psRwAddrShipFrm.lctn_ct_1_nm as shp_frm_lctn_cty_nm ,
           |psRwAddrShipFrm.lct_1_nm as shp_frm_lctn_nm ,
           |psRwAddrShipFrm.lctn_zi_1_cd as shp_frm_lctn_pstl_cd ,
           |psRwAddrShipFrm.lctn_s_1_cd as shp_frm_lctn_st_cd ,
           |psRwAddrSoldTo.rw_addr_i_1_id as sold_to_raw_address_id ,
           |psRwAddrSoldTo.normalized_lctn_counrty_cd as sld_to_lctn_ctry_cd ,
           |psRwAddrSoldTo.lctn_addr_1_cd as sld_to_lctn_addr_1 ,
           |psRwAddrSoldTo.lctn_addr_2_cd as sld_to_lctn_addr_2 ,
           |psRwAddrSoldTo.lctn_ct_1_nm as sld_to_lctn_cty_nm ,
           |psRwAddrSoldTo.lct_1_nm as sld_to_lctn_nm ,
           |psRwAddrSoldTo.lctn_zi_1_cd as sld_to_lctn_pstl_cd ,
           |psRwAddrSoldTo.lctn_s_1_cd as sld_to_lctn_st_cd ,
           |psRwAddrEndCust.rw_addr_i_1_id as end_cust_raw_address_id ,
           |psRwAddrEndCust.normalized_lctn_counrty_cd as end_cust_lctn_ctry_cd ,
           |psRwAddrEndCust.lctn_addr_1_cd as end_cust_lctn_addr_1 ,
           |psRwAddrEndCust.lctn_addr_2_cd as end_cust_lctn_addr_2 ,
           |psRwAddrEndCust.lctn_ct_1_nm as end_cust_lctn_cty_nm ,
           |psRwAddrEndCust.lct_1_nm as end_cust_lctn_nm ,
           |psRwAddrEndCust.lctn_zi_1_cd as end_cust_lctn_pstl_cd ,
           |psRwAddrEndCust.lctn_s_1_cd as end_cust_lctn_st_cd ,
           |psRwAddrEntltd.rw_addr_i_1_id as enttld_lctn_raw_address_id ,
           |psRwAddrEntltd.normalized_lctn_counrty_cd as enttld_lctn_ctry_cd ,
           |psRwAddrEntltd.lctn_addr_1_cd as enttld_lctn_addr_1 ,
           |psRwAddrEntltd.lctn_addr_2_cd as enttld_lctn_addr_2 ,
           |psRwAddrEntltd.lctn_ct_1_nm as enttld_lctn_cty_nm ,
           |psRwAddrEntltd.lct_1_nm as enttld_lctn_nm ,
           |psRwAddrEntltd.lctn_zi_1_cd as enttld_lctn_pstl_cd ,
           |psRwAddrEntltd.lctn_s_1_cd as enttld_lctn_st_cd,
           |psRwAddrBillTo.e2open_rwaddr_extrt_nm AS ps_rwaddr_E2Open_extrt_nm,
           |psRwAddrBillTo.e2open_rwaddr_seq_id AS ps_rwaddr_E2Open_seq_ID,
           |psRwAddrBillTo.e2open_rwaddr_extrt_dt AS ps_rwaddr_E2Open_extrt_dt,
           |psRwAddrBillTo.e2open_rwaddr_extrt_ts AS ps_rwaddr_E2Open_extrt_ts,
           |psRwAddrBillTo.ps_rwaddr_eap_load_dt AS ps_rwaddr_eap_load_dt,
           |psRwAddrBillTo.ps_rwaddr_eap_load_ts AS ps_rwaddr_eap_load_ts
           |FROM ea_support_temp_an.ps_ps_prsDF ps
           |LEFT JOIN ea_support_temp_an.psRwAddrBillTo psRwAddrBillTo
           |ON ps.bll_to_rw_addr_id = psRwAddrBillTo.rw_addr_i_1_id
           |LEFT JOIN ea_support_temp_an.psRwAddrSellFrm psRwAddrSellFrm
           |ON ps.sl_frm_rw_addr_i_1_id = psRwAddrSellFrm.rw_addr_i_1_id
           |LEFT JOIN ea_support_temp_an.psRwAddrShipTo psRwAddrShipTo
           |ON ps.shp_to_rw_addr_id = psRwAddrShipTo.rw_addr_i_1_id
           |LEFT JOIN ea_support_temp_an.psRwAddrShipFrm psRwAddrShipFrm
           |ON ps.shp_frm_rw_addr_id = psRwAddrShipFrm.rw_addr_i_1_id
           |LEFT JOIN ea_support_temp_an.psRwAddrSoldTo psRwAddrSoldTo
           |ON ps.sld_to_rw_addr_id = psRwAddrSoldTo.rw_addr_i_1_id
           |LEFT JOIN ea_support_temp_an.psRwAddrEndCust psRwAddrEndCust
           |ON ps.end_usr_rw_addr_id = psRwAddrEndCust.rw_addr_i_1_id
           |LEFT JOIN ea_support_temp_an.psRwAddrEntltd psRwAddrEntltd
           |ON ps.enttld_rw_addr_id = psRwAddrEntltd.rw_addr_i_1_id
           |""".stripMargin)
      //val chnlptnr_tmp_rw_addrDF_load = Utilities.storeDataFrame(chnlptnrTmpRwAddrDf, "overwrite", "ORC", dbName + "." + "chnlptnr_tmp_rw_addr")
      // var chnlptnr_tmp_rw_addrDF_load = chnlptnrTmpRwAddrDf.repartition(20).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "chnlptnr_tmp_rw_addr")
      logger.info("chnlptnr_tmp_rw_addr Loaded")

      val chnlptnrPosFact = spark.sql(
        s"""
           |SELECT /*+ BROADCAST(rptgptnr,skuBmtvol,skuBmt,skuBmtOptCode,skuSrvcProdLn)*/ distinct
           |ps.src_sys_nm,
           |ps.rec_src,
           |ps.backend_deal_1_cleansed_cd as Backend_deal_id_1 ,
           |ps.backend_deal_1_rptd_cd as rptd_Backend_deal_1,
           |ps.backend_deal_2_cleansed_cd AS Backend_deal_id_2 ,
           |ps.backend_deal_2_rptd_cd as rptd_Backend_deal_2 ,
           |ps.backend_deal_3_cleansed_cd AS Backend_deal_id_3 ,
           |ps.backend_deal_3_rptd_cd as rptd_Backend_deal_3 ,
           |ps.backend_deal_4_cleansed_cd AS Backend_deal_id_4 ,
           |ps.backend_deal_4_rptd_cd as rptd_Backend_deal_4 ,
           |ps.bll_to_asian_addr_cd as bll_to_asian_addr ,
           |ps.bll_to_co_tx_id as bll_to_co_tx_id ,
           |ps.bll_to_co_tx_id_enr_id as bll_to_co_tx_id_enr ,
           |ps.bll_to_cntct_nm as bll_to_cntct_nm ,
           |ps.bll_to_ctry_rptd_cd as bll_to_ctry_rptd ,
           |ps.bll_to_ky_cd as bll_to_ky ,
           |ps.bll_to_ph_nr as bll_to_ph_nr ,
           |ps.bll_to_rw_addr_id as bll_to_rw_addr_id ,
           |ps.bll_to_rptd_id AS bll_to_rptd_id ,
           |ps.nmso_nm as NMSO_NAME ,
           |ps.nmso_dt as NMSO_DATE ,
           |ps.stndg_ofr_nr as STANDING_OFFER_NUMBER ,
           |ps.deal_reg_1_cleansed_cd as deal_Reg_1 ,
           |ps.deal_reg_1_rptd_cd as rptd_deal_Reg_1 ,
           |ps.deal_reg_2_cleansed_cd as deal_Reg_2 ,
           |ps.deal_reg_2_rptd_cd as rptd_deal_Reg_2 ,
           |COALESCE(ps.nt_cst_aftr_rbt_lcy_ex_1_cd,0) as nt_cst_aftr_rbt_LCY ,
           |COALESCE(ps.ptnr_prch_prc_lcy_un_1_cd,0) as ptnr_prch_LCY_unt_prc ,
           |COALESCE(ps.ptnr_sl_prc_lcy_un_1_cd,0) as ptnr_sl_LCY_unt_prc ,
           |COALESCE(ps.ptnr_sl_prc_usd_un_1_cd,0) as ptnr_sl_usd_unt_prc ,
           |COALESCE(ps.bndl_qt_1_cd,0) as sls_bndl_qty,
           |COALESCE(ps.sale_qty_cd,0) as sls_qty,
           |ps.iud_fl_1_cd as POS_IUD_FLAG,
           |ps.rsrv_fld_1_cd as POS_RESERVE_FIELD_1,
           |ps.rsrv_fld_2_cd as POS_RESERVE_FIELD_2,
           |ps.rptg_prd_strt_dt AS rptg_prd_strt_dt,
           |ps.rptg_prd_end_dt AS rptg_prd_end_dt,
           |ps.rsrv_fld_5_cd as POS_RESERVE_FIELD_5,
           |ps.rsrv_fld_6_cd as POS_RESERVE_FIELD_6,
           |ps.rsrv_fld_7_cd as POS_RESERVE_FIELD_7,
           |ps.rsrv_fld_8_cd as POS_RESERVE_FIELD_8,
           |ps.rsrv_fld_9_cd as POS_RESERVE_FIELD_9,
           |ps.rsrv_fld_10_cd as POS_RESERVE_FIELD_10,
           |ps.bndl_prod_i_1_id as bndl_prod_id ,
           |ps.prod_ln_i_1_id as orgl_prod_ln_id ,
           |ps.ptnr_bndl_prod_i_1_id as ptnr_bndl_prod_id ,
           |ps.ptnr_inrn_sk_1_cd as ptnr_inrn_prod_nr ,
           |ps.hpe_prod_i_1_id AS prod_nr,
           |ps.rptd_prod_i_1_id as rptd_prod_nr ,
           |ps.vl_vol_fl_1_cd as vl_vol_flg ,
           |ps.reporter_i_2_id as reporter_id ,
           |ps.sl_frm_cntc_1_nm as sl_frm_cntct_nm ,
           |ps.sl_frm_ctry_rptd_cd as sl_frm_ctry_rptd ,
           |ps.sl_frm_ky_cd as sl_frm_ky ,
           |ps.sl_frm_p_1_nr as sl_frm_ph_nr ,
           |ps.sl_frm_rw_addr_i_1_id as sl_frm_rw_addr_id ,
           |ps.sl_frm_rptd_id as sl_frm_rptd_id ,
           |ps.shp_frm_cntct_nm as shp_frm_cntct_nm ,
           |ps.shp_frm_ctry_rptd_cd as shp_frm_ctry_rptd ,
           |ps.shp_frm_ky_cd as shp_frm_ky ,
           |ps.shp_frm_ph_nr as shp_frm_ph_nr ,
           |ps.shp_frm_rw_addr_id as shp_frm_rw_addr_id ,
           |ps.shp_frm_rptd_id as shp_frm_rptd_id ,
           |ps.shp_to_asian_addr_cd as shp_to_asian_addr ,
           |ps.shp_to_co_tx_id as shp_to_co_tx_id ,
           |ps.shp_to_co_tx_id_enr_id as shp_to_co_tx_id_enr ,
           |ps.shp_to_cntct_nm as shp_to_cntct_nm ,
           |ps.shp_to_ctry_rptd_cd as shp_to_ctry_rptd ,
           |ps.shp_to_ky_cd as shp_to_ky ,
           |ps.shp_to_ph_nr as shp_to_ph_nr ,
           |ps.shp_to_rw_addr_id as shp_to_rw_addr_id ,
           |ps.shipto_rptd_id as shp_to_rptd_id ,
           |ps.sld_to_asian_addr_cd as sld_to_asian_addr ,
           |ps.sld_to_co_tx_id as sld_to_co_tx_id ,
           |ps.sld_to_co_tx_id_enr_id as sld_to_co_tx_id_enr ,
           |ps.sld_to_cntct_nm as sld_to_cntct_nm ,
           |ps.sld_to_ctry_rptd_cd as sld_to_ctry_rptd ,
           |ps.sld_to_ky_cd as sld_to_ky ,
           |ps.sld_to_ph_nr as sld_to_ph_nr ,
           |ps.sld_to_rw_addr_id as sld_to_rw_addr_id ,
           |ps.sld_to_rptd_id as sld_to_rptd_id ,
           |ps.end_usr_addr_src_rptd_cd AS end_usr_addr_src_rptd ,
           |ps.end_usr_asian_addr_cd as end_usr_asian_addr ,
           |ps.end_usr_co_tx_id as end_usr_co_tx_id ,
           |ps.end_usr_co_tx_id_enr_id as end_usr_co_tx_id_enr ,
           |ps.end_usr_cntct_nm as end_usr_cntct_nm ,
           |ps.end_usr_ctry_rptd_cd as end_usr_ctry_rptd ,
           |ps.end_usr_ky_cd as end_usr_ky ,
           |ps.end_usr_ph_nr as end_usr_ph_nr ,
           |ps.end_usr_rw_addr_id as end_usr_rw_addr_id ,
           |ps.end_usr_rptd_id as end_usr_rptd_id ,
           |ps.enttld_asian_addr_cd as enttld_asian_addr ,
           |ps.enttld_co_tx_id as enttld_co_tx_id ,
           |ps.enttld_co_tx_id_enr_id as enttld_co_tx_id_enr ,
           |ps.enttld_cntct_nm as enttld_cntct_nm ,
           |ps.enttld_ctry_rptd_cd as enttld_ctry_rptd ,
           |ps.enttld_ky_cd as enttld_ky ,
           |ps.enttld_ph_nr as enttld_ph_nr ,
           |ps.enttld_rw_addr_id as enttld_rw_addr_id ,
           |ps.enttld_rptd_id as enttld_rptd_id ,
           |ps.agt_flg_cd as agt_flg ,
           |ps.brm_err_flg_cd AS brm_err_flg ,
           |ps.crss_shp_flg_cd as crss_shp_flg ,
           |ps.cust_to_ptnr_po_nr as cust_to_ptnr_po_nr ,
           |ps.trs_1_dt AS dt ,
           |ps.drp_shp_flg_cd as drp_shp_flg ,
           |ps.fl_i_1_id as E2Open_fl_id ,
           |ps.fl_rcv_1_dt as E2Open_fl_rcvd_dt ,
           |ps.hpe_inv_nr as HPE_inv_nr ,
           |ps.nt_cst_aftr_rbt_curr_cd as nt_cst_aftr_rbt_curr_cd ,
           |ps.org_sls_nm as Org_sls_nm ,
           |ps.orgl_hpe_asngd_trsn_id AS orgl_HPE_asngd_trsn_id ,
           |ps.orgl_trsn_i_1_id as orgl_trsn_id ,
           |ps.prnt_trsn_i_1_id as prnt_trsn_id ,
           |ps.ptnr_inrn_trsn_id as ptnr_inrn_trsn_id ,
           |ps.inv_dt as ptnr_inv_dt ,
           |ps.ptnr_inv_nr AS ptnr_inv_nr,
           |ps.ptnr_prch_curr_cd as ptnr_prch_curr_cd ,
           |ps.ptnr_sls_rep_nm as ptnr_sls_rep_nm ,
           |ps.ptnr_sl_prc_curr_cd as ptnr_sl_prc_curr_cd ,
           |ps.sm_dt as ptnr_sm_dt,
           |ps.ptnr_to_hpe_po_nr as ptnr_to_HPE_po_nr ,
           |ps.prv_trsn_i_1_id as prv_trsn_id ,
           |ps.prod_orgn_cd as prod_orgn_cd ,
           |ps.prchg_src_ws_ind_cd as prchg_src_WS_ind ,
           |ps.splyr_iso_ctry_cd as splyr_iso_ctry_cd ,
           |ps.tty_mgr_cd as tty_mgr_cd ,
           |ps.trsn_i_1_id AS trsn_id,
           |ps.vldtn_wrnn_1_cd as vldtn_wrnng_cd ,
           |ps.upfrnt_deal_1_rptd_cd as rptd_upfrnt_deal_1 ,
           |ps.upfrnt_deal_1_cleansed_cd AS upfrnt_deal_id_1_id ,
           |ps.upfrnt_deal_2_rptd_cd as rptd_upfrnt_deal_2 ,
           |ps.upfrnt_deal_2_cleansed_cd as upfrnt_deal_id_2 ,
           |ps.upfrnt_deal_3_rptd_cd as rptd_upfrnt_deal_3 ,
           |ps.upfrnt_deal_3_cleansed_cd as upfrnt_deal_id_3_id ,
           |OPEPRS.ope_bmt_dstr_br_typ as OPE_bmt_dstr_bsn_rshp_typ ,
           |OPEPRS.ope_bmt_dstr_prty_id as OPE_bmt_dstr_prty_id ,
           |OPEPRS.ope_bmt_dstr_sls_comp_rlvnt as OPE_bmt_dstr_sls_Comp_rlvnt ,
           |OPEPRS.ope_dstr_br_typ as OPE_dstr_bsn_rshp_typ ,
           |OPEPRS.ope_dstr_prty_id as OPE_dstr_prty_id ,
           |OPEPRS.ope_dstr_rsn_cd as OPE_dstr_rsn_cd ,
           |OPEPRS.ope_bmt_end_cust_br_typ as OPE_bmt_end_cust_bsn_rshp_typ ,
           |OPEPRS.ope_bmt_end_cust_prty_id as OPE_bmt_end_cust_prty_id ,
           |OPEPRS.ope_bmt_end_cust_sls_comp_rlvnt as OPE_bmt_end_cust_sls_Comp_rlvnt ,
           |OPEPRS.ope_end_cust_br_typ as OPE_deal_end_cust_bsn_rshp_typ ,
           |OPEPRS.ope_deal_end_cust_prty_id as OPE_deal_end_cust_prty_id ,
           |OPEPRS.ope_end_cust_br_typ as OPE_end_cust_bsn_rshp_typ ,
           |OPEPRS.ope_end_cust_prty_id as OPE_end_cust_prty_id ,
           |OPEPRS.ope_end_cust_rsn_cd as OPE_end_cust_rsn_cd ,
           |OPEPRS.ope_lvl_3_br_typ as OPE_lvl_3_bsn_rshp_typ ,
           |OPEPRS.ope_lvl_3_prty_id as OPE_lvl_3_prty_id ,
           |OPEPRS.ope_lvl_3_rshp as OPE_lvl_3_rshp ,
           |OPEPRS.ope_enttld_prty_ctry as OPE_src_enttld_prty_ctry ,
           |OPEPRS.ope_enttld_prty_id as OPE_src_enttld_prty_prty_id ,
           |OPEPRS.ope_src_end_cust_ctry as OPE_src_end_cust_ctry ,
           |OPEPRS.ope_src_end_cust_prty_id as OPE_src_end_cust_prty_id ,
           |OPEPRS.ope_enttld_prty_ctry as OPE_enttld_prty_ctry ,
           |OPEPRS.ope_enttld_prty_id as OPE_Entitlted_prty_id ,
           |OPEPRS.dbm_rptg_dstr_ope as OPE_dstr_bsn_mgr_rptg_dstr ,
           |OPEPRS.dstr_rptg_dstr_ope as OPE_dstr_rptg_dstr ,
           |OPEPRS.end_cust_sls_rep_rptg_dstr_ope as OPE_end_cust_sls_rep_rptg_dstr ,
           |OPEPRS.flipped_dbm_vw_flg_ope as OPE_Flipped_dstr_bsn_mgr_vw_flg ,
           |OPEPRS.flipped_dstr_vw_flg_ope as OPE_Flipped_dstr_vw_flg ,
           |OPEPRS.flipped_end_cust_vw_flg_ope as OPE_Flipped_end_cust_vw_flg ,
           |OPEPRS.flipped_pbm_vw_flg_ope as OPE_Flipped_ptnr_bsn_mgr_vw_flg ,
           |OPEPRS.flipped_rslr_vw_flg_ope as OPE_Flipped_rslr_vw_flg ,
           |OPEPRS.ope_lvl_1_br_typ as OPE_lvl_1_bsn_rshp_typ ,
           |OPEPRS.ope_lvl_1_prty_id as OPE_lvl_1_prty_id ,
           |OPEPRS.ope_lvl_1_rshp as OPE_lvl_1_rshp ,
           |OPEPRS.pbm_rptg_dstr_ope as OPE_ptnr_bsn_mgr_rptg_dstr ,
           |OPEPRS.rslr_rptg_dstr_ope as OPE_rslr_rptg_dstr ,
           |OPEPRS.ope_bmt_rslr_br_typ as OPE_bmt_rslr_bsn_rshp_typ ,
           |OPEPRS.ope_bmt_rslr_prty_id as OPE_bmt_rslr_prty_id ,
           |OPEPRS.ope_bmt_dstr_sls_comp_rlvnt as OPE_bmt_rslr_sls_Comp_rlvnt ,
           |OPEPRS.ope_rslr_br_typ as OPE_rslr_bsn_rshp_typ ,
           |OPEPRS.ope_rslr_prty_id as OPE_rslr_prty_id ,
           |OPEPRS.ope_rslr_rsn_cd as OPE_rslr_rsn_cd ,
           |OPEPRS.ope_lvl_2_br_typ as OPE_lvl_2_bsn_rshp_typ ,
           |OPEPRS.ope_lvl_2_prty_id as OPE_lvl_2_prty_id ,
           |OPEPRS.ope_lvl_2_rshp as OPE_lvl_2_rshp ,
           |OPEPRS.ope_sldt_br_typ as OPE_sld_to_BR_typ ,
           |OPEPRS.ope_sldt_ctry as OPE_sld_to_ctry ,
           |OPEPRS.ope_sldt_prty_id as OPE_sld_to_prty_id ,
           |OPEPRS.ope_big_deal_xclsn as OPE_big_deal_xclsn ,
           |OPEPRS.ope_crss_brdr as OPE_crss_brdr ,
           |OPEPRS.ope_crss_srcd as OPE_crss_srcd ,
           |OPEPRS.dstr_rptg_dstr_to_rptg_dstr_ope as OPE_dstr_rptg_dstr_to_another_rptg_dstr ,
           |OPEPRS.ope_dstr_comp_vw as OPE_dstr_Bookings_Comp_vw ,
           |OPEPRS.ope_dstr_bookings_vw as OPE_dstr_Bookings_vw ,
           |OPEPRS.ope_dbm_bookings_vw as OPE_dstr_bsn_mgr_Bookings_vw ,
           |OPEPRS.ope_dbm_comp_vw as OPE_dstr_bsn_mgr_Comp_vw ,
           |OPEPRS.ope_drp_shp as OPE_drp_shp ,
           |OPEPRS.emb_srv_ln_itms_ope as OPE_emb_srv_ln_itms ,
           |OPEPRS.end_cust_sls_rep_rptg_dstr_to_rptg_dstr_ope as OPE_end_cust_rptg_dstr_to_another_rptg_dstr ,
           |OPEPRS.ope_end_cust_sls_rep_bookings_vw as OPE_end_cust_sls_rep_Bookings_vw ,
           |OPEPRS.end_cust_sls_rep_rptg_dstr_ope as OPE_end_cust_sls_rep_comp_vw ,
           |OPEPRS.ope_fdrl_bsn as OPE_fdrl_bsn ,
           |OPEPRS.ope_pbm_bookings_vw as OPE_ptnr_bsn_mgr_Bookings_vw ,
           |OPEPRS.ope_pbm_comp_vw as OPE_ptnr_bsn_mgr_Comp_vw ,
           |OPEPRS.pft_cntr_cd_ope as OPE_pft_cntr ,
           |OPEPRS.ope_rslr_bookings_vw as OPE_rslr_Bookings_vw ,
           |OPEPRS.ope_rslr_comp_vw as OPE_rslr_Comp_vw ,
           |OPEPRS.srv_ln_itm_ope as OPE_srv_ln_itm ,
           |OPEPRS.trsn_i_1_id as OPE_trsn_id ,
           |OPEPRS.reporter_emdm_prty_id_ope as ope_reporter_prty_id ,
           |OPEPRS.reporter_new_br_typ_ope as ope_br_ty_1_cd ,
           |crc32(trim(rptgptnr.reporter_i_1_id)) as rptr_prty_ky,
           |rw_ad.bll_to_raw_address_id,
           |rw_ad.bll_to_lctn_ctry_cd AS bll_to_lctn_ctry_cd,
           |rw_ad.bll_to_lctn_addr_1,
           |rw_ad.bll_to_lctn_addr_2,
           |rw_ad.bll_to_lctn_cty_nm AS bll_to_lctn_cty_nm,
           |rw_ad.bll_to_lctn_nm AS bll_to_lctn_nm,
           |rw_ad.bll_to_lctn_pstl_cd,
           |rw_ad.bll_to_lctn_st_cd AS bll_to_lctn_st_cd,
           |rw_ad.sell_from_raw_address_id,
           |rw_ad.sl_frm_lctn_ctry_cd,
           |rw_ad.sl_frm_lctn_addr_1,
           |rw_ad.sl_frm_lctn_addr_2,
           |rw_ad.sl_frm_lctn_cty_nm,
           |rw_ad.sl_frm_lctn_nm,
           |rw_ad.sl_frm_lctn_pstl_cd,
           |rw_ad.sl_frm_lctn_st_cd,
           |rw_ad.ship_to_raw_address_id,
           |rw_ad.shp_to_lctn_ctry_cd AS shp_to_lctn_ctry_cd,
           |rw_ad.shp_to_lctn_addr_1,
           |rw_ad.shp_to_lctn_addr_2,
           |rw_ad.shp_to_lctn_cty_nm AS shp_to_lctn_cty_nm,
           |rw_ad.shp_to_lctn_nm AS shp_to_lctn_nm,
           |rw_ad.shp_to_lctn_pstl_cd,
           |rw_ad.shp_to_lctn_st_cd AS shp_to_lctn_st_cd,
           |rw_ad.ship_from_raw_address_id,
           |rw_ad.shp_frm_lctn_ctry_cd,
           |rw_ad.shp_frm_lctn_addr_1,
           |rw_ad.shp_frm_lctn_addr_2,
           |rw_ad.shp_frm_lctn_cty_nm,
           |rw_ad.shp_frm_lctn_nm,
           |rw_ad.shp_frm_lctn_pstl_cd,
           |rw_ad.shp_frm_lctn_st_cd,
           |rw_ad.sold_to_raw_address_id,
           |rw_ad.sld_to_lctn_ctry_cd,
           |rw_ad.sld_to_lctn_addr_1,
           |rw_ad.sld_to_lctn_addr_2,
           |rw_ad.sld_to_lctn_cty_nm,
           |rw_ad.sld_to_lctn_nm,
           |rw_ad.sld_to_lctn_pstl_cd,
           |rw_ad.sld_to_lctn_st_cd,
           |rw_ad.end_cust_raw_address_id,
           |rw_ad.end_cust_lctn_ctry_cd AS end_cust_lctn_ctry_cd,
           |rw_ad.end_cust_lctn_addr_1,
           |rw_ad.end_cust_lctn_addr_2,
           |rw_ad.end_cust_lctn_cty_nm AS end_cust_lctn_cty_nm,
           |rw_ad.end_cust_lctn_nm AS end_cust_lctn_nm,
           |rw_ad.end_cust_lctn_pstl_cd,
           |rw_ad.end_cust_lctn_st_cd AS end_cust_lctn_st_cd,
           |rw_ad.enttld_lctn_raw_address_id,
           |rw_ad.enttld_lctn_ctry_cd,
           |rw_ad.enttld_lctn_addr_1,
           |rw_ad.enttld_lctn_addr_2,
           |rw_ad.enttld_lctn_cty_nm,
           |rw_ad.enttld_lctn_nm,
           |rw_ad.enttld_lctn_pstl_cd,
           |rw_ad.enttld_lctn_st_cd,
           |ps.backend_deal_1_ln_itm_cd as Backend_deal_1_ln_itm ,
           |ps.backend_deal_1_m_cd as Backend_deal_1_MC_cd ,
           |ps.backend_deal_1_vrsn_cd as Backend_deal_1_vrsn ,
           |ps.backend_deal_2_ln_itm_cd as backend_deal_2_ln_itm_cd ,
           |ps.backend_deal_2_m_cd as backend_deal_2_m_cd ,
           |ps.backend_deal_2_vrsn_cd as Backend_deal_2_vrsn ,
           |ps.backend_deal_3_ln_itm_cd as Backend_deal_3_ln_itm ,
           |ps.backend_deal_3_m_cd as Backend_deal_3_MC_cd ,
           |ps.backend_deal_3_vrsn_cd as Backend_deal_3_vrsn ,
           |ps.backend_deal_4_ln_itm_cd as Backend_deal_4_ln_itm ,
           |ps.backend_deal_4_m_cd as Backend_deal_4_MC_cd ,
           |ps.backend_deal_4_vrsn_cd as backend_deal_4_vrsn ,
           |ps.ndp_coefficien_1_cd as NDP_COEFFICIENT ,
           |COALESCE(ps.ndp_estmd_lcy_ext_cd,0) as NDP_ESTIMATED_LCY_EXT ,
           |COALESCE(ps.ndp_estmd_lcy_unt_cd,0) as NDP_ESTIMATED_LCY_UNIT ,
           |COALESCE(ps.ndp_estmd_usd_ext_cd,0) as NDP_ESTIMATED_USD_EXT ,
           |COALESCE(ps.ndp_estmd_usd_unt_cd,0) as NDP_ESTIMATED_USD_UNIT ,
           |ps.hpe_ord_id as HPE_ORDER ,
           |ps.hpe_ord_itm_mcc_cd as HPE_ORDER_ITEM_MCC ,
           |ps.hpe_ord_itm_nr as HPE_ORDER_ITEM_NUMBER ,
           |ps.hpe_ord_dt as HPE_ORDER_DATE ,
           |COALESCE(ps.hpe_ord_itm_disc_lcy_ext_cd,0) as HPE_ORDER_ITEM_DISC_LCY_EXT ,
           |COALESCE(ps.hpe_ord_itm_disc_usd_ext_cd,0) as HPE_ORDER_ITEM_DISC_USD_EXT ,
           |ps.hpe_upfrnt_deal_id as HPE_UPFRONT_DEAL ,
           |ps.hpe_ord_itm_cleansed_deal_id as HPE_ORDER_ITEM_CLEANSED_DEAL ,
           |ps.hpe_upfrnt_deal_typ_cd as HPE_UPFRONT_DEAL_TYPE ,
           |ps.hpe_ord_itm_orig_deal_id as HPE_ORDER_ITEM_ORIG_DEAL ,
           |COALESCE(ps.hpe_upfrnt_disc_lcy_ext_cd,0) as HPE_UPFRONT_DISC_LCY_EXT ,
           |COALESCE(ps.hpe_upfrnt_disc_usd_ext_cd,0) as HPE_UPFRONT_DISC_USD_EXT ,
           |ps.hpe_ord_end_usr_addr_id as HPE_ORDER_END_USER_ADDR ,
           |ps.hpe_upfrnt_deal_end_usr_addr_id as HPE_UPFRONT_DEAL_END_USER_ADDR ,
           |ps.upfrnt_deal_1_typ_cd as UPFRONT_DEAL_1_TYPE ,
           |ps.upfrnt_deal_1_end_usr_addr_id as UPFRONT_DEAL_1_END_USER_ADDR ,
           |ps.backend_deal_1_typ_cd as BACKEND_DEAL_1_TYPE ,
           |ps.backend_deal_1_end_usr_addr_id as BACKEND_DEAL_1_END_USER_ADDR ,
           |ps.ptnr_prch_prc_lcy_ex_1_cd as PARTNER_PURCHASE_PRICE_LCY_EXT ,
           |COALESCE(ps.ptnr_prch_prc_lcy_un_2_cd,0) as PARTNER_PURCHASE_PRICE_LCY_UNIT ,
           |ps.ptnr_prch_prc_usd_ex_1_cd as PARTNER_PURCHASE_PRICE_USD_EXT ,
           |COALESCE(ps.ptnr_prch_prc_usd_un_1_cd,0) as PARTNER_PURCHASE_PRICE_USD_UNIT ,
           |ps.nt_cst_aftr_rbt_lcy_ex_2_cd as NET_COST_AFTER_REBATE_LCY_EXT ,
           |ps.nt_cst_aftr_rbt_usd_ext_cd as NET_COST_AFTER_REBATE_USD_EXT ,
           |ps.nt_cst_curr_cd as NET_COST_CURRENCY_CODE ,
           |ps.rbt_amt_lcy_ext_cd as REBATE_AMOUNT_LCY_EXT ,
           |COALESCE(ps.rbt_amt_lcy_unt_cd,0) as REBATE_AMOUNT_LCY_UNIT ,
           |ps.rbt_amt_usd_ext_cd as REBATE_AMOUNT_USD_EXT ,
           |COALESCE(ps.rbt_amt_usd_unt_cd,0) as REBATE_AMOUNT_USD_UNIT ,
           |ps.ptnr_sl_prc_lcy_ext_cd as PARTNER_SELL_PRICE_LCY_EXT,
           |COALESCE(ps.ps_prs_ptnr_sl_prc_lcy_un_1_cd,0) as PARTNER_SELL_PRICE_LCY_UNIT ,
           |ps.ptnr_sl_prc_usd_ext_cd as PARTNER_SELL_PRICE_USD_EXT ,
           |COALESCE(ps.ptnr_sl_prc_usd_un_2_cd,0) as PARTNER_SELL_PRICE_USD_UNIT ,
           |ps.backend_deal_1_dscnt_lcy_ext_cd as Backend_deal_1_dscnt_LCY_EXT ,
           |COALESCE(ps.backend_deal_1_dscnt_lcy_unt_cd,0) as Backend_deal_1_dscnt_LCY_pr_unt ,
           |ps.backend_deal_1_dscnt_usd_ext_cd as Backend_deal_1_dscnt_usd_EXT ,
           |COALESCE(ps.backend_deal_1_dscnt_usd_unt_cd,0) as Backend_deal_1_dscnt_usd_pr_unt ,
           |COALESCE(ps.backend_deal_1_prc_lcy_unt_cd,0) as Backend_deal_1_LCY_unt_prc ,
           |COALESCE(ps.backend_deal_1_prc_usd_unt_cd,0) as Backend_deal_1_usd_unt_prc ,
           |ps.backend_deal_2_dscnt_lcy_ext_cd as Backend_deal_2_dscnt_LCY_EXT ,
           |COALESCE(ps.backend_deal_2_dscnt_lcy_unt_cd,0) as Backend_deal_2_dscnt_LCY_pr_unt ,
           |ps.backend_deal_2_dscnt_usd_ext_cd as Backend_deal_2_dscnt_usd_EXT ,
           |COALESCE(ps.backend_deal_2_dscnt_usd_unt_cd,0) as Backend_deal_2_dscnt_usd_pr_unt ,
           |COALESCE(ps.backend_deal_2_prc_lcy_unt_cd,0) as Backend_deal_2_LCY_unt_prc ,
           |COALESCE(ps.backend_deal_2_prc_usd_unt_cd,0) as Backend_deal_2_usd_unt_prc ,
           |ps.backend_deal_3_dscnt_lcy_ext_cd as Backend_deal_3_dscnt_LCY_EXT ,
           |COALESCE(ps.backend_deal_3_dscnt_lcy_unt_cd,0) as Backend_deal_3_dscnt_LCY_pr_unt ,
           |ps.backend_deal_3_dscnt_usd_ext_cd as Backend_deal_3_dscnt_usd_EXT ,
           |COALESCE(ps.backend_deal_3_dscnt_usd_unt_cd,0) as Backend_deal_3_dscnt_usd_pr_unt ,
           |COALESCE(ps.backend_deal_3_prc_lcy_unt_cd,0) as Backend_deal_3_LCY_unt_prc ,
           |COALESCE(ps.backend_deal_3_prc_usd_unt_cd,0) as Backend_deal_3_usd_unt_prc ,
           |ps.backend_deal_4_dscnt_lcy_ext_cd as Backend_deal_4_dscnt_LCY_EXT ,
           |COALESCE(ps.backend_deal_4_dscnt_lcy_unt_cd,0) as Backend_deal_4_dscnt_LCY_pr_unt ,
           |ps.backend_deal_4_dscnt_usd_ext_cd as Backend_deal_4_dscnt_usd_EXT ,
           |COALESCE(ps.backend_deal_4_dscnt_usd_unt_cd,0) as Backend_deal_4_dscnt_usd_pr_unt ,
           |COALESCE(ps.backend_deal_4_prc_lcy_unt_cd,0) as Backend_deal_4_LCY_unt_prc ,
           |COALESCE(ps.backend_deal_4_prc_usd_unt_cd,0) as Backend_deal_4_usd_unt_prc ,
           |COALESCE(ps.backend_rbt_1_lcy_unt_cd,0) as Backend_rbt_1_LCY_pr_unt ,
           |COALESCE(ps.backend_rbt_1_usd_unt_cd,0) as Backend_rbt_1_usd_pr_unt ,
           |COALESCE(ps.backend_rbt_2_lcy_unt_cd,0) as Backend_rbt_2_LCY_pr_unt ,
           |COALESCE(ps.backend_rbt_2_usd_unt_cd,0) as Backend_rbt_2_usd_pr_unt ,
           |COALESCE(ps.backend_rbt_3_lcy_unt_cd,0) as Backend_rbt_3_LCY_pr_unt ,
           |COALESCE(ps.backend_rbt_3_usd_unt_cd,0) as Backend_rbt_3_usd_pr_unt ,
           |COALESCE(ps.backend_rbt_4_lcy_unt_cd,0) as Backend_rbt_4_LCY_pr_unt ,
           |COALESCE(ps.backend_rbt_4_usd_unt_cd,0) as Backend_rbt_4_usd_pr_unt ,
           |COALESCE(ps.pa_dscnt_lcy_un_1_cd,0) as prch_agrmnt_dscnt_LCY_unt ,
           |COALESCE(ps.pa_dscnt_usd_un_1_cd,0) as prch_agrmnt_dscnt_usd_unt ,
           |COALESCE(ps.asp_lcy_ex_1_cd,0) AS sls_ASP_LCY,
           |ps.asp_lcy_un_1_cd AS sls_ASP_LCY_unt_prc ,
           |ps.asp_usd_ex_1_cd AS sls_ASP_usd ,
           |ps.asp_usd_un_1_cd AS sls_ASP_usd_unt_prc ,
           |COALESCE(ps.AVRG_deal_nt_LCY_EXT_nm,0) AS sls_AVRG_deal_nt_LCY ,
           |COALESCE(ps.AVRG_deal_nt_LCY_unt_nm,0) AS sls_AVRG_deal_nt_LCY_unt_prc ,
           |COALESCE(ps.AVRG_deal_nt_usd_EXT_nm,0) AS sls_AVRG_deal_nt_usd ,
           |COALESCE(ps.AVRG_deal_nt_usd_unt_nm,0) AS sls_AVRG_deal_nt_usd_unt_prc ,
           |COALESCE(ps.AVRG_deal_nt_VCY_EXT_nm,0) AS sls_AVRG_deal_nt_VCY ,
           |COALESCE(ps.AVRG_deal_nt_VCY_unt_nm,0) AS sls_AVRG_deal_nt_VCY_unt_prc ,
           |COALESCE(ps.deal_nt_lcy_ext_cd,0) AS sls_deal_nt_LCY ,
           |ps.deal_nt_lcy_un_1_cd  AS sls_deal_nt_LCY_unt_prc,
           |COALESCE(ps.deal_nt_usd_ext_cd,0) AS sls_deal_nt_usd,
           |ps.deal_nt_usd_un_1_cd AS sls_deal_nt_usd_unt_prc ,
           |COALESCE(ps.deal_nt_VCY_EXT_nm,0) as sls_deal_nt_VCY,
           |COALESCE(ps.deal_nt_VCY_unt_nm,0) as sls_deal_nt_VCY_unt_prc,
           |COALESCE(ps.lst_prc_lcy_ex_1_cd,0) AS sls_lst_LCY ,
           |COALESCE(ps.lst_prc_lcy_un_1_cd,0) AS sls_lst_LCY_unt_prc,
           |COALESCE(ps.lst_prc_usd_ex_1_cd,0) AS sls_lst_usd,
           |ps.lst_prc_usd_un_1_cd AS sls_lst_usd_unt_prc,
           |COALESCE(ps.lst_prc_VCY_EXT_nm,0) as sls_lst_VCY ,
           |COALESCE(ps.lst_prc_VCY_unt_nm,0) as sls_lst_VCY_unt_prc ,
           |COALESCE(ps.ndp_lcy_ex_1_cd,0) AS sls_NDP_LCY ,
           |ps.ndp_lcy_un_1_cd AS sls_NDP_LCY_unt_prc ,
           |COALESCE(ps.ndp_usd_ex_1_cd,0) AS sls_NDP_usd ,
           |ps.ndp_usd_un_1_cd AS sls_NDP_usd_unt_prc ,
           |COALESCE(ps.NDP_VCY_EXT_nm,0) as sls_NDP_VCY ,
           |COALESCE(ps.NDP_VCY_unt_nm,0) as sls_NDP_VCY_unt_prc ,
           |ps.upfrnt_deal_1_dscnt_lcy_ex_1_cd as upfrnt_deal_1_dscnt_LCY ,
           |COALESCE(ps.upfrnt_deal_1_dscnt_lcy_un_1_cd,0) as upfrnt_deal_1_dscnt_LCY_pr_unt ,
           |COALESCE(ps.upfrnt_deal_1_dscnt_usd_ex_1_cd) as upfrnt_deal_1_dscnt_usd ,
           |COALESCE(ps.upfrnt_deal_1_dscnt_usd_un_1_cd,0) as upfrnt_deal_1_dscnt_usd_pr_unt ,
           |COALESCE(ps.upfrnt_deal_1_prc_lcy_un_1_cd,0) as upfrnt_deal_1_LCY_unt_prc ,
           |COALESCE(ps.upfrnt_deal_1_prc_usd_un_1_cd,0) as upfrnt_deal_1_usd_unt_prc ,
           |ps.upfrnt_deal_2_dscnt_lcy_ext_cd as upfrnt_deal_2_dscnt_LCY ,
           |COALESCE(ps.upfrnt_deal_2_dscnt_lcy_unt_cd,0) as upfrnt_deal_2_dscnt_LCY_pr_unt ,
           |ps.upfrnt_deal_2_dscnt_usd_ext_cd as upfrnt_deal_2_dscnt_usd ,
           |COALESCE(ps.upfrnt_deal_2_dscnt_usd_unt_cd,0) as upfrnt_deal_2_dscnt_usd_pr_unt ,
           |COALESCE(ps.upfrnt_deal_2_prc_lcy_unt_cd,0) as upfrnt_deal_2_LCY_unt_prc ,
           |COALESCE(ps.upfrnt_deal_2_prc_usd_unt_cd,0) as upfrnt_deal_2_usd_unt_prc ,
           |ps.upfrnt_deal_3_dscnt_lcy_ext_cd as upfrnt_deal_3_dscnt_LCY_EXT ,
           |COALESCE(ps.upfrnt_deal_3_dscnt_lcy_unt_cd,0) as upfrnt_deal_3_dscnt_LCY_pr_unt ,
           |ps.upfrnt_deal_3_dscnt_usd_ext_cd as upfrnt_deal_3_dscnt_usd_EXT ,
           |COALESCE(ps.upfrnt_deal_3_dscnt_usd_unt_cd,0) as upfrnt_deal_3_dscnt_usd_pr_unt ,
           |COALESCE(ps.upfrnt_deal_3_prc_lcy_unt_cd,0) as upfrnt_deal_3_LCY_unt_prc ,
           |COALESCE(ps.upfrnt_deal_3_prc_usd_unt_cd,0) as upfrnt_deal_3_usd_unt_prc ,
           |ps.iud_fl_8_cd as POS_PRS_IUD_FLAG ,
           |ps.rsrv_fld_1_nm as POS_PRS_RESERVE_FIELD_1 ,
           |ps.rsrv_fld_2_nm as POS_PRS_RESERVE_FIELD_2 ,
           |ps.rsrv_fld_3_nm as POS_PRS_RESERVE_FIELD_3 ,
           |ps.rsrv_fld_4_nm as POS_PRS_RESERVE_FIELD_4 ,
           |ps.rsrv_fld_5_nm as POS_PRS_RESERVE_FIELD_5 ,
           |ps.rsrv_fld_6_nm as POS_PRS_RESERVE_FIELD_6 ,
           |ps.rsrv_fld_7_nm as POS_PRS_RESERVE_FIELD_7 ,
           |ps.rsrv_fld_8_nm as POS_PRS_RESERVE_FIELD_8 ,
           |ps.rsrv_fld_9_nm as POS_PRS_RESERVE_FIELD_9 ,
           |ps.rsrv_fld_10_nm as POS_PRS_RESERVE_FIELD_10 ,
           |ps.upfrnt_deal_1_ln_it_1_cd as upfrnt_deal_1_ln_itm ,
           |ps.upfrnt_deal_1__1_cd as upfrnt_deal_1_MC_cd ,
           |ps.upfrnt_deal_1_vrs_1_cd as upfrnt_deal_1_vrsn ,
           |ps.upfrnt_deal_2_ln_itm_cd as upfrnt_deal_2_ln_itm ,
           |ps.upfrnt_deal_2_m_cd as upfrnt_deal_2_MC_cd ,
           |ps.upfrnt_deal_2_vrsn_cd as upfrnt_deal_2_vrsn ,
           |ps.upfrnt_deal_3_ln_itm_cd as upfrnt_deal_3_ln_itm ,
           |ps.upfrnt_deal_3_m_cd as upfrnt_deal_3_MC_cd ,
           |ps.upfrnt_deal_3_vrsn_cd as upfrnt_deal_3_vrsn ,
           |ps.asp_coefficien_1_cd as ASP_Coefficient ,
           |COALESCE(ps.AVRG_deal_nt_COEFFICIENT_nm,0) as AVRG_deal_nt_Coefficient ,
           |ps.deal_geo_cd as deal_Geo ,
           |ps.dscnt_geo_cd as dscnt_Geo ,
           |ps.exch_rate_pcy_to_usd_cd as Ex_rate_PCY_to_usd ,
           |ps.exch_rate_lcy_to_us_1_cd as Ex_rate_LCY_to_usd ,
           |COALESCE(ps.EX_rate_VCY_to_usd_nm,0) as Ex_rate_VCY_to_usd ,
           |ps.used_cur_1_cd as lcl_curr_cd ,
           |ps.valtn_curr_cd as valtn_curr_cd ,
           |ps.cust_app_2_cd as valtn_cust_appl_cd ,
           |ps.pa_dscn_1_cd as valtn_prch_agrmnt_dscnt ,
           |ps.p_1_nr as valtn_prch_agrmnt ,
           |ps.used_prc_ds_1_cd as valtn_prc_dsc ,
           |ps.valtn_trsn_i_1_id as valtn_trsn_id ,
           |ps.valtn_wrnn_1_cd as valtn_wrnng_cd ,
           |ps.fl_i_6_id as E2Open_fl_id_valtn_rspns ,
           |ps.prnt_trsn_i_4_id as prnt_trsn_id_valtn_rspns,
           |ps.prv_trsn_i_5_id as prv_trsn_id_valtn_rspns,
           |ps.reporter_i_7_id as Reporter_id_valtn_rspns ,
           |ps.trsn_i_3_id as trsn_id_valtn_rspns ,
           |ps.ps_prs_ptnr_sl_prc_lcy_un_1_cd * ps.sale_qty_cd AS ptnr_rptd_prc_lcy,
           |-- ps.ps_prs_ptnr_sl_prc_lcy_un_1_cd * ps.sale_qty_cd AS ptnr_rptd_prc_lcy,
           |((ps.ptnr_sl_prc_lcy_un_1_cd * ps.sale_qty_cd) / ps.exch_rate_pcy_to_usd_cd) AS ptnr_rptd_prc_usd,
           |-- ((ps.ptnr_sl_prc_lcy_un_1_cd * ps.sale_qty_cd) / ps.exch_rate_pcy_to_usd_cd) AS ptnr_rptd_prc_usd,
           |case when split(ps.hpe_prod_i_1_id,'#')[0] == ps.hpe_prod_i_1_id then ps.sale_qty_cd end AS sls_bs_qty,
           |case when split(ps.hpe_prod_i_1_id,'#')[0] != ps.hpe_prod_i_1_id then ps.sale_qty_cd end AS sls_opt_qty,
           |0 AS sls_real_unt_qty , --CASE WHEN pdm_clss_typ.prod_typ IN ('U','RC') and split(ps.hpe_prod_i_1_id,'#')[0] == ps.hpe_prod_i_1_id then ps.sale_qty_cd end AS sls_real_unt_qty,
           |ps.src_sys_upd_ts,
           |ps.src_sys_ky,
           |ps.lgcl_dlt_ind,
           |ps.ins_gmt_ts,
           |ps.upd_gmt_ts,
           |ps.src_sys_extrc_gmt_ts,
           |ps.src_sys_btch_nr,
           |ps.fl_nm,
           |ps.ld_jb_nr,
           |COALESCE(dealBMT.deal_ind,'Base Business') AS deal_ind,
           |COALESCE(dealBMT.deal_ind_grp,'Others') AS deal_ind_grp,
           |COALESCE(case when PS.hpe_prod_i_1_id like '%#%' then split(PS.hpe_prod_i_1_id,'#')[0] END,PS.hpe_prod_i_1_id) AS bs_prod_nr,
           |trsnBMT.bi_trsn_xclsn_flg AS bi_trsn_xclsn_flg,
           |trsnBMT.bi_trsn_xclsn_rsn_cd,
           |case when PS.hpe_prod_i_1_id like '%#%' then split(PS.hpe_prod_i_1_id,'#')[1] end AS prod_opt,
           |COALESCE(skuBmtvol.vol_sku_flg,'No Volume SKU') AS vol_sku_flg,
           |CASE WHEN UPPER(TRIM(skuSrvcProdLn.srv_prod_ln)) = 'Y' THEN 'N/A' WHEN skuBmt.ctobto_flg is NULL and skuBmtOptCode.ctobto_flg is NULL THEN 'BTO' WHEN skuBmt.ctobto_flg is NULL and skuBmtOptCode.ctobto_flg is NOT NULL THEN skuBmtOptCode.ctobto_flg ELSE skuBmt.ctobto_flg END as bi_cto_bto_flg,
           |NULL AS bi_rslr_ctr_cd,
           |NULL AS bi_dstr_ctr_cd,
           |rptgptnr.ctry_cd AS bi_rptr_ctr_cd,
           |NULL AS bi_enttld_ctry_cd,
           |NULL AS bi_end_cust_ctry_cd,
           |dealBMT.MC_cd,
           |dealBMT.mc_cd_dn,
           |dealBMT.upfrnt_deal_1_mc_dn_cd,
           |dealBMT.upfrnt_deal_2_mc_dn_cd,
           |dealBMT.upfrnt_deal_3_mc_dn_cd,
           |dealBMT.backend_deal_1_mc_dn_cd,
           |dealBMT.backend_deal_2_mc_dn_cd,
           |dealBMT.backend_deal_3_mc_dn_cd,
           |dealBMT.backend_deal_4_mc_dn_cd,
           |psGrpgSoldTo.ps_grpg_e2open_extrt_nm AS ps_grpg_E2Open_extrt_nm,
           |psGrpgSoldTo.ps_grpg_e2open_seq_id AS ps_grpg_E2Open_seq_ID,
           |psGrpgSoldTo.ps_grpg_e2open_extrt_dt AS ps_grpg_E2Open_extrt_dt,
           |psGrpgSoldTo.ps_grpg_e2open_extrt_ts AS ps_grpg_E2Open_extrt_ts,
           |ps.ps_E2Open_extrt_nm,
           |ps.ps_E2Open_seq_ID,
           |ps.ps_E2Open_extrt_dt,
           |ps.ps_E2Open_extrt_ts,
           |ps.ps_prs_E2Open_extrt_nm,
           |ps.ps_prs_E2Open_seq_ID,
           |ps.ps_prs_E2Open_extrt_dt,
           |ps.ps_prs_E2Open_extrt_ts,
           |psGrpgSoldTo.ps_grpg_eap_load_dt AS ps_grpg_EAP_load_dt,
           |psGrpgSoldTo.ps_grpg_eap_load_ts AS ps_grpg_EAP_load_ts,
           |ps.ps_EAP_load_dt,
           |ps.ps_EAP_load_ts,
           |ps.ps_prs_EAP_load_dt,
           |ps.ps_prs_EAP_load_ts,
           |rw_ad.ps_rwaddr_E2Open_extrt_nm,
           |rw_ad.ps_rwaddr_E2Open_seq_ID,
           |rw_ad.ps_rwaddr_E2Open_extrt_dt,
           |rw_ad.ps_rwaddr_E2Open_extrt_ts,
           |rw_ad.ps_rwaddr_eap_load_dt,
           |rw_ad.ps_rwaddr_eap_load_ts,
           |ord.hpe_invoice_dt AS hpe_invoice_dt,
           |ord.hpe_ord_bundle_id AS hpe_ord_bundle_id,
           |ord.hpe_ord_deal_id AS hpe_ord_deal_id,
           |ord.hpe_obj_srvc_mtrl_id AS hpe_obj_srvc_mtrl_id,
           |ord.hpe_ord_item_nr AS hpe_ord_item_nr,
           |ord.hpe_ord_mc_cd AS hpe_ord_mc_cd,
           |ord.hpe_ord_pft_cntr AS hpe_ord_pft_cntr,
           |ord.hpe_ord_sales_dvsn_cd AS hpe_ord_sales_dvsn_cd,
           |ord.hpe_shpmt_dt AS hpe_shpmt_dt,
           |ord.hpe_ord_dt AS hpe_ord_dt,
           |ord.hpe_ord_drp_shp_flg AS hpe_ord_drp_shp_flg,
           |ord.hpe_ord_end_cust_prty_id AS hpe_ord_end_cust_prty_id,
           |ord.hpe_ord_nr AS hpe_ord_nr,
           |ord.hpe_ord_rtm AS hpe_ord_rtm,
           |ord.hpe_ord_sld_br_typ AS hpe_ord_sld_br_typ,
           |ord.hpe_ord_sld_pa_nr AS hpe_ord_sld_pa_nr,
           |ord.hpe_ord_sld_prty_id AS hpe_ord_sld_prty_id,
           |ord.hpe_ord_tier2_rslr_br_typ AS hpe_ord_tier2_rslr_br_typ,
           |ord.hpe_ord_tier2_rslr_prty_id AS hpe_ord_tier2_rslr_prty_id,
           |ord.hpe_ord_typ AS hpe_ord_typ,
           |OPEPRS.ope_rtm
           |FROM ea_support_temp_an.ps_ps_prsDF ps
           |LEFT JOIN ea_support_temp_an.chnlptnr_order_tmp_ps_fact ord ON ps.trsn_i_1_id=ord.trsn_i_2_id
           |LEFT JOIN rptg_ptnr_mstr_dmnsn_tmp rptgptnr ON rptgptnr.reporter_i_1_id=PS.reporter_i_2_id
           |LEFT JOIN OpePsDmnsnDF OPEPRS ON OPEPRS.trsn_i_1_id=PS.trsn_i_1_id
           |LEFT JOIN ea_common.chnlptnr_PDM_dmnsn pdm_clss_typ ON ps.hpe_prod_i_1_id = pdm_clss_typ.mtrl_mstr_mtrl_id
           |LEFT join ea_support_temp_an.bmtDealDF dealBMT ON PS.trsn_i_1_id = dealBMT.trsn_i_1_id
           |LEFT JOIN ea_support_temp_an.psGrpgSoldTo psGrpgSoldTo ON ps.sld_to_rw_addr_id = psGrpgSoldTo.rawaddr_id
           |LEFT JOIN ea_support_temp_an.SPRM_ptnr_pfl_dmnsn_DF sldToSblHrchy
           |ON psGrpgSoldTo.ptnr_pro_i_2_id = sldToSblHrchy.ptnr_pfl_row_id
           |LEFT JOIN ea_support_temp_an.bmtTrnsnDF trsnBMT ON trsnBMT.trsn_i_1_id=PS.trsn_i_1_id
           |LEFT JOIN ea_support_temp_an.chnlptnr_tmp_rw_addr rw_ad ON PS.trsn_i_1_id=rw_ad.trsn_id
           |LEFT JOIN (select * from ea_common.bmt_bi_sku_dmnsn where rec_src='VOLUME_SKU' and  product_nr is not NULL) skuBmtvol ON pdm_clss_typ.mtrl_mstr_mtrl_id = skuBmtvol.product_nr and rptgptnr.ctry_cd = skuBmtvol.reporter_ctry_cd
           |LEFT JOIN (select * from ea_common.bmt_bi_sku_dmnsn where UPPER(TRIM(rec_src))='CTO_BTO' and  product_nr is not NULL) skuBmt ON PS.hpe_prod_i_1_id = skuBmt.product_nr
           |LEFT JOIN (select * from ea_common.bmt_bi_sku_dmnsn where UPPER(TRIM(rec_src))='CTO_BTO' and  product_option is not NULL) skuBmtOptCode ON split(PS.hpe_prod_i_1_id,'#')[1]=skuBmtOptCode.product_option
           |LEFT JOIN (select * from ea_common.bmt_bi_sku_dmnsn where UPPER(TRIM(rec_src))='SERVICES_PL' and  product_nr is not NULL) skuSrvcProdLn ON ps.prod_ln_i_1_id=skuSrvcProdLn.product_nr
           |""".stripMargin)
      var chnlptnr_pos_fact_df = chnlptnrPosFact.repartition(25).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "chnlptnr_tmp_ps_fact")
      chnlptnrPosFact.createOrReplaceTempView("chnlptnr_tmp_ps_fact")
      logger.info("tmp POS FACT Load is completed")

      val chnlptnrTmpGroupingDf = spark.sql(
        s"""
           |select ps.trsn_id as trsn_id,
           |psGrpgBillTo.dun_2_cd as bll_to_duns ,
           |psGrpgBillTo.ent_mdm_br_ty_2_cd as bll_to_ent_MDM_BR_typ ,
           |psGrpgBillTo.ent_mdm_prty_id as bll_to_ent_MDM_prty_id ,
           |psGrpgBillTo.mdcp_br_ty_2_cd as bll_to_MDCP_BR_typ ,
           |psGrpgBillTo.MDCP_BRI_ID as bll_to_MDCP_BRI_id ,
           |psGrpgBillTo.MDCP_LOC_ID as bll_to_MDCP_LOC_id ,
           |psGrpgBillTo.MDCP_OPSI_ID as bll_to_MDCP_opsi_id ,
           |psGrpgBillTo.MDCP_ORG_ID as bll_to_MDCP_ORG_id ,
           |psGrpgBillTo.ptnr_pro_i_2_id as bll_to_ptnr_pro_id ,
           |psGrpgBillTo.rawaddr_id as Bill_To_RAWADDR_id ,
           |psGrpgBillTo.rawaddr_ky_cd as Bill_To_RAWADDR_KEY ,
           |psGrpgBillTo.rsrv_fld_1_nm as POS_GRP_Bill_To_RESERVE_FIELD_1 ,
           |psGrpgBillTo.rsrv_fld_2_nm as POS_GRP_Bill_To_RESERVE_FIELD_2 ,
           |psGrpgBillTo.rsrv_fld_3_nm as POS_GRP_Bill_To_RESERVE_FIELD_3 ,
           |psGrpgBillTo.rsrv_fld_4_nm as POS_GRP_Bill_To_RESERVE_FIELD_4 ,
           |psGrpgBillTo.rsrv_fld_5_nm as POS_GRP_Bill_To_RESERVE_FIELD_5 ,
           |psGrpgSellFrm.dun_2_cd as sl_frm_duns ,
           |psGrpgSellFrm.ent_mdm_br_ty_2_cd as sl_frm_ent_MDM_BR_typ ,
           |psGrpgSellFrm.ent_mdm_prty_id as sl_frm_ent_MDM_prty_id ,
           |psGrpgSellFrm.mdcp_br_ty_2_cd as sl_frm_MDCP_BR_typ ,
           |psGrpgSellFrm.mdcp_bri_id as sl_frm_MDCP_BRI_id ,
           |psGrpgSellFrm.mdcp_loc_id as sl_frm_MDCP_LOC_id ,
           |psGrpgSellFrm.mdcp_opsi_id as sl_frm_MDCP_opsi_id ,
           |psGrpgSellFrm.mdcp_org_id as sl_frm_MDCP_ORG_id ,
           |psGrpgSellFrm.ptnr_pro_i_2_id as sl_frm_ptnr_pro_id ,
           |psGrpgSellFrm.rawaddr_id as Sell_From_RAWADDR_id ,
           |psGrpgSellFrm.rawaddr_ky_cd as Sell_From_RAWADDR_KEY ,
           |psGrpgSellFrm.rsrv_fld_1_nm as POS_GRP_Sell_From_RESERVE_FIELD_1 ,
           |psGrpgSellFrm.rsrv_fld_2_nm as POS_GRP_Sell_From_RESERVE_FIELD_2 ,
           |psGrpgSellFrm.rsrv_fld_3_nm as POS_GRP_Sell_From_RESERVE_FIELD_3 ,
           |psGrpgSellFrm.rsrv_fld_4_nm as POS_GRP_Sell_From_RESERVE_FIELD_4 ,
           |psGrpgSellFrm.rsrv_fld_5_nm as POS_GRP_Sell_From_RESERVE_FIELD_5 ,
           |psGrpgShipFrm.rawaddr_id as Ship_From_RAWADDR_id ,
           |psGrpgShipFrm.rawaddr_ky_cd as Ship_From_RAWADDR_KEY ,
           |psGrpgShipFrm.rsrv_fld_1_nm as POS_GRP_Ship_From_RESERVE_FIELD_1 ,
           |psGrpgShipFrm.rsrv_fld_2_nm as POS_GRP_Ship_From_RESERVE_FIELD_2 ,
           |psGrpgShipFrm.rsrv_fld_3_nm as POS_GRP_Ship_From_RESERVE_FIELD_3 ,
           |psGrpgShipFrm.rsrv_fld_4_nm as POS_GRP_Ship_From_RESERVE_FIELD_4 ,
           |psGrpgShipFrm.rsrv_fld_5_nm as POS_GRP_Ship_From_RESERVE_FIELD_5 ,
           |psGrpgShipFrm.dun_2_cd as shp_frm_duns ,
           |psGrpgShipFrm.ent_mdm_br_ty_2_cd as shp_frm_ent_MDM_BR_typ ,
           |psGrpgShipFrm.ent_mdm_prty_id as shp_frm_ent_MDM_prty_id ,
           |psGrpgShipFrm.mdcp_br_ty_2_cd as shp_frm_MDCP_BR_typ ,
           |psGrpgShipFrm.mdcp_bri_id as shp_frm_MDCP_BRI_id ,
           |psGrpgShipFrm.mdcp_loc_id as shp_frm_MDCP_LOC_id ,
           |psGrpgShipFrm.mdcp_opsi_id as shp_frm_MDCP_opsi_id ,
           |psGrpgShipFrm.mdcp_org_id as shp_frm_MDCP_ORG_id ,
           |psGrpgShipFrm.ptnr_pro_i_2_id as shp_frm_ptnr_pro_id ,
           |psGrpgShipTo.rawaddr_id as Ship_To_RAWADDR_id ,
           |psGrpgShipTo.rawaddr_ky_cd as Ship_To_RAWADDR_KEY ,
           |psGrpgShipTo.rsrv_fld_1_nm as POS_GRP_Ship_To_RESERVE_FIELD_1 ,
           |psGrpgShipTo.rsrv_fld_2_nm as POS_GRP_Ship_To_RESERVE_FIELD_2 ,
           |psGrpgShipTo.rsrv_fld_3_nm as POS_GRP_Ship_To_RESERVE_FIELD_3 ,
           |psGrpgShipTo.rsrv_fld_4_nm as POS_GRP_Ship_To_RESERVE_FIELD_4 ,
           |psGrpgShipTo.rsrv_fld_5_nm as POS_GRP_Ship_To_RESERVE_FIELD_5 ,
           |psGrpgShipTo.dun_2_cd as shp_to_duns ,
           |psGrpgShipTo.ent_mdm_br_ty_2_cd as shp_to_ent_MDM_BR_typ ,
           |psGrpgShipTo.ent_mdm_prty_id as shp_to_ent_MDM_prty_id ,
           |psGrpgShipTo.mdcp_br_ty_2_cd AS shp_to_MDCP_BR_typ ,
           |psGrpgShipTo.MDCP_BRI_ID as shp_to_MDCP_BRI_id ,
           |psGrpgShipTo.MDCP_LOC_ID AS shp_to_MDCP_LOC_id ,
           |psGrpgShipTo.MDCP_OPSI_ID as shp_to_MDCP_opsi_id ,
           |psGrpgShipTo.MDCP_ORG_ID as shp_to_MDCP_ORG_id ,
           |psGrpgShipTo.ptnr_pro_i_2_id as shp_to_ptnr_pro_id ,
           |psGrpgSoldTo.rawaddr_id as Sold_To_RAWADDR_id ,
           |psGrpgSoldTo.rawaddr_ky_cd as Sold_To_RAWADDR_KEY ,
           |psGrpgSoldTo.rsrv_fld_1_nm as POS_GRP_Sold_To_RESERVE_FIELD_1 ,
           |psGrpgSoldTo.rsrv_fld_2_nm as POS_GRP_Sold_To_RESERVE_FIELD_2 ,
           |psGrpgSoldTo.rsrv_fld_3_nm as POS_GRP_Sold_To_RESERVE_FIELD_3 ,
           |psGrpgSoldTo.rsrv_fld_4_nm as POS_GRP_Sold_To_RESERVE_FIELD_4 ,
           |psGrpgSoldTo.rsrv_fld_5_nm as POS_GRP_Sold_To_RESERVE_FIELD_5 ,
           |psGrpgSoldTo.dun_2_cd AS sld_to_duns ,
           |psGrpgSoldTo.ent_mdm_br_ty_2_cd as sld_to_ent_MDM_BR_typ ,
           |psGrpgSoldTo.ent_mdm_prty_id as sld_to_ent_MDM_prty_id ,
           |psGrpgSoldTo.mdcp_br_ty_2_cd AS sld_to_MDCP_BR_typ,
           |psGrpgSoldTo.MDCP_BRI_ID as sld_to_MDCP_BRI_id ,
           |psGrpgSoldTo.MDCP_LOC_ID as sld_to_MDCP_LOC_id ,
           |psGrpgSoldTo.MDCP_OPSI_ID as sld_to_MDCP_opsi_id ,
           |psGrpgSoldTo.MDCP_ORG_ID as sld_to_MDCP_ORG_id ,
           |psGrpgSoldTo.ptnr_pro_i_2_id AS sld_to_ptnr_pro_id,
           |psGrpgEndCust.rawaddr_id as end_cust_RAWADDR_id ,
           |psGrpgEndCust.rawaddr_ky_cd as end_cust_RAWADDR_KEY ,
           |psGrpgEndCust.rsrv_fld_1_nm as POS_GRP_End_Customer_RESERVE_FIELD_1 ,
           |psGrpgEndCust.rsrv_fld_2_nm as POS_GRP_End_Customer_RESERVE_FIELD_2 ,
           |psGrpgEndCust.rsrv_fld_3_nm as POS_GRP_End_Customer_RESERVE_FIELD_3 ,
           |psGrpgEndCust.rsrv_fld_4_nm as POS_GRP_End_Customer_RESERVE_FIELD_4 ,
           |psGrpgEndCust.rsrv_fld_5_nm as POS_GRP_End_Customer_RESERVE_FIELD_5 ,
           |psGrpgEndCust.dun_2_cd AS end_cust_duns ,
           |psGrpgEndCust.ent_mdm_br_ty_2_cd as end_cust_ent_MDM_BR_typ ,
           |psGrpgEndCust.ent_mdm_prty_id as end_cust_ent_MDM_prty ,
           |psGrpgEndCust.mdcp_br_ty_2_cd AS end_cust_MDCP_BR_typ,
           |psGrpgEndCust.mdcp_bri_id AS end_cust_MDCP_BRI_id,
           |psGrpgEndCust.mdcp_loc_id AS end_cust_MDCP_LOC_id,
           |psGrpgEndCust.mdcp_opsi_id as end_cust_MDCP_opsi_id,
           |psGrpgEndCust.mdcp_org_id as end_cust_MDCP_ORG_id,
           |psGrpgEndCust.ptnr_pro_i_2_id as end_cust_ptnr_pro_id ,
           |psGrpgEntltd.rawaddr_id as enttld_ent_RAWADDR_id ,
           |psGrpgEntltd.rawaddr_ky_cd as enttld_ent_RAWADDR_KEY ,
           |psGrpgEntltd.rsrv_fld_1_nm as POS_GRP_enttld_ent_RESERVE_FIELD_1 ,
           |psGrpgEntltd.rsrv_fld_2_nm as POS_GRP_enttld_ent_RESERVE_FIELD_2 ,
           |psGrpgEntltd.rsrv_fld_3_nm as POS_GRP_enttld_ent_RESERVE_FIELD_3 ,
           |psGrpgEntltd.rsrv_fld_4_nm as POS_GRP_enttld_ent_RESERVE_FIELD_4 ,
           |psGrpgEntltd.rsrv_fld_5_nm as POS_GRP_enttld_ent_RESERVE_FIELD_5 ,
           |psGrpgEntltd.dun_2_cd as enttld_duns ,
           |psGrpgEntltd.ent_mdm_br_ty_2_cd as enttld_ent_MDM_BR_typ ,
           |psGrpgEntltd.ent_mdm_prty_id as enttld_ent_MDM_prty_id ,
           |psGrpgEntltd.mdcp_br_ty_2_cd as enttld_MDCP_BR_typ ,
           |psGrpgEntltd.mdcp_bri_id as enttld_MDCP_BRI_id ,
           |psGrpgEntltd.mdcp_loc_id as enttld_MDCP_LOC_id ,
           |psGrpgEntltd.mdcp_opsi_id as enttld_MDCP_opsi_id ,
           |psGrpgEntltd.mdcp_org_id as enttld_MDCP_ORG_id ,
           |psGrpgEntltd.ptnr_pro_i_2_id as enttld_ptnr_pro_id
           |FROM ea_support_temp_an.chnlptnr_tmp_ps_fact ps
           |LEFT JOIN ea_support_temp_an.psGrpgBillTo psGrpgBillTo
           |ON ps.bll_to_rw_addr_id = psGrpgBillTo.rawaddr_id
           |LEFT JOIN ea_support_temp_an.psGrpgSellFrm psGrpgSellFrm
           |ON ps.sl_frm_rw_addr_id = psGrpgSellFrm.rawaddr_id
           |LEFT JOIN ea_support_temp_an.psGrpgShipTo psGrpgShipTo
           |ON ps.shp_to_rw_addr_id = psGrpgShipTo.rawaddr_id
           |LEFT JOIN ea_support_temp_an.psGrpgShipFrm psGrpgShipFrm
           |ON ps.shp_frm_rw_addr_id = psGrpgShipFrm.rawaddr_id
           |LEFT JOIN ea_support_temp_an.psGrpgSoldTo psGrpgSoldTo
           |ON ps.sld_to_rw_addr_id = psGrpgSoldTo.rawaddr_id
           |LEFT JOIN ea_support_temp_an.psGrpgEndCust psGrpgEndCust
           |ON ps.end_usr_rw_addr_id = psGrpgEndCust.rawaddr_id
           |LEFT JOIN ea_support_temp_an.psGrpgEntltd psGrpgEntltd
           |ON ps.enttld_rw_addr_id = psGrpgEntltd.rawaddr_id
           |""".stripMargin)

      var chnlptnr_tmp_GrpLoad = chnlptnrTmpGroupingDf.repartition(20).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "chnlptnr_tmp_Grouping")
      logger.info("************chnlptnr_tmp_Grouping loaded")

      val chnlptnrPosFactGrpgDf = spark.sql(
        s"""
           |select
           |ps.src_sys_nm,
           |ps.rec_src,
           |ps.backend_deal_id_1,
           |ps.rptd_backend_deal_1,
           |ps.backend_deal_id_2,
           |ps.rptd_backend_deal_2,
           |ps.backend_deal_id_3,
           |ps.rptd_backend_deal_3,
           |ps.backend_deal_id_4,
           |ps.rptd_backend_deal_4,
           |ps.bll_to_asian_addr,
           |ps.bll_to_co_tx_id,
           |ps.bll_to_co_tx_id_enr,
           |ps.bll_to_cntct_nm,
           |ps.bll_to_ctry_rptd,
           |ps.bll_to_ky,
           |ps.bll_to_ph_nr,
           |ps.bll_to_rw_addr_id,
           |ps.bll_to_rptd_id,
           |ps.nmso_name,
           |ps.nmso_date,
           |ps.standing_offer_number,
           |ps.deal_reg_1,
           |ps.rptd_deal_reg_1,
           |ps.deal_reg_2,
           |ps.rptd_deal_reg_2,
           |ps.nt_cst_aftr_rbt_lcy,
           |ps.ptnr_prch_lcy_unt_prc,
           |ps.ptnr_sl_lcy_unt_prc,
           |ps.ptnr_sl_usd_unt_prc,
           |ps.sls_bndl_qty,
           |ps.sls_qty,
           |ps.pos_iud_flag,
           |ps.pos_reserve_field_1,
           |ps.pos_reserve_field_2,
           |ps.rptg_prd_strt_dt,
           |ps.rptg_prd_end_dt,
           |ps.pos_reserve_field_5,
           |ps.pos_reserve_field_6,
           |ps.pos_reserve_field_7,
           |ps.pos_reserve_field_8,
           |ps.pos_reserve_field_9,
           |ps.pos_reserve_field_10,
           |ps.bndl_prod_id,
           |ps.orgl_prod_ln_id,
           |ps.ptnr_bndl_prod_id,
           |ps.ptnr_inrn_prod_nr,
           |ps.prod_nr,
           |ps.rptd_prod_nr,
           |ps.vl_vol_flg,
           |ps.reporter_id,
           |ps.sl_frm_cntct_nm,
           |ps.sl_frm_ctry_rptd,
           |ps.sl_frm_ky,
           |ps.sl_frm_ph_nr,
           |ps.sl_frm_rw_addr_id,
           |ps.sl_frm_rptd_id,
           |ps.shp_frm_cntct_nm,
           |ps.shp_frm_ctry_rptd,
           |ps.shp_frm_ky,
           |ps.shp_frm_ph_nr,
           |ps.shp_frm_rw_addr_id,
           |ps.shp_frm_rptd_id,
           |ps.shp_to_asian_addr,
           |ps.shp_to_co_tx_id,
           |ps.shp_to_co_tx_id_enr,
           |ps.shp_to_cntct_nm,
           |ps.shp_to_ctry_rptd,
           |ps.shp_to_ky,
           |ps.shp_to_ph_nr,
           |ps.shp_to_rw_addr_id,
           |ps.shp_to_rptd_id,
           |ps.sld_to_asian_addr,
           |ps.sld_to_co_tx_id,
           |ps.sld_to_co_tx_id_enr,
           |ps.sld_to_cntct_nm,
           |ps.sld_to_ctry_rptd,
           |ps.sld_to_ky,
           |ps.sld_to_ph_nr,
           |ps.sld_to_rw_addr_id,
           |ps.sld_to_rptd_id,
           |ps.end_usr_addr_src_rptd,
           |ps.end_usr_asian_addr,
           |ps.end_usr_co_tx_id,
           |ps.end_usr_co_tx_id_enr,
           |ps.end_usr_cntct_nm,
           |ps.end_usr_ctry_rptd,
           |ps.end_usr_ky,
           |ps.end_usr_ph_nr,
           |ps.end_usr_rw_addr_id,
           |ps.end_usr_rptd_id,
           |ps.enttld_asian_addr,
           |ps.enttld_co_tx_id,
           |ps.enttld_co_tx_id_enr,
           |ps.enttld_cntct_nm,
           |ps.enttld_ctry_rptd,
           |ps.enttld_ky,
           |ps.enttld_ph_nr,
           |ps.enttld_rw_addr_id,
           |ps.enttld_rptd_id,
           |ps.agt_flg,
           |ps.brm_err_flg,
           |ps.crss_shp_flg,
           |ps.cust_to_ptnr_po_nr,
           |ps.dt,
           |ps.drp_shp_flg,
           |ps.e2open_fl_id,
           |ps.e2open_fl_rcvd_dt,
           |ps.hpe_inv_nr,
           |ps.nt_cst_aftr_rbt_curr_cd,
           |ps.org_sls_nm,
           |ps.orgl_hpe_asngd_trsn_id,
           |ps.orgl_trsn_id,
           |ps.prnt_trsn_id,
           |ps.ptnr_inrn_trsn_id,
           |ps.ptnr_inv_dt,
           |ps.ptnr_inv_nr,
           |ps.ptnr_prch_curr_cd,
           |ps.ptnr_sls_rep_nm,
           |ps.ptnr_sl_prc_curr_cd,
           |ps.ptnr_sm_dt,
           |ps.ptnr_to_hpe_po_nr,
           |ps.prv_trsn_id,
           |ps.prod_orgn_cd,
           |ps.prchg_src_ws_ind,
           |ps.splyr_iso_ctry_cd,
           |ps.tty_mgr_cd,
           |ps.trsn_id,
           |ps.vldtn_wrnng_cd,
           |ps.rptd_upfrnt_deal_1,
           |ps.upfrnt_deal_id_1_id,
           |ps.rptd_upfrnt_deal_2,
           |ps.upfrnt_deal_id_2,
           |ps.rptd_upfrnt_deal_3,
           |ps.upfrnt_deal_id_3_id,
           |ps.ope_bmt_dstr_bsn_rshp_typ,
           |ps.ope_bmt_dstr_prty_id,
           |ps.ope_bmt_dstr_sls_comp_rlvnt,
           |ps.ope_dstr_bsn_rshp_typ,
           |ps.ope_dstr_prty_id,
           |ps.ope_dstr_rsn_cd,
           |ps.ope_bmt_end_cust_bsn_rshp_typ,
           |ps.ope_bmt_end_cust_prty_id,
           |ps.ope_bmt_end_cust_sls_comp_rlvnt,
           |ps.ope_deal_end_cust_bsn_rshp_typ,
           |ps.ope_deal_end_cust_prty_id,
           |ps.ope_end_cust_bsn_rshp_typ,
           |ps.ope_end_cust_prty_id,
           |ps.ope_end_cust_rsn_cd,
           |ps.ope_lvl_3_bsn_rshp_typ,
           |ps.ope_lvl_3_prty_id,
           |ps.ope_lvl_3_rshp,
           |ps.ope_src_enttld_prty_ctry,
           |ps.ope_src_enttld_prty_prty_id,
           |ps.ope_src_end_cust_ctry,
           |ps.ope_src_end_cust_prty_id,
           |ps.ope_enttld_prty_ctry,
           |ps.ope_entitlted_prty_id,
           |ps.ope_dstr_bsn_mgr_rptg_dstr,
           |ps.ope_dstr_rptg_dstr,
           |ps.ope_end_cust_sls_rep_rptg_dstr,
           |ps.ope_flipped_dstr_bsn_mgr_vw_flg,
           |ps.ope_flipped_dstr_vw_flg,
           |ps.ope_flipped_end_cust_vw_flg,
           |ps.ope_flipped_ptnr_bsn_mgr_vw_flg,
           |ps.ope_flipped_rslr_vw_flg,
           |ps.ope_lvl_1_bsn_rshp_typ,
           |ps.ope_lvl_1_prty_id,
           |ps.ope_lvl_1_rshp,
           |ps.ope_ptnr_bsn_mgr_rptg_dstr,
           |ps.ope_rslr_rptg_dstr,
           |ps.ope_bmt_rslr_bsn_rshp_typ,
           |ps.ope_bmt_rslr_prty_id,
           |ps.ope_bmt_rslr_sls_comp_rlvnt,
           |ps.ope_rslr_bsn_rshp_typ,
           |ps.ope_rslr_prty_id,
           |ps.ope_rslr_rsn_cd,
           |ps.ope_lvl_2_bsn_rshp_typ,
           |ps.ope_lvl_2_prty_id,
           |ps.ope_lvl_2_rshp,
           |ps.ope_sld_to_br_typ,
           |ps.ope_sld_to_ctry,
           |ps.ope_sld_to_prty_id,
           |ps.ope_big_deal_xclsn,
           |ps.ope_crss_brdr,
           |ps.ope_crss_srcd,
           |ps.ope_dstr_rptg_dstr_to_another_rptg_dstr,
           |ps.ope_dstr_bookings_comp_vw,
           |ps.ope_dstr_bookings_vw,
           |ps.ope_dstr_bsn_mgr_bookings_vw,
           |ps.ope_dstr_bsn_mgr_comp_vw,
           |ps.ope_drp_shp,
           |ps.ope_emb_srv_ln_itms,
           |ps.ope_end_cust_rptg_dstr_to_another_rptg_dstr,
           |ps.ope_end_cust_sls_rep_bookings_vw,
           |ps.ope_end_cust_sls_rep_comp_vw,
           |ps.ope_fdrl_bsn,
           |ps.ope_ptnr_bsn_mgr_bookings_vw,
           |ps.ope_ptnr_bsn_mgr_comp_vw,
           |ps.ope_pft_cntr,
           |ps.ope_rslr_bookings_vw,
           |ps.ope_rslr_comp_vw,
           |ps.ope_srv_ln_itm,
           |ps.ope_trsn_id,
           |ps.ope_reporter_prty_id,
           |ps.ope_br_ty_1_cd,
           |ps.rptr_prty_ky,
           |ps.bll_to_raw_address_id,
           |coalesce(ps.bll_to_lctn_ctry_cd,pdw.bill_to_country_code_cd) as bll_to_lctn_ctry_cd,
           |ps.bll_to_lctn_addr_1,
           |ps.bll_to_lctn_addr_2,
           |coalesce(ps.bll_to_lctn_cty_nm,pdw.bill_to_city_cd) as bll_to_lctn_cty_nm,
           |coalesce(ps.bll_to_lctn_nm,pdw.bill_to_customer_name_nm) as bll_to_lctn_nm,
           |ps.bll_to_lctn_pstl_cd,
           |coalesce(ps.bll_to_lctn_st_cd,pdw.bill_to_state_code_cd) as bll_to_lctn_st_cd,
           |ps.sell_from_raw_address_id,
           |ps.sl_frm_lctn_ctry_cd,
           |ps.sl_frm_lctn_addr_1,
           |ps.sl_frm_lctn_addr_2,
           |ps.sl_frm_lctn_cty_nm,
           |ps.sl_frm_lctn_nm,
           |ps.sl_frm_lctn_pstl_cd,
           |ps.sl_frm_lctn_st_cd,
           |ps.ship_to_raw_address_id,
           |coalesce(ps.shp_to_lctn_ctry_cd,pdw.ship_to_country_code_cd) as shp_to_lctn_ctry_cd,
           |ps.shp_to_lctn_addr_1,
           |ps.shp_to_lctn_addr_2,
           |coalesce(ps.shp_to_lctn_cty_nm,pdw.ship_to_city_cd) as shp_to_lctn_cty_nm,
           |coalesce(ps.shp_to_lctn_nm,pdw.ship_to_customer_name_nm) as shp_to_lctn_nm,
           |ps.shp_to_lctn_pstl_cd,
           |coalesce(ps.shp_to_lctn_st_cd,pdw.ship_to_state_code_cd) as shp_to_lctn_st_cd,
           |ps.ship_from_raw_address_id,
           |ps.shp_frm_lctn_ctry_cd,
           |ps.shp_frm_lctn_addr_1,
           |ps.shp_frm_lctn_addr_2,
           |ps.shp_frm_lctn_cty_nm,
           |ps.shp_frm_lctn_nm,
           |ps.shp_frm_lctn_pstl_cd,
           |ps.shp_frm_lctn_st_cd,
           |ps.sold_to_raw_address_id,
           |coalesce(ps.sld_to_lctn_ctry_cd,pdw.buyer_wcc_cty_code_cd) AS sld_to_lctn_ctry_cd,
           |ps.sld_to_lctn_addr_1,
           |ps.sld_to_lctn_addr_2,
           |ps.sld_to_lctn_cty_nm,
           |ps.sld_to_lctn_nm,
           |ps.sld_to_lctn_pstl_cd,
           |ps.sld_to_lctn_st_cd,
           |ps.end_cust_raw_address_id,
           |coalesce(ps.end_cust_lctn_ctry_cd,pdw.end_user_country_code_cd) as end_cust_lctn_ctry_cd,
           |ps.end_cust_lctn_addr_1,
           |ps.end_cust_lctn_addr_2,
           |coalesce(ps.end_cust_lctn_cty_nm,pdw.end_user_add_city_cd) as end_cust_lctn_cty_nm,
           |coalesce(ps.end_cust_lctn_nm,pdw.end_user_name_nm) as end_cust_lctn_nm,
           |ps.end_cust_lctn_pstl_cd,
           |coalesce(ps.end_cust_lctn_st_cd,pdw.end_user_add_state_cd) as end_cust_lctn_st_cd,
           |ps.enttld_lctn_raw_address_id,
           |ps.enttld_lctn_ctry_cd,
           |ps.enttld_lctn_addr_1,
           |ps.enttld_lctn_addr_2,
           |ps.enttld_lctn_cty_nm,
           |ps.enttld_lctn_nm,
           |ps.enttld_lctn_pstl_cd,
           |ps.enttld_lctn_st_cd,
           |coalesce(grp.bll_to_duns,pdw.bill_to_duns_id) as bll_to_duns,
           |grp.bll_to_ent_MDM_BR_typ,
           |grp.bll_to_ent_MDM_prty_id,
           |coalesce(grp.bll_to_MDCP_BR_typ,pdw.bill_to_mdcp_br_type_cd) as bll_to_MDCP_BR_typ,
           |coalesce(grp.bll_to_MDCP_BRI_id,pdw.bill_to_mdcp_br_item_id) as bll_to_MDCP_BRI_id,
           |coalesce(grp.bll_to_MDCP_LOC_id,pdw.bill_to_mdcp_locator_id) as bll_to_MDCP_LOC_id,
           |coalesce(grp.bll_to_MDCP_opsi_id,pdw.bill_to_mdcp_opsi_id) as bll_to_MDCP_opsi_id,
           |coalesce(grp.bll_to_MDCP_ORG_id,pdw.bill_to_mdcp_org_id) as bll_to_MDCP_ORG_id,
           |grp.bll_to_ptnr_pro_id,
           |grp.Bill_To_RAWADDR_id,
           |grp.Bill_To_RAWADDR_KEY,
           |grp.POS_GRP_Bill_To_RESERVE_FIELD_1,
           |grp.POS_GRP_Bill_To_RESERVE_FIELD_2,
           |grp.POS_GRP_Bill_To_RESERVE_FIELD_3,
           |grp.POS_GRP_Bill_To_RESERVE_FIELD_4,
           |grp.POS_GRP_Bill_To_RESERVE_FIELD_5,
           |grp.sl_frm_duns,
           |grp.sl_frm_ent_MDM_BR_typ,
           |grp.sl_frm_ent_MDM_prty_id,
           |grp.sl_frm_MDCP_BR_typ,
           |grp.sl_frm_MDCP_BRI_id,
           |grp.sl_frm_MDCP_LOC_id,
           |grp.sl_frm_MDCP_opsi_id,
           |grp.sl_frm_MDCP_ORG_id,
           |grp.sl_frm_ptnr_pro_id,
           |grp.Sell_From_RAWADDR_id,
           |grp.Sell_From_RAWADDR_KEY,
           |grp.POS_GRP_Sell_From_RESERVE_FIELD_1,
           |grp.POS_GRP_Sell_From_RESERVE_FIELD_2,
           |grp.POS_GRP_Sell_From_RESERVE_FIELD_3,
           |grp.POS_GRP_Sell_From_RESERVE_FIELD_4,
           |grp.POS_GRP_Sell_From_RESERVE_FIELD_5,
           |grp.Ship_From_RAWADDR_id,
           |grp.Ship_From_RAWADDR_KEY,
           |grp.POS_GRP_Ship_From_RESERVE_FIELD_1,
           |grp.POS_GRP_Ship_From_RESERVE_FIELD_2,
           |grp.POS_GRP_Ship_From_RESERVE_FIELD_3,
           |grp.POS_GRP_Ship_From_RESERVE_FIELD_4,
           |grp.POS_GRP_Ship_From_RESERVE_FIELD_5,
           |grp.shp_frm_duns,
           |grp.shp_frm_ent_MDM_BR_typ,
           |grp.shp_frm_ent_MDM_prty_id,
           |grp.shp_frm_MDCP_BR_typ,
           |grp.shp_frm_MDCP_BRI_id,
           |grp.shp_frm_MDCP_LOC_id,
           |grp.shp_frm_MDCP_opsi_id,
           |grp.shp_frm_MDCP_ORG_id,
           |grp.shp_frm_ptnr_pro_id,
           |grp.Ship_To_RAWADDR_id,
           |grp.Ship_To_RAWADDR_KEY,
           |grp.POS_GRP_Ship_To_RESERVE_FIELD_1,
           |grp.POS_GRP_Ship_To_RESERVE_FIELD_2,
           |grp.POS_GRP_Ship_To_RESERVE_FIELD_3,
           |grp.POS_GRP_Ship_To_RESERVE_FIELD_4,
           |grp.POS_GRP_Ship_To_RESERVE_FIELD_5,
           |coalesce(grp.shp_to_duns,pdw.ship_to_duns_id) as shp_to_duns,
           |grp.shp_to_ent_MDM_BR_typ,
           |grp.shp_to_ent_MDM_prty_id,
           |coalesce(grp.shp_to_MDCP_BR_typ,pdw.ship_to_mdcp_br_type_cd) as shp_to_MDCP_BR_typ,
           |coalesce(grp.shp_to_MDCP_BRI_id,pdw.ship_to_mdcp_br_item_id) as shp_to_MDCP_BRI_id,
           |coalesce(grp.shp_to_MDCP_LOC_id,pdw.ship_to_mdcp_locator_id) as shp_to_MDCP_LOC_id,
           |coalesce(grp.shp_to_MDCP_opsi_id,pdw.ship_to_mdcp_opsi_id) as shp_to_MDCP_opsi_id,
           |coalesce(grp.shp_to_MDCP_ORG_id,pdw.ship_to_mdcp_org_id) as shp_to_MDCP_ORG_id,
           |grp.shp_to_ptnr_pro_id,
           |grp.Sold_To_RAWADDR_id,
           |grp.Sold_To_RAWADDR_KEY,
           |grp.POS_GRP_Sold_To_RESERVE_FIELD_1,
           |grp.POS_GRP_Sold_To_RESERVE_FIELD_2,
           |grp.POS_GRP_Sold_To_RESERVE_FIELD_3,
           |grp.POS_GRP_Sold_To_RESERVE_FIELD_4,
           |grp.POS_GRP_Sold_To_RESERVE_FIELD_5,
           |coalesce(grp.sld_to_duns,pdw.sold_to_duns_id) as sld_to_duns,
           |grp.sld_to_ent_MDM_BR_typ,
           |grp.sld_to_ent_MDM_prty_id,
           |coalesce(grp.sld_to_MDCP_BR_typ,pdw.sold_to_mdcp_br_type_cd) as sld_to_MDCP_BR_typ,
           |coalesce(grp.sld_to_MDCP_BRI_id,pdw.sold_to_mdcp_br_item_id) as sld_to_MDCP_BRI_id,
           |coalesce(grp.sld_to_MDCP_LOC_id,pdw.sold_to_mdcp_locator_id) as sld_to_MDCP_LOC_id,
           |coalesce(grp.sld_to_MDCP_opsi_id,pdw.sold_to_mdcp_opsi_id) as sld_to_MDCP_opsi_id,
           |coalesce(grp.sld_to_MDCP_ORG_id,pdw.sold_to_mdcp_org_id) as sld_to_MDCP_ORG_id,
           |coalesce(grp.sld_to_ptnr_pro_id,pdw.co_id_buyer_co_id) as sld_to_ptnr_pro_id,
           |grp.end_cust_RAWADDR_id,
           |grp.end_cust_RAWADDR_KEY,
           |grp.POS_GRP_End_Customer_RESERVE_FIELD_1,
           |grp.POS_GRP_End_Customer_RESERVE_FIELD_2,
           |grp.POS_GRP_End_Customer_RESERVE_FIELD_3,
           |grp.POS_GRP_End_Customer_RESERVE_FIELD_4,
           |grp.POS_GRP_End_Customer_RESERVE_FIELD_5,
           |coalesce(grp.end_cust_duns,pdw.end_user_duns_id) as end_cust_duns,
           |grp.end_cust_ent_MDM_BR_typ,
           |grp.end_cust_ent_MDM_prty,
           |coalesce(grp.end_cust_MDCP_BR_typ,pdw.end_user_mdcp_br_type_cd) as end_cust_MDCP_BR_typ,
           |coalesce(grp.end_cust_MDCP_BRI_id,pdw.end_user_mdcp_br_item_id) as end_cust_MDCP_BRI_id,
           |coalesce(grp.end_cust_MDCP_LOC_id,pdw.end_user_mdcp_locator_id) as end_cust_MDCP_LOC_id,
           |coalesce(grp.end_cust_MDCP_opsi_id,pdw.end_user_mdcp_opsi_id) as end_cust_MDCP_opsi_id,
           |coalesce(grp.end_cust_MDCP_ORG_id,pdw.end_user_mdcp_org_id) as end_cust_MDCP_ORG_id,
           |grp.end_cust_ptnr_pro_id,
           |grp.enttld_ent_RAWADDR_id,
           |grp.enttld_ent_RAWADDR_KEY,
           |grp.POS_GRP_enttld_ent_RESERVE_FIELD_1,
           |grp.POS_GRP_enttld_ent_RESERVE_FIELD_2,
           |grp.POS_GRP_enttld_ent_RESERVE_FIELD_3,
           |grp.POS_GRP_enttld_ent_RESERVE_FIELD_4,
           |grp.POS_GRP_enttld_ent_RESERVE_FIELD_5,
           |grp.enttld_duns,
           |grp.enttld_ent_MDM_BR_typ,
           |grp.enttld_ent_MDM_prty_id,
           |grp.enttld_MDCP_BR_typ,
           |grp.enttld_MDCP_BRI_id,
           |grp.enttld_MDCP_LOC_id,
           |grp.enttld_MDCP_opsi_id,
           |grp.enttld_MDCP_ORG_id,
           |grp.enttld_ptnr_pro_id,
           |ps.backend_deal_1_ln_itm,
           |ps.backend_deal_1_mc_cd,
           |ps.backend_deal_1_vrsn,
           |ps.backend_deal_2_ln_itm_cd,
           |ps.backend_deal_2_m_cd,
           |ps.backend_deal_2_vrsn,
           |ps.backend_deal_3_ln_itm,
           |ps.backend_deal_3_mc_cd,
           |ps.backend_deal_3_vrsn,
           |ps.backend_deal_4_ln_itm,
           |ps.backend_deal_4_mc_cd,
           |ps.backend_deal_4_vrsn,
           |ps.ndp_coefficient,
           |ps.ndp_estimated_lcy_ext,
           |ps.ndp_estimated_lcy_unit,
           |ps.ndp_estimated_usd_ext,
           |ps.ndp_estimated_usd_unit,
           |ps.hpe_order,
           |ps.hpe_order_item_mcc,
           |ps.hpe_order_item_number,
           |ps.hpe_order_date,
           |ps.hpe_order_item_disc_lcy_ext,
           |ps.hpe_order_item_disc_usd_ext,
           |ps.hpe_upfront_deal,
           |ps.hpe_order_item_cleansed_deal,
           |ps.hpe_upfront_deal_type,
           |ps.hpe_order_item_orig_deal,
           |ps.hpe_upfront_disc_lcy_ext,
           |ps.hpe_upfront_disc_usd_ext,
           |ps.hpe_order_end_user_addr,
           |ps.hpe_upfront_deal_end_user_addr,
           |ps.upfront_deal_1_type,
           |ps.upfront_deal_1_end_user_addr,
           |ps.backend_deal_1_type,
           |ps.backend_deal_1_end_user_addr,
           |ps.partner_purchase_price_lcy_ext,
           |ps.partner_purchase_price_lcy_unit,
           |ps.partner_purchase_price_usd_ext,
           |ps.partner_purchase_price_usd_unit,
           |ps.net_cost_after_rebate_lcy_ext,
           |ps.net_cost_after_rebate_usd_ext,
           |ps.net_cost_currency_code,
           |ps.rebate_amount_lcy_ext,
           |ps.rebate_amount_lcy_unit,
           |ps.rebate_amount_usd_ext,
           |ps.rebate_amount_usd_unit,
           |ps.partner_sell_price_lcy_ext,
           |ps.partner_sell_price_lcy_unit,
           |ps.partner_sell_price_usd_ext,
           |ps.partner_sell_price_usd_unit,
           |ps.backend_deal_1_dscnt_lcy_ext,
           |ps.backend_deal_1_dscnt_lcy_pr_unt,
           |ps.backend_deal_1_dscnt_usd_ext,
           |ps.backend_deal_1_dscnt_usd_pr_unt,
           |ps.backend_deal_1_lcy_unt_prc,
           |ps.backend_deal_1_usd_unt_prc,
           |ps.backend_deal_2_dscnt_lcy_ext,
           |ps.backend_deal_2_dscnt_lcy_pr_unt,
           |ps.backend_deal_2_dscnt_usd_ext,
           |ps.backend_deal_2_dscnt_usd_pr_unt,
           |ps.backend_deal_2_lcy_unt_prc,
           |ps.backend_deal_2_usd_unt_prc,
           |ps.backend_deal_3_dscnt_lcy_ext,
           |ps.backend_deal_3_dscnt_lcy_pr_unt,
           |ps.backend_deal_3_dscnt_usd_ext,
           |ps.backend_deal_3_dscnt_usd_pr_unt,
           |ps.backend_deal_3_lcy_unt_prc,
           |ps.backend_deal_3_usd_unt_prc,
           |ps.backend_deal_4_dscnt_lcy_ext,
           |ps.backend_deal_4_dscnt_lcy_pr_unt,
           |ps.backend_deal_4_dscnt_usd_ext,
           |ps.backend_deal_4_dscnt_usd_pr_unt,
           |ps.backend_deal_4_lcy_unt_prc,
           |ps.backend_deal_4_usd_unt_prc,
           |ps.backend_rbt_1_lcy_pr_unt,
           |ps.backend_rbt_1_usd_pr_unt,
           |ps.backend_rbt_2_lcy_pr_unt,
           |ps.backend_rbt_2_usd_pr_unt,
           |ps.backend_rbt_3_lcy_pr_unt,
           |ps.backend_rbt_3_usd_pr_unt,
           |ps.backend_rbt_4_lcy_pr_unt,
           |ps.backend_rbt_4_usd_pr_unt,
           |ps.prch_agrmnt_dscnt_lcy_unt,
           |ps.prch_agrmnt_dscnt_usd_unt,
           |ps.sls_asp_lcy,
           |ps.sls_asp_lcy_unt_prc,
           |ps.sls_asp_usd,
           |ps.sls_asp_usd_unt_prc,
           |ps.sls_avrg_deal_nt_lcy,
           |ps.sls_avrg_deal_nt_lcy_unt_prc,
           |ps.sls_avrg_deal_nt_usd,
           |ps.sls_avrg_deal_nt_usd_unt_prc,
           |ps.sls_avrg_deal_nt_vcy,
           |ps.sls_avrg_deal_nt_vcy_unt_prc,
           |ps.sls_deal_nt_lcy,
           |ps.sls_deal_nt_lcy_unt_prc,
           |ps.sls_deal_nt_usd,
           |ps.sls_deal_nt_usd_unt_prc,
           |ps.sls_deal_nt_vcy,
           |ps.sls_deal_nt_vcy_unt_prc,
           |ps.sls_lst_lcy,
           |ps.sls_lst_lcy_unt_prc,
           |ps.sls_lst_usd,
           |ps.sls_lst_usd_unt_prc,
           |ps.sls_lst_vcy,
           |ps.sls_lst_vcy_unt_prc,
           |ps.sls_ndp_lcy,
           |ps.sls_ndp_lcy_unt_prc,
           |ps.sls_ndp_usd,
           |ps.sls_ndp_usd_unt_prc,
           |ps.sls_ndp_vcy,
           |ps.sls_ndp_vcy_unt_prc,
           |ps.upfrnt_deal_1_dscnt_lcy,
           |ps.upfrnt_deal_1_dscnt_lcy_pr_unt,
           |ps.upfrnt_deal_1_dscnt_usd,
           |ps.upfrnt_deal_1_dscnt_usd_pr_unt,
           |ps.upfrnt_deal_1_lcy_unt_prc,
           |ps.upfrnt_deal_1_usd_unt_prc,
           |ps.upfrnt_deal_2_dscnt_lcy,
           |ps.upfrnt_deal_2_dscnt_lcy_pr_unt,
           |ps.upfrnt_deal_2_dscnt_usd,
           |ps.upfrnt_deal_2_dscnt_usd_pr_unt,
           |ps.upfrnt_deal_2_lcy_unt_prc,
           |ps.upfrnt_deal_2_usd_unt_prc,
           |ps.upfrnt_deal_3_dscnt_lcy_ext,
           |ps.upfrnt_deal_3_dscnt_lcy_pr_unt,
           |ps.upfrnt_deal_3_dscnt_usd_ext,
           |ps.upfrnt_deal_3_dscnt_usd_pr_unt,
           |ps.upfrnt_deal_3_lcy_unt_prc,
           |ps.upfrnt_deal_3_usd_unt_prc,
           |ps.pos_prs_iud_flag,
           |ps.pos_prs_reserve_field_1,
           |ps.pos_prs_reserve_field_2,
           |ps.pos_prs_reserve_field_3,
           |ps.pos_prs_reserve_field_4,
           |ps.pos_prs_reserve_field_5,
           |ps.pos_prs_reserve_field_6,
           |ps.pos_prs_reserve_field_7,
           |ps.pos_prs_reserve_field_8,
           |ps.pos_prs_reserve_field_9,
           |ps.pos_prs_reserve_field_10,
           |ps.upfrnt_deal_1_ln_itm,
           |ps.upfrnt_deal_1_mc_cd,
           |ps.upfrnt_deal_1_vrsn,
           |ps.upfrnt_deal_2_ln_itm,
           |ps.upfrnt_deal_2_mc_cd,
           |ps.upfrnt_deal_2_vrsn,
           |ps.upfrnt_deal_3_ln_itm,
           |ps.upfrnt_deal_3_mc_cd,
           |ps.upfrnt_deal_3_vrsn,
           |ps.asp_coefficient,
           |ps.avrg_deal_nt_coefficient,
           |ps.deal_geo,
           |ps.dscnt_geo,
           |ps.ex_rate_pcy_to_usd,
           |ps.ex_rate_lcy_to_usd,
           |ps.ex_rate_vcy_to_usd,
           |ps.lcl_curr_cd,
           |ps.valtn_curr_cd,
           |ps.valtn_cust_appl_cd,
           |ps.valtn_prch_agrmnt_dscnt,
           |ps.valtn_prch_agrmnt,
           |ps.valtn_prc_dsc,
           |ps.valtn_trsn_id,
           |ps.valtn_wrnng_cd,
           |ps.e2open_fl_id_valtn_rspns,
           |ps.prnt_trsn_id_valtn_rspns,
           |ps.prv_trsn_id_valtn_rspns,
           |ps.reporter_id_valtn_rspns,
           |ps.trsn_id_valtn_rspns,
           |ps.ptnr_rptd_prc_lcy,
           |ps.ptnr_rptd_prc_usd,
           |ps.sls_bs_qty,
           |ps.sls_opt_qty,
           |ps.sls_real_unt_qty,
           |ps.src_sys_upd_ts,
           |ps.src_sys_ky,
           |ps.lgcl_dlt_ind,
           |ps.ins_gmt_ts,
           |ps.upd_gmt_ts,
           |ps.src_sys_extrc_gmt_ts,
           |ps.src_sys_btch_nr,
           |ps.fl_nm,
           |ps.ld_jb_nr,
           |ps.deal_ind,
           |ps.deal_ind_grp,
           |ps.bs_prod_nr,
           |CASE WHEN ps.bi_trsn_xclsn_flg IS NOT NULL THEN ps.bi_trsn_xclsn_flg ELSE 'N' END AS bi_trsn_xclsn_flg,
           |CASE WHEN ps.bi_trsn_xclsn_rsn_cd IS NOT NULL THEN ps.bi_trsn_xclsn_rsn_cd ELSE 'N' END AS bi_trsn_xclsn_rsn_cd,
           |ps.prod_opt,
           |ps.vol_sku_flg,
           |NULL AS bi_cto_bto_flg,
           |NULL AS bi_cto_bto_qty_mtch_flg,
           |ps.bi_rslr_ctr_cd,
           |ps.bi_dstr_ctr_cd,
           |ps.bi_rptr_ctr_cd,
           |ps.bi_enttld_ctry_cd,
           |ps.bi_end_cust_ctry_cd,
           |ps.mc_cd,
           |ps.mc_cd_dn,
           |ps.upfrnt_deal_1_mc_dn_cd,
           |ps.upfrnt_deal_2_mc_dn_cd,
           |ps.upfrnt_deal_3_mc_dn_cd,
           |ps.backend_deal_1_mc_dn_cd,
           |ps.backend_deal_2_mc_dn_cd,
           |ps.backend_deal_3_mc_dn_cd,
           |ps.backend_deal_4_mc_dn_cd,
           |ps.ps_grpg_E2Open_extrt_nm,
           |ps.ps_grpg_E2Open_seq_ID,
           |ps.ps_grpg_E2Open_extrt_dt,
           |ps.ps_grpg_E2Open_extrt_ts,
           |ps.ps_E2Open_extrt_nm,
           |ps.ps_E2Open_seq_ID,
           |ps.ps_E2Open_extrt_dt,
           |ps.ps_E2Open_extrt_ts,
           |ps.ps_prs_E2Open_extrt_nm,
           |ps.ps_prs_E2Open_seq_ID,
           |ps.ps_prs_E2Open_extrt_dt,
           |ps.ps_prs_E2Open_extrt_ts,
           |ps.ps_grpg_EAP_load_dt,
           |ps.ps_grpg_EAP_load_ts,
           |ps.ps_EAP_load_dt,
           |ps.ps_EAP_load_ts,
           |ps.ps_prs_EAP_load_dt,
           |ps.ps_prs_EAP_load_ts,
           |ps.ps_rwaddr_E2Open_extrt_nm,
           |ps.ps_rwaddr_E2Open_seq_ID,
           |ps.ps_rwaddr_E2Open_extrt_dt,
           |ps.ps_rwaddr_E2Open_extrt_ts,
           |ps.ps_rwaddr_EAP_load_dt,
           |ps.ps_rwaddr_EAP_load_ts,
           |ps.hpe_invoice_dt,
           |ps.hpe_ord_bundle_id,
           |ps.hpe_ord_deal_id,
           |ps.hpe_obj_srvc_mtrl_id,
           |ps.hpe_ord_item_nr,
           |ps.hpe_ord_mc_cd,
           |ps.hpe_ord_pft_cntr,
           |ps.hpe_ord_sales_dvsn_cd,
           |ps.hpe_shpmt_dt,
           |ps.hpe_ord_dt,
           |ps.hpe_ord_drp_shp_flg,
           |ps.hpe_ord_end_cust_prty_id,
           |ps.hpe_ord_nr,
           |ps.hpe_ord_rtm,
           |ps.hpe_ord_sld_br_typ,
           |ps.hpe_ord_sld_pa_nr,
           |ps.hpe_ord_sld_prty_id,
           |ps.hpe_ord_tier2_rslr_br_typ,
           |ps.hpe_ord_tier2_rslr_prty_id,
           |ps.hpe_ord_typ,
           |ps.ope_rtm
           |from ea_support_temp_an.chnlptnr_tmp_ps_fact PS
           |LEFT JOIN ea_support_temp_an.chnlptnr_tmp_Grouping grp on ps.trsn_id=grp.trsn_id
           |LEFT JOIN ea_common.dwtf_sl_thru_ref pdw ON Cast(regexp_replace(PDW.trans_id_zyme_trans_id,'-','') as bigint)=PS.trsn_id
           |""".stripMargin).repartition($"trsn_id")
      //chnlptnrPosFactGrpgDf.createOrReplaceTempView("chnlptnrPosFactGrpgDf")
      var chnlptnr_pos_fact_grpg_load = chnlptnrPosFactGrpgDf.coalesce(20).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "chnlptnr_tmp_ps_fact_grpg")
      logger.info("******chnlptnr_tmp_ps_fact_grpg Loaded*****")
	  
      // CTO BTO Logic Implementation
	    val POSFactDF = spark.sql(s"""select sls_qty,dt,bs_prod_nr,reporter_id,trsn_id,sld_to_rptd_id,end_usr_rptd_id,bi_cto_bto_flg,prod_opt from ea_support_temp_an.chnlptnr_tmp_ps_fact """)
      POSFactDF.createOrReplaceTempView("POSFactDF")

      val POSFactDF_CTO_BTO = spark.sql(
        s"""
           |select
           |  ps.sls_qty,ps.dt,ps.bs_prod_nr,ps.reporter_id,ps.trsn_id,ps.sld_to_rptd_id,ps.end_usr_rptd_id,ps.prod_opt,
           |CASE WHEN ps.bi_cto_bto_flg='N/A' THEN ps.bi_cto_bto_flg
           | WHEN ps_bto.sls_qty = ps_cto.sls_qty and
           | ps_bto.bto_rw_num=ps_cto.cto_rw_num THEN 'CTO' ELSE ps.bi_cto_bto_flg
           | END AS bi_cto_bto_flg,
           |CASE WHEN ps_bto.sls_qty = ps_cto.sls_qty and ps_bto.bto_rw_num=ps_cto.cto_rw_num THEN 'Y' ELSE 'N' END AS bi_cto_bto_qty_mtch_flg
           |from POSFactDF ps
           |LEFT JOIN (select sls_qty,dt,bs_prod_nr,reporter_id,trsn_id,sld_to_rptd_id,end_usr_rptd_id,row_number()
           |over (partition by dt,bs_prod_nr,reporter_id,sls_qty,bi_cto_bto_flg,sld_to_rptd_id,end_usr_rptd_id order by sls_qty) as rw_num from POSFactDF) ps_prime ON ps.trsn_id=ps_prime.trsn_id
           |LEFT JOIN (select sls_qty,dt,bs_prod_nr,reporter_id,trsn_id,sld_to_rptd_id,end_usr_rptd_id,row_number()
           |over (partition by dt,reporter_id,bs_prod_nr,sls_qty,sld_to_rptd_id,end_usr_rptd_id order by sls_qty) as cto_rw_num from POSFactDF
           |where bi_cto_bto_flg = 'CTO' ) ps_cto ON ps_cto.sls_qty=ps_prime.sls_qty and ps_cto.dt=ps_prime.dt and ps_cto.bs_prod_nr=ps_prime.bs_prod_nr
           |and ps_cto.reporter_id=ps_prime.reporter_id and
           | ps_cto.sld_to_rptd_id=ps_prime.sld_to_rptd_id and
           | ps_cto.end_usr_rptd_id=ps_prime.end_usr_rptd_id and
           |ps_cto.cto_rw_num=ps_prime.rw_num
           |LEFT JOIN (select sls_qty,dt,bs_prod_nr,reporter_id,trsn_id,sld_to_rptd_id,end_usr_rptd_id,row_number() over (partition by dt,reporter_id,bs_prod_nr,sls_qty,
           |sld_to_rptd_id,end_usr_rptd_id  order by sls_qty) as bto_rw_num from POSFactDF where bi_cto_bto_flg = 'BTO' ) ps_bto ON ps_cto.sls_qty=ps_bto.sls_qty
           |and ps_cto.dt=ps_bto.dt and ps_cto.bs_prod_nr=ps_bto.bs_prod_nr and ps_cto.reporter_id=ps_bto.reporter_id and
           |ps_cto.sld_to_rptd_id=ps_bto.sld_to_rptd_id and
           |ps_cto.end_usr_rptd_id=ps_bto.end_usr_rptd_id and
           |ps_cto.cto_rw_num=ps_bto.bto_rw_num
           |""".stripMargin)

      POSFactDF_CTO_BTO.createOrReplaceTempView("POSFactDF_CTO_BTO")

      val POSFactDF_CTO_BTO_Final = spark.sql(
        s"""
           |select ps.trsn_id,
           |CASE WHEN ps_bto.cumulative_sum <= ps_cto.cto_sum THEN 'CTO'
           |WHEN lag(ps_bto.cumulative_sum,1) over (partition by ps.dt,ps.bs_prod_nr,ps.reporter_id,ps.sld_to_rptd_id,ps.end_usr_rptd_id
           |order by ps.sls_qty,ps.trsn_id) is NULL and ps_bto.cumulative_sum > ps_cto.cto_sum THEN 'Mixed' WHEN  lag(ps_bto.cumulative_sum,1)
           |over (partition by ps.dt,ps.bs_prod_nr,ps.reporter_id,ps.sld_to_rptd_id,ps.end_usr_rptd_id order by ps.sls_qty,ps.trsn_id) < ps_cto.cto_sum
           |and ps_bto.cumulative_sum > ps_cto.cto_sum THEN 'Mixed' ELSE ps.bi_cto_bto_flg END AS bi_cto_bto_flg
           |from POSFactDF_CTO_BTO ps
           |LEFT JOIN (select sum(sls_qty) as cto_sum,dt,bs_prod_nr,reporter_id, sld_to_rptd_id,end_usr_rptd_id from POSFactDF_CTO_BTO
           |where bi_cto_bto_flg='CTO' and bi_cto_bto_qty_mtch_flg = 'N' group by dt,bs_prod_nr,reporter_id,sld_to_rptd_id,end_usr_rptd_id) ps_cto
           |ON ps_cto.bs_prod_nr=ps.bs_prod_nr and ps_cto.reporter_id=ps.reporter_id and ps_cto.dt=ps.dt and ps_cto.end_usr_rptd_id=ps.end_usr_rptd_id
           |and ps_cto.sld_to_rptd_id=ps.sld_to_rptd_id
           |LEFT JOIN (select trsn_id,sum(sls_qty) over (partition by dt,bs_prod_nr,reporter_id,sld_to_rptd_id,end_usr_rptd_id order by sls_qty,trsn_id ROWS
           |BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sum from POSFactDF_CTO_BTO where bi_cto_bto_flg = 'BTO' and bi_cto_bto_qty_mtch_flg='N'
           |and prod_opt is NULL) ps_bto ON ps.trsn_id=ps_bto.trsn_id
           |""".stripMargin)
    
	    val POSCTOBTODFLoadStatus = POSFactDF_CTO_BTO_Final.coalesce(10).write.mode("overwrite").format("orc").insertInto("ea_support_temp_an" + "." + "POS_cto_bto_fact")
      logger.info("#######POS_cto_bto_fact CTO BTO Fact Load is completed " + POSCTOBTODFLoadStatus)

      var chnlptnrPsFact = spark.sql(
        s"""
           |select distinct PS.src_sys_nm,
           |PS.rec_src,
           |trim(PS.backend_deal_id_1) as backend_deal_id_1,
           |trim(PS.rptd_backend_deal_1) as rptd_backend_deal_1,
           |trim(PS.backend_deal_id_2) as backend_deal_id_2,
           |trim(PS.rptd_backend_deal_2) as rptd_backend_deal_2,
           |trim(PS.backend_deal_id_3) as backend_deal_id_3,
           |trim(PS.rptd_backend_deal_3) as rptd_backend_deal_3,
           |trim(PS.backend_deal_id_4) as backend_deal_id_4,
           |trim(PS.rptd_backend_deal_4) as rptd_backend_deal_4,
           |PS.bll_to_asian_addr,
           |PS.bll_to_co_tx_id,
           |PS.bll_to_co_tx_id_enr,
           |PS.bll_to_cntct_nm,
           |PS.bll_to_ctry_rptd,
           |PS.bll_to_ky,
           |PS.bll_to_ph_nr,
           |PS.bll_to_rw_addr_id,
           |PS.bll_to_rptd_id,
           |PS.nmso_name,
           |PS.nmso_date,
           |PS.standing_offer_number,
           |PS.deal_reg_1,
           |PS.rptd_deal_reg_1,
           |PS.deal_reg_2,
           |PS.rptd_deal_reg_2,
           |PS.nt_cst_aftr_rbt_lcy,
           |PS.ptnr_prch_lcy_unt_prc,
           |PS.ptnr_sl_lcy_unt_prc,
           |PS.ptnr_sl_usd_unt_prc,
           |PS.sls_bndl_qty,
           |PS.sls_qty,
           |PS.pos_iud_flag,
           |PS.pos_reserve_field_1,
           |PS.pos_reserve_field_2,
           |PS.rptg_prd_strt_dt,
           |PS.rptg_prd_end_dt,
           |PS.pos_reserve_field_5,
           |PS.pos_reserve_field_6,
           |PS.pos_reserve_field_7,
           |PS.pos_reserve_field_8,
           |PS.pos_reserve_field_9,
           |PS.pos_reserve_field_10,
           |PS.bndl_prod_id,
           |PS.orgl_prod_ln_id,
           |PS.ptnr_bndl_prod_id,
           |PS.ptnr_inrn_prod_nr,
           |trim(PS.prod_nr) as prod_nr,
           |PS.rptd_prod_nr,
           |PS.vl_vol_flg,
           |PS.reporter_id,
           |PS.sl_frm_cntct_nm,
           |PS.sl_frm_ctry_rptd,
           |PS.sl_frm_ky,
           |PS.sl_frm_ph_nr,
           |PS.sl_frm_rw_addr_id,
           |PS.sl_frm_rptd_id,
           |PS.shp_frm_cntct_nm,
           |PS.shp_frm_ctry_rptd,
           |PS.shp_frm_ky,
           |PS.shp_frm_ph_nr,
           |PS.shp_frm_rw_addr_id,
           |PS.shp_frm_rptd_id,
           |PS.shp_to_asian_addr,
           |PS.shp_to_co_tx_id,
           |PS.shp_to_co_tx_id_enr,
           |PS.shp_to_cntct_nm,
           |PS.shp_to_ctry_rptd,
           |PS.shp_to_ky,
           |PS.shp_to_ph_nr,
           |PS.shp_to_rw_addr_id,
           |PS.shp_to_rptd_id,
           |PS.sld_to_asian_addr,
           |PS.sld_to_co_tx_id,
           |PS.sld_to_co_tx_id_enr,
           |PS.sld_to_cntct_nm,
           |PS.sld_to_ctry_rptd,
           |PS.sld_to_ky,
           |PS.sld_to_ph_nr,
           |PS.sld_to_rw_addr_id,
           |PS.sld_to_rptd_id,
           |PS.end_usr_addr_src_rptd,
           |PS.end_usr_asian_addr,
           |PS.end_usr_co_tx_id,
           |PS.end_usr_co_tx_id_enr,
           |PS.end_usr_cntct_nm,
           |PS.end_usr_ctry_rptd,
           |PS.end_usr_ky,
           |PS.end_usr_ph_nr,
           |PS.end_usr_rw_addr_id,
           |PS.end_usr_rptd_id,
           |PS.enttld_asian_addr,
           |PS.enttld_co_tx_id,
           |PS.enttld_co_tx_id_enr,
           |PS.enttld_cntct_nm,
           |PS.enttld_ctry_rptd,
           |PS.enttld_ky,
           |PS.enttld_ph_nr,
           |PS.enttld_rw_addr_id,
           |PS.enttld_rptd_id,
           |PS.agt_flg,
           |PS.brm_err_flg,
           |PS.crss_shp_flg,
           |PS.cust_to_ptnr_po_nr,
           |PS.dt,
           |PS.drp_shp_flg,
           |PS.e2open_fl_id,
           |PS.e2open_fl_rcvd_dt,
           |PS.hpe_inv_nr,
           |PS.nt_cst_aftr_rbt_curr_cd,
           |PS.org_sls_nm,
           |PS.orgl_hpe_asngd_trsn_id,
           |PS.orgl_trsn_id,
           |PS.prnt_trsn_id,
           |PS.ptnr_inrn_trsn_id,
           |PS.ptnr_inv_dt,
           |PS.ptnr_inv_nr,
           |PS.ptnr_prch_curr_cd,
           |PS.ptnr_sls_rep_nm,
           |PS.ptnr_sl_prc_curr_cd,
           |PS.ptnr_sm_dt,
           |PS.ptnr_to_hpe_po_nr,
           |PS.prv_trsn_id,
           |PS.prod_orgn_cd,
           |PS.prchg_src_ws_ind,
           |PS.splyr_iso_ctry_cd,
           |PS.tty_mgr_cd,
           |PS.trsn_id,
           |PS.vldtn_wrnng_cd,
           |PS.rptd_upfrnt_deal_1,
           |trim(PS.upfrnt_deal_id_1_id) as upfrnt_deal_id_1_id,
           |PS.rptd_upfrnt_deal_2,
           |trim(PS.upfrnt_deal_id_2) as upfrnt_deal_id_2,
           |PS.rptd_upfrnt_deal_3,
           |trim(PS.upfrnt_deal_id_3_id) as upfrnt_deal_id_3_id,
           |PS.ope_bmt_dstr_bsn_rshp_typ,
           |PS.ope_bmt_dstr_prty_id,
           |PS.ope_bmt_dstr_sls_comp_rlvnt,
           |PS.ope_dstr_bsn_rshp_typ,
           |PS.ope_dstr_prty_id,
           |PS.ope_dstr_rsn_cd,
           |PS.ope_bmt_end_cust_bsn_rshp_typ,
           |PS.ope_bmt_end_cust_prty_id,
           |PS.ope_bmt_end_cust_sls_comp_rlvnt,
           |PS.ope_deal_end_cust_bsn_rshp_typ,
           |PS.ope_deal_end_cust_prty_id,
           |PS.ope_end_cust_bsn_rshp_typ,
           |PS.ope_end_cust_prty_id,
           |PS.ope_end_cust_rsn_cd,
           |PS.ope_lvl_3_bsn_rshp_typ,
           |PS.ope_lvl_3_prty_id,
           |PS.ope_lvl_3_rshp,
           |PS.ope_src_enttld_prty_ctry,
           |PS.ope_src_enttld_prty_prty_id,
           |PS.ope_src_end_cust_ctry,
           |PS.ope_src_end_cust_prty_id,
           |PS.ope_enttld_prty_ctry,
           |PS.ope_entitlted_prty_id,
           |PS.ope_dstr_bsn_mgr_rptg_dstr,
           |PS.ope_dstr_rptg_dstr,
           |PS.ope_end_cust_sls_rep_rptg_dstr,
           |PS.ope_flipped_dstr_bsn_mgr_vw_flg,
           |PS.ope_flipped_dstr_vw_flg,
           |PS.ope_flipped_end_cust_vw_flg,
           |PS.ope_flipped_ptnr_bsn_mgr_vw_flg,
           |PS.ope_flipped_rslr_vw_flg,
           |PS.ope_lvl_1_bsn_rshp_typ,
           |PS.ope_lvl_1_prty_id,
           |PS.ope_lvl_1_rshp,
           |PS.ope_ptnr_bsn_mgr_rptg_dstr,
           |PS.ope_rslr_rptg_dstr,
           |PS.ope_bmt_rslr_bsn_rshp_typ,
           |PS.ope_bmt_rslr_prty_id,
           |PS.ope_bmt_rslr_sls_comp_rlvnt,
           |PS.ope_rslr_bsn_rshp_typ,
           |PS.ope_rslr_prty_id,
           |PS.ope_rslr_rsn_cd,
           |PS.ope_lvl_2_bsn_rshp_typ,
           |PS.ope_lvl_2_prty_id,
           |PS.ope_lvl_2_rshp,
           |PS.ope_sld_to_br_typ,
           |PS.ope_sld_to_ctry,
           |PS.ope_sld_to_prty_id,
           |PS.ope_big_deal_xclsn,
           |PS.ope_crss_brdr,
           |PS.ope_crss_srcd,
           |PS.ope_dstr_rptg_dstr_to_another_rptg_dstr,
           |PS.ope_dstr_bookings_comp_vw,
           |PS.ope_dstr_bookings_vw,
           |PS.ope_dstr_bsn_mgr_bookings_vw,
           |PS.ope_dstr_bsn_mgr_comp_vw,
           |PS.ope_drp_shp,
           |PS.ope_emb_srv_ln_itms,
           |PS.ope_end_cust_rptg_dstr_to_another_rptg_dstr,
           |PS.ope_end_cust_sls_rep_bookings_vw,
           |PS.ope_end_cust_sls_rep_comp_vw,
           |PS.ope_fdrl_bsn,
           |PS.ope_ptnr_bsn_mgr_bookings_vw,
           |PS.ope_ptnr_bsn_mgr_comp_vw,
           |PS.ope_pft_cntr,
           |PS.ope_rslr_bookings_vw,
           |PS.ope_rslr_comp_vw,
           |PS.ope_srv_ln_itm,
           |PS.ope_trsn_id,
           |PS.ope_reporter_prty_id,
           |PS.ope_br_ty_1_cd,
           |PS.rptr_prty_ky,
           |PS.bll_to_raw_address_id,
           |PS.bll_to_lctn_ctry_cd,
           |PS.bll_to_lctn_addr_1,
           |PS.bll_to_lctn_addr_2,
           |PS.bll_to_lctn_cty_nm,
           |PS.bll_to_lctn_nm,
           |PS.bll_to_lctn_pstl_cd,
           |PS.bll_to_lctn_st_cd,
           |PS.sell_from_raw_address_id,
           |PS.sl_frm_lctn_ctry_cd,
           |PS.sl_frm_lctn_addr_1,
           |PS.sl_frm_lctn_addr_2,
           |PS.sl_frm_lctn_cty_nm,
           |PS.sl_frm_lctn_nm,
           |PS.sl_frm_lctn_pstl_cd,
           |PS.sl_frm_lctn_st_cd,
           |PS.ship_to_raw_address_id,
           |PS.shp_to_lctn_ctry_cd,
           |PS.shp_to_lctn_addr_1,
           |PS.shp_to_lctn_addr_2,
           |PS.shp_to_lctn_cty_nm,
           |PS.shp_to_lctn_nm,
           |PS.shp_to_lctn_pstl_cd,
           |PS.shp_to_lctn_st_cd,
           |PS.ship_from_raw_address_id,
           |PS.shp_frm_lctn_ctry_cd,
           |PS.shp_frm_lctn_addr_1,
           |PS.shp_frm_lctn_addr_2,
           |PS.shp_frm_lctn_cty_nm,
           |PS.shp_frm_lctn_nm,
           |PS.shp_frm_lctn_pstl_cd,
           |PS.shp_frm_lctn_st_cd,
           |PS.sold_to_raw_address_id,
           |PS.sld_to_lctn_ctry_cd,
           |PS.sld_to_lctn_addr_1,
           |PS.sld_to_lctn_addr_2,
           |PS.sld_to_lctn_cty_nm,
           |PS.sld_to_lctn_nm,
           |PS.sld_to_lctn_pstl_cd,
           |PS.sld_to_lctn_st_cd,
           |PS.end_cust_raw_address_id,
           |PS.end_cust_lctn_ctry_cd,
           |PS.end_cust_lctn_addr_1,
           |PS.end_cust_lctn_addr_2,
           |PS.end_cust_lctn_cty_nm,
           |PS.end_cust_lctn_nm,
           |PS.end_cust_lctn_pstl_cd,
           |PS.end_cust_lctn_st_cd,
           |PS.enttld_lctn_raw_address_id,
           |PS.enttld_lctn_ctry_cd,
           |PS.enttld_lctn_addr_1,
           |PS.enttld_lctn_addr_2,
           |PS.enttld_lctn_cty_nm,
           |PS.enttld_lctn_nm,
           |PS.enttld_lctn_pstl_cd,
           |PS.enttld_lctn_st_cd,
           |PS.bll_to_duns,
           |PS.bll_to_ent_mdm_br_typ,
           |PS.bll_to_ent_mdm_prty_id,
           |PS.bll_to_mdcp_br_typ,
           |PS.bll_to_mdcp_bri_id,
           |PS.bll_to_mdcp_loc_id,
           |PS.bll_to_mdcp_opsi_id,
           |PS.bll_to_mdcp_org_id,
           |PS.bll_to_ptnr_pro_id,
           |PS.bill_to_rawaddr_id,
           |PS.bill_to_rawaddr_key,
           |PS.pos_grp_bill_to_reserve_field_1,
           |PS.pos_grp_bill_to_reserve_field_2,
           |PS.pos_grp_bill_to_reserve_field_3,
           |PS.pos_grp_bill_to_reserve_field_4,
           |PS.pos_grp_bill_to_reserve_field_5,
           |PS.sl_frm_duns,
           |PS.sl_frm_ent_mdm_br_typ,
           |PS.sl_frm_ent_mdm_prty_id,
           |PS.sl_frm_mdcp_br_typ,
           |PS.sl_frm_mdcp_bri_id,
           |PS.sl_frm_mdcp_loc_id,
           |PS.sl_frm_mdcp_opsi_id,
           |PS.sl_frm_mdcp_org_id,
           |PS.sl_frm_ptnr_pro_id,
           |PS.sell_from_rawaddr_id,
           |PS.sell_from_rawaddr_key,
           |PS.pos_grp_sell_from_reserve_field_1,
           |PS.pos_grp_sell_from_reserve_field_2,
           |PS.pos_grp_sell_from_reserve_field_3,
           |PS.pos_grp_sell_from_reserve_field_4,
           |PS.pos_grp_sell_from_reserve_field_5,
           |PS.ship_from_rawaddr_id,
           |PS.ship_from_rawaddr_key,
           |PS.pos_grp_ship_from_reserve_field_1,
           |PS.pos_grp_ship_from_reserve_field_2,
           |PS.pos_grp_ship_from_reserve_field_3,
           |PS.pos_grp_ship_from_reserve_field_4,
           |PS.pos_grp_ship_from_reserve_field_5,
           |PS.shp_frm_duns,
           |PS.shp_frm_ent_mdm_br_typ,
           |PS.shp_frm_ent_mdm_prty_id,
           |PS.shp_frm_mdcp_br_typ,
           |PS.shp_frm_mdcp_bri_id,
           |PS.shp_frm_mdcp_loc_id,
           |PS.shp_frm_mdcp_opsi_id,
           |PS.shp_frm_mdcp_org_id,
           |PS.shp_frm_ptnr_pro_id,
           |PS.ship_to_rawaddr_id,
           |PS.ship_to_rawaddr_key,
           |PS.pos_grp_ship_to_reserve_field_1,
           |PS.pos_grp_ship_to_reserve_field_2,
           |PS.pos_grp_ship_to_reserve_field_3,
           |PS.pos_grp_ship_to_reserve_field_4,
           |PS.pos_grp_ship_to_reserve_field_5,
           |PS.shp_to_duns,
           |PS.shp_to_ent_mdm_br_typ,
           |PS.shp_to_ent_mdm_prty_id,
           |PS.shp_to_mdcp_br_typ,
           |PS.shp_to_mdcp_bri_id,
           |PS.shp_to_mdcp_loc_id,
           |PS.shp_to_mdcp_opsi_id,
           |PS.shp_to_mdcp_org_id,
           |PS.shp_to_ptnr_pro_id,
           |PS.sold_to_rawaddr_id,
           |PS.sold_to_rawaddr_key,
           |PS.pos_grp_sold_to_reserve_field_1,
           |PS.pos_grp_sold_to_reserve_field_2,
           |PS.pos_grp_sold_to_reserve_field_3,
           |PS.pos_grp_sold_to_reserve_field_4,
           |PS.pos_grp_sold_to_reserve_field_5,
           |PS.sld_to_duns,
           |PS.sld_to_ent_mdm_br_typ,
           |PS.sld_to_ent_mdm_prty_id,
           |PS.sld_to_mdcp_br_typ,
           |PS.sld_to_mdcp_bri_id,
           |PS.sld_to_mdcp_loc_id,
           |PS.sld_to_mdcp_opsi_id,
           |PS.sld_to_mdcp_org_id,
           |PS.sld_to_ptnr_pro_id,
           |PS.end_cust_rawaddr_id,
           |PS.end_cust_rawaddr_key,
           |PS.pos_grp_end_customer_reserve_field_1,
           |PS.pos_grp_end_customer_reserve_field_2,
           |PS.pos_grp_end_customer_reserve_field_3,
           |PS.pos_grp_end_customer_reserve_field_4,
           |PS.pos_grp_end_customer_reserve_field_5,
           |PS.end_cust_duns,
           |PS.end_cust_ent_mdm_br_typ,
           |PS.end_cust_ent_mdm_prty,
           |PS.end_cust_mdcp_br_typ,
           |PS.end_cust_mdcp_bri_id,
           |PS.end_cust_mdcp_loc_id,
           |PS.end_cust_mdcp_opsi_id,
           |PS.end_cust_mdcp_org_id,
           |PS.end_cust_ptnr_pro_id,
           |PS.enttld_ent_rawaddr_id,
           |PS.enttld_ent_rawaddr_key,
           |PS.pos_grp_enttld_ent_reserve_field_1,
           |PS.pos_grp_enttld_ent_reserve_field_2,
           |PS.pos_grp_enttld_ent_reserve_field_3,
           |PS.pos_grp_enttld_ent_reserve_field_4,
           |PS.pos_grp_enttld_ent_reserve_field_5,
           |PS.enttld_duns,
           |PS.enttld_ent_mdm_br_typ,
           |PS.enttld_ent_mdm_prty_id,
           |PS.enttld_mdcp_br_typ,
           |PS.enttld_mdcp_bri_id,
           |PS.enttld_mdcp_loc_id,
           |PS.enttld_mdcp_opsi_id,
           |PS.enttld_mdcp_org_id,
           |PS.enttld_ptnr_pro_id,
           |PS.backend_deal_1_ln_itm,
           |PS.backend_deal_1_mc_cd,
           |PS.backend_deal_1_vrsn,
           |PS.backend_deal_2_ln_itm_cd,
           |PS.backend_deal_2_m_cd,
           |PS.backend_deal_2_vrsn,
           |PS.backend_deal_3_ln_itm,
           |PS.backend_deal_3_mc_cd,
           |PS.backend_deal_3_vrsn,
           |PS.backend_deal_4_ln_itm,
           |PS.backend_deal_4_mc_cd,
           |PS.backend_deal_4_vrsn,
           |PS.ndp_coefficient,
           |PS.ndp_estimated_lcy_ext,
           |PS.ndp_estimated_lcy_unit,
           |PS.ndp_estimated_usd_ext,
           |PS.ndp_estimated_usd_unit,
           |PS.hpe_order,
           |PS.hpe_order_item_mcc,
           |PS.hpe_order_item_number,
           |PS.hpe_order_date,
           |PS.hpe_order_item_disc_lcy_ext,
           |PS.hpe_order_item_disc_usd_ext,
           |PS.hpe_upfront_deal,
           |PS.hpe_order_item_cleansed_deal,
           |PS.hpe_upfront_deal_type,
           |PS.hpe_order_item_orig_deal,
           |PS.hpe_upfront_disc_lcy_ext,
           |PS.hpe_upfront_disc_usd_ext,
           |PS.hpe_order_end_user_addr,
           |PS.hpe_upfront_deal_end_user_addr,
           |PS.upfront_deal_1_type,
           |PS.upfront_deal_1_end_user_addr,
           |PS.backend_deal_1_type,
           |PS.backend_deal_1_end_user_addr,
           |PS.partner_purchase_price_lcy_ext,
           |PS.partner_purchase_price_lcy_unit,
           |PS.partner_purchase_price_usd_ext,
           |PS.partner_purchase_price_usd_unit,
           |PS.net_cost_after_rebate_lcy_ext,
           |PS.net_cost_after_rebate_usd_ext,
           |PS.net_cost_currency_code,
           |PS.rebate_amount_lcy_ext,
           |PS.rebate_amount_lcy_unit,
           |PS.rebate_amount_usd_ext,
           |PS.rebate_amount_usd_unit,
           |PS.partner_sell_price_lcy_ext,
           |PS.partner_sell_price_lcy_unit,
           |PS.partner_sell_price_usd_ext,
           |PS.partner_sell_price_usd_unit,
           |PS.backend_deal_1_dscnt_lcy_ext,
           |PS.backend_deal_1_dscnt_lcy_pr_unt,
           |PS.backend_deal_1_dscnt_usd_ext,
           |PS.backend_deal_1_dscnt_usd_pr_unt,
           |PS.backend_deal_1_lcy_unt_prc,
           |PS.backend_deal_1_usd_unt_prc,
           |PS.backend_deal_2_dscnt_lcy_ext,
           |PS.backend_deal_2_dscnt_lcy_pr_unt,
           |PS.backend_deal_2_dscnt_usd_ext,
           |PS.backend_deal_2_dscnt_usd_pr_unt,
           |PS.backend_deal_2_lcy_unt_prc,
           |PS.backend_deal_2_usd_unt_prc,
           |PS.backend_deal_3_dscnt_lcy_ext,
           |PS.backend_deal_3_dscnt_lcy_pr_unt,
           |PS.backend_deal_3_dscnt_usd_ext,
           |PS.backend_deal_3_dscnt_usd_pr_unt,
           |PS.backend_deal_3_lcy_unt_prc,
           |PS.backend_deal_3_usd_unt_prc,
           |PS.backend_deal_4_dscnt_lcy_ext,
           |PS.backend_deal_4_dscnt_lcy_pr_unt,
           |PS.backend_deal_4_dscnt_usd_ext,
           |PS.backend_deal_4_dscnt_usd_pr_unt,
           |PS.backend_deal_4_lcy_unt_prc,
           |PS.backend_deal_4_usd_unt_prc,
           |PS.backend_rbt_1_lcy_pr_unt,
           |PS.backend_rbt_1_usd_pr_unt,
           |PS.backend_rbt_2_lcy_pr_unt,
           |PS.backend_rbt_2_usd_pr_unt,
           |PS.backend_rbt_3_lcy_pr_unt,
           |PS.backend_rbt_3_usd_pr_unt,
           |PS.backend_rbt_4_lcy_pr_unt,
           |PS.backend_rbt_4_usd_pr_unt,
           |PS.prch_agrmnt_dscnt_lcy_unt,
           |PS.prch_agrmnt_dscnt_usd_unt,
           |COALESCE(APJ.sls_asp_lcy_cd,PS.sls_asp_lcy) as sls_asp_lcy,
           |PS.sls_asp_lcy_unt_prc,
           |COALESCE(APJ.sls_asp_usd_cd,PS.sls_asp_usd) as sls_asp_usd,
           |PS.sls_asp_usd_unt_prc,
           |PS.sls_avrg_deal_nt_lcy,
           |PS.sls_avrg_deal_nt_lcy_unt_prc,
           |PS.sls_avrg_deal_nt_usd,
           |PS.sls_avrg_deal_nt_usd_unt_prc,
           |PS.sls_avrg_deal_nt_vcy,
           |PS.sls_avrg_deal_nt_vcy_unt_prc,
           |COALESCE(APJ.sls_deal_nt_lcy_cd,PS.sls_deal_nt_lcy) as sls_deal_nt_lcy,
           |PS.sls_deal_nt_lcy_unt_prc,
           |COALESCE(csis_emea_sim.sls_deal_nt_usd_cd,APJ.sls_deal_nt_usd_cd,PS.sls_deal_nt_usd) as sls_deal_nt_usd,
           |PS.sls_deal_nt_usd_unt_prc,
           |PS.sls_deal_nt_vcy,
           |PS.sls_deal_nt_vcy_unt_prc,
           |COALESCE(APJ.sls_lst_lcy_cd,PS.sls_lst_lcy) as sls_lst_lcy,
           |PS.sls_lst_lcy_unt_prc,
           |COALESCE(APJ.sls_lst_usd_cd,PS.sls_lst_usd) as sls_lst_usd,
           |PS.sls_lst_usd_unt_prc,
           |PS.sls_lst_vcy,
           |PS.sls_lst_vcy_unt_prc,
           |COALESCE(APJ.sls_ndp_lcy_cd,PS.sls_ndp_lcy) as sls_ndp_lcy,
           |PS.sls_ndp_lcy_unt_prc,
           |COALESCE(APJ.sls_ndp_usd_cd,PS.sls_ndp_usd) as sls_ndp_usd,
           |PS.sls_ndp_usd_unt_prc,
           |PS.sls_ndp_vcy,
           |PS.sls_ndp_vcy_unt_prc,
           |PS.upfrnt_deal_1_dscnt_lcy,
           |PS.upfrnt_deal_1_dscnt_lcy_pr_unt,
           |PS.upfrnt_deal_1_dscnt_usd,
           |PS.upfrnt_deal_1_dscnt_usd_pr_unt,
           |PS.upfrnt_deal_1_lcy_unt_prc,
           |PS.upfrnt_deal_1_usd_unt_prc,
           |PS.upfrnt_deal_2_dscnt_lcy,
           |PS.upfrnt_deal_2_dscnt_lcy_pr_unt,
           |PS.upfrnt_deal_2_dscnt_usd,
           |PS.upfrnt_deal_2_dscnt_usd_pr_unt,
           |PS.upfrnt_deal_2_lcy_unt_prc,
           |PS.upfrnt_deal_2_usd_unt_prc,
           |PS.upfrnt_deal_3_dscnt_lcy_ext,
           |PS.upfrnt_deal_3_dscnt_lcy_pr_unt,
           |PS.upfrnt_deal_3_dscnt_usd_ext,
           |PS.upfrnt_deal_3_dscnt_usd_pr_unt,
           |PS.upfrnt_deal_3_lcy_unt_prc,
           |PS.upfrnt_deal_3_usd_unt_prc,
           |PS.pos_prs_iud_flag,
           |PS.pos_prs_reserve_field_1,
           |PS.pos_prs_reserve_field_2,
           |PS.pos_prs_reserve_field_3,
           |PS.pos_prs_reserve_field_4,
           |PS.pos_prs_reserve_field_5,
           |PS.pos_prs_reserve_field_6,
           |PS.pos_prs_reserve_field_7,
           |PS.pos_prs_reserve_field_8,
           |PS.pos_prs_reserve_field_9,
           |PS.pos_prs_reserve_field_10,
           |PS.upfrnt_deal_1_ln_itm,
           |PS.upfrnt_deal_1_mc_cd,
           |PS.upfrnt_deal_1_vrsn,
           |PS.upfrnt_deal_2_ln_itm,
           |PS.upfrnt_deal_2_mc_cd,
           |PS.upfrnt_deal_2_vrsn,
           |PS.upfrnt_deal_3_ln_itm,
           |PS.upfrnt_deal_3_mc_cd,
           |PS.upfrnt_deal_3_vrsn,
           |PS.asp_coefficient,
           |PS.avrg_deal_nt_coefficient,
           |PS.deal_geo,
           |PS.dscnt_geo,
           |PS.ex_rate_pcy_to_usd,
           |PS.ex_rate_lcy_to_usd,
           |PS.ex_rate_vcy_to_usd,
           |PS.lcl_curr_cd,
           |PS.valtn_curr_cd,
           |PS.valtn_cust_appl_cd,
           |PS.valtn_prch_agrmnt_dscnt,
           |PS.valtn_prch_agrmnt,
           |PS.valtn_prc_dsc,
           |PS.valtn_trsn_id,
           |PS.valtn_wrnng_cd,
           |PS.e2open_fl_id_valtn_rspns,
           |PS.prnt_trsn_id_valtn_rspns,
           |PS.prv_trsn_id_valtn_rspns,
           |PS.reporter_id_valtn_rspns,
           |PS.trsn_id_valtn_rspns,
           |COALESCE(APJ.ptnr_rptd_prc_lcy_cd,PS.ptnr_rptd_prc_lcy) as ptnr_rptd_prc_lcy,
           |COALESCE(APJ.ptnr_rptd_prc_usd_cd,PS.ptnr_rptd_prc_usd) AS ptnr_rptd_prc_usd,
           |PS.sls_bs_qty,
           |PS.sls_opt_qty,
           |PS.src_sys_upd_ts,
           |PS.src_sys_ky,
           |PS.lgcl_dlt_ind,
           |PS.ins_gmt_ts,
           |PS.upd_gmt_ts,
           |PS.src_sys_extrc_gmt_ts,
           |PS.src_sys_btch_nr,
           |PS.fl_nm,
           |PS.ld_jb_nr,
           |UPPER(COALESCE(APJ.bi_deal_ind_cd,PS.deal_ind)) as deal_ind,
           |UPPER(COALESCE(APJ.bi_deal_ind_grp_cd,PS.deal_ind_grp)) as deal_ind_grp,
           |PS.bs_prod_nr,
           |PS.bi_trsn_xclsn_flg,
           |PS.bi_trsn_xclsn_rsn_cd,
           |PS.prod_opt,
           |PS.vol_sku_flg,
           |cto_bto.bi_cto_bto_flg AS bi_cto_bto_flg,
           |case when trim(upper(reseller.bi_rslr_rsn_cd))='NO RESELLER' or reseller.bi_rslr_rsn_cd is null then 'NO RESELLER'
           |WHEN reseller.bi_rslr_ctr_cd IS NOT NULL  THEN trim(reseller.bi_rslr_ctr_cd)
           |WHEN RSLR_LGCY_HST_UP.byr_ctry_nm is not null and trim(RSLR_LGCY_HST_UP.byr_ctry_nm) <> '' THEN trim(RSLR_LGCY_HST_UP.byr_ctry_nm) END AS bi_rslr_ctr_cd,
           |case when upper(trim(distributor.bi_dstr_rsn_cd))='NO DISTRIBUTOR' or distributor.bi_dstr_rsn_cd is NULL then 'NO DISTRIBUTOR'
           |WHEN distributor.bi_dstr_ctr_cd IS NOT NULL  THEN trim(distributor.bi_dstr_ctr_cd) END AS bi_dstr_ctr_cd,
           |PS.bi_rptr_ctr_cd,
           |CASE WHEN ENTITLED.Entitled_Party_id IS NULL or  upper(trim(ENTITLED.BI_Enti_rsn_cd))='NO ENTITLED PARTY' THEN 'NO ENTITLED PARTY'
           |WHEN UPPER(TRIM(ENTITLED.BI_Enti_rsn_cd))='OPE' THEN trim(PS.ope_enttld_prty_ctry) ELSE trim(ENTITLED.bi_enttld_ctry_cd) END as bi_enttld_ctry_cd,
           |case WHEN upper(trim(ps.ope_end_cust_rsn_cd))='NO END CUSTOMER' and upper(trim(ps.ope_src_end_cust_prty_id))='UNMATCHED' then ps.ope_src_end_cust_ctry
           |when END_CUST.END_Cust_Party_id IS NULL or upper(trim(END_CUST.BI_END_rsn_cd))='NO END CUSTOMER'  THEN 'NO END CUSTOMER' else trim(END_CUST.bi_end_cust_ctry_cd) end as bi_end_cust_ctry_cd,
           |trim(PS.mc_cd) as mc_cd,
           |PS.mc_cd_dn,
           |trim(PS.upfrnt_deal_1_mc_dn_cd) as upfrnt_deal_1_mc_dn_cd,
           |trim(PS.upfrnt_deal_2_mc_dn_cd) as upfrnt_deal_2_mc_dn_cd,
           |trim(PS.upfrnt_deal_3_mc_dn_cd) as upfrnt_deal_3_mc_dn_cd,
           |trim(PS.backend_deal_1_mc_dn_cd) as backend_deal_1_mc_dn_cd,
           |trim(PS.backend_deal_2_mc_dn_cd) as backend_deal_2_mc_dn_cd,
           |trim(PS.backend_deal_3_mc_dn_cd) as backend_deal_3_mc_dn_cd,
           |trim(PS.backend_deal_4_mc_dn_cd) as backend_deal_4_mc_dn_cd,
           |PS.ps_e2open_extrt_nm,
           |PS.ps_e2open_seq_id,
           |PS.ps_e2open_extrt_dt,
           |PS.ps_e2open_extrt_ts,
           |PS.ps_prs_e2open_extrt_nm,
           |PS.ps_prs_e2open_seq_id,
           |PS.ps_prs_e2open_extrt_dt,
           |PS.ps_prs_e2open_extrt_ts,
           |PS.ps_eap_load_dt,
           |PS.ps_eap_load_ts,
           |PS.ps_prs_eap_load_dt,
           |PS.ps_prs_eap_load_ts,
           |PS.hpe_invoice_dt,
           |PS.hpe_ord_bundle_id,
           |PS.hpe_ord_deal_id,
           |PS.hpe_obj_srvc_mtrl_id,
           |PS.hpe_ord_item_nr,
           |PS.hpe_ord_mc_cd,
           |PS.hpe_ord_pft_cntr,
           |PS.hpe_ord_sales_dvsn_cd,
           |PS.hpe_shpmt_dt,
           |PS.hpe_ord_dt,
           |PS.hpe_ord_drp_shp_flg,
           |PS.hpe_ord_end_cust_prty_id,
           |PS.hpe_ord_nr,
           |PS.hpe_ord_rtm,
           |PS.hpe_ord_sld_br_typ,
           |PS.hpe_ord_sld_pa_nr,
           |PS.hpe_ord_sld_prty_id,
           |PS.hpe_ord_tier2_rslr_br_typ,
           |PS.hpe_ord_tier2_rslr_prty_id,
           |PS.hpe_ord_typ,
           |case when upper(trim(reseller.bi_rslr_rsn_cd))='NO RESELLER' or reseller.bi_rslr_rsn_cd is null then 'NO RESELLER'
           |else trim(reseller.bi_rslr_prty_id) end as  bi_rslr_prty_id,
           |COALESCE(reseller.bi_rslr_rsn_cd,'NO RESELLER') AS bi_rslr_rsn_cd,
           |case when trim(upper(distributor.bi_dstr_rsn_cd))='NO DISTRIBUTOR' or distributor.bi_dstr_rsn_cd is NULL then 'NO DISTRIBUTOR'
           |else trim(distributor.bi_dstr_prty_id)  end as bi_dstr_prty_id,
           |UPPER(COALESCE(trim(distributor.bi_dstr_rsn_cd),'NO DISTRIBUTOR')) AS bi_dstr_rsn_cd,
           |CASE when upper(trim(reseller.bi_rslr_rsn_cd))='NO RESELLER' or reseller.bi_rslr_rsn_cd is null then 'NO RESELLER'
           |WHEN reseller.bi_rslr_prty_id IS NOT NULL AND reseller.BI_rslr_BR_typ_nm IS NULL THEN 'UNMATCHED'
           |WHEN reseller.bi_rslr_prty_id IS NOT NULL AND reseller.BI_rslr_BR_typ_nm IS NOT NULL THEN UPPER(reseller.BI_rslr_BR_typ_nm)
           |WHEN upper(trim(reseller.bi_rslr_rsn_cd))='NO RESELLER' or reseller.bi_rslr_rsn_cd is null then 'NO RESELLER' END AS BI_rslr_BR_typ_nm,
           |CASE WHEN distributor.bi_dstr_prty_id IS NOT NULL AND distributor.BI_Dstr_BR_typ_nm IS NULL THEN 'UNMATCHED'
           |WHEN distributor.bi_dstr_prty_id IS NOT NULL AND distributor.BI_Dstr_BR_typ_nm IS NOT NULL THEN UPPER(distributor.BI_Dstr_BR_typ_nm)
           |when trim(upper(distributor.bi_dstr_rsn_cd))='NO DISTRIBUTOR' or distributor.bi_dstr_rsn_cd is NULL then 'NO DISTRIBUTOR' END AS BI_Dstr_BR_typ_nm,
           |CASE WHEN upper(trim(ps.ope_end_cust_rsn_cd))='NO END CUSTOMER' and upper(trim(ps.ope_src_end_cust_prty_id))='UNMATCHED' then 'UNMATCHED'
           |WHEN END_CUST.END_Cust_Party_id IS NULL or upper(trim(END_CUST.BI_END_rsn_cd))='NO END CUSTOMER'  THEN 'NO END CUSTOMER' else trim(END_CUST.END_Cust_Party_id) end as END_Cust_Party_id,
           |CASE WHEN upper(trim(ps.ope_end_cust_rsn_cd))='NO END CUSTOMER' and upper(trim(ps.ope_src_end_cust_prty_id))='UNMATCHED' then 'UNMATCHED'
           |WHEN END_CUST.END_Cust_Party_id IS NULL or upper(trim(END_CUST.BI_END_rsn_cd))='NO END CUSTOMER'  THEN 'NO END CUSTOMER' ELSE trim(END_CUST.bi_end_cust_br_typ) end as bi_end_cust_BR_typ_nm,
           |CASE WHEN upper(trim(ps.ope_end_cust_rsn_cd))='NO END CUSTOMER' and upper(trim(ps.ope_src_end_cust_prty_id))='UNMATCHED' then 'UNMATCHED'
           |when END_CUST.END_Cust_Party_id IS NULL THEN 'NO END CUSTOMER' ELSE upper(trim(END_CUST.BI_END_rsn_cd)) END as BI_END_rsn_cd,
           |CASE WHEN ENTITLED.Entitled_Party_id IS NULL or  upper(trim(ENTITLED.BI_Enti_rsn_cd))='NO ENTITLED PARTY' THEN 'NO ENTITLED PARTY' ELSE UPPER(trim(ENTITLED.BI_Enti_rsn_cd)) END as BI_Enti_rsn_cd,
           |CASE WHEN ENTITLED.Entitled_Party_id IS NULL or  upper(trim(ENTITLED.BI_Enti_rsn_cd))='NO ENTITLED PARTY' THEN 'NO ENTITLED PARTY' ELSE trim(ENTITLED.Entitled_Party_id) end as Entitled_Party_id,
           |Current_timestamp as ins_ts,
           |case when upper(trim(reseller.bi_rslr_rsn_cd))='NO RESELLER' or reseller.bi_rslr_rsn_cd is null then 'NO RESELLER' ELSE trim(reseller.bi_rslr_lgcy_pro_id) end AS bi_rslr_lgcy_pro_id,
           |case when trim(upper(distributor.bi_dstr_rsn_cd))='NO DISTRIBUTOR' or distributor.bi_dstr_rsn_cd is NULL then 'NO DISTRIBUTOR' ELSE trim(distributor.dstr_lgcy_pro_id) END AS dstr_lgcy_pro_id,
           |CASE WHEN END_CUST.bi_end_customer_legacy_opsi_id='-1' THEN NULL ELSE END_CUST.bi_end_customer_legacy_opsi_id END AS bi_end_customer_legacy_opsi_id,
           |CASE WHEN ENTITLED.Entitled_Party_id IS NULL or upper(trim(ENTITLED.BI_Enti_rsn_cd))='NO ENTITLED PARTY' THEN 'NO ENTITLED PARTY'  ELSE ENTITLED.bi_enttld_prty_br_typ end as bi_enttld_prty_br_typ_nm,
           |PS.ope_rtm,
           |substring(ps.dt,0,7) as partition_year_month
           |from ea_support_temp_an.chnlptnr_tmp_ps_fact_grpg PS
           |LEFT JOIN resellerTrsn reseller on reseller.trsn_i_1_id=PS.trsn_id
           |LEFT JOIN distributorTrsn distributor on distributor.trsn_i_1_id=PS.trsn_id
           |LEFT JOIN ea_common.END_CUST_DF_trsn END_CUST ON END_CUST.trsn_i_1_id=PS.trsn_id
           |LEFT JOIN entitledPrtyDfTrsn ENTITLED ON ENTITLED.trsn_i_1_id=PS.trsn_id
           |LEFT JOIN ea_common.csis_apj_ps_upd_ref APJ ON APJ.trsn_id=PS.trsn_id
           |LEFT JOIN ea_common.csis_emea_simulated_nt_ref csis_emea_sim ON csis_emea_sim.trsn_id=PS.trsn_id
           |LEFT JOIN rptg_ptnr_mstr_dmnsn_tmp rpm ON ps.reporter_id = rpm.reporter_i_1_id
           |LEFT JOIN ea_common.bi_rslr_lgcy_hq_ctry_bmt_ref RSLR_LGCY_HST_UP ON trim(RSLR_LGCY_HST_UP.slr_site_rowid_id)=trim(rpm.ptnr_pro_i_1_id) and RSLR_LGCY_HST_UP.inv_nr=ps.ptnr_inv_nr
           |LEFT JOIN ea_support_temp_an.POS_cto_bto_fact cto_bto ON PS.trsn_id =cto_bto.trsn_id
           |""".stripMargin)

      //var ps_dmnsnDF = spark.emptyDataFrame
      if (!data_history_check) {
        val loadPOSSF_DF = chnlptnrPsFact.repartition(25).write.mode("overwrite").format("orc").insertInto("ea_common_an"+ "." + "chnlptnr_ps_wout_sn_fact_inc")
        logger.info("POS_INC_FACT Load is completed")
        load_pos_sn_fact_load = true
      }
      else {

        val csisAmsWoSnDataLoad = spark.sql(
          s"""
             |Select 'CSIS AMS' as src_sys_nm,
             |'POS' as rec_src,
             |trim(be_big_deal_id) as backend_deal_id_1,
             |NULL as rptd_backend_deal_1,
             |NULL as backend_deal_id_2,
             |NULL as rptd_backend_deal_2,
             |NULL as backend_deal_id_3,
             |NULL as rptd_backend_deal_3,
             |NULL as backend_deal_id_4,
             |NULL as rptd_backend_deal_4,
             |NULL as bll_to_asian_addr,
             |NULL as bll_to_co_tx_id,
             |NULL as bll_to_co_tx_id_enr,
             |NULL as bll_to_cntct_nm,
             |NULL as bll_to_ctry_rptd,
             |NULL as bll_to_ky,
             |NULL as bll_to_ph_nr,
             |NULL as bll_to_rw_addr_id,
             |NULL as bll_to_rptd_id,
             |NULL as nmso_name,
             |NULL as nmso_date,
             |NULL as standing_offer_number,
             |NULL as deal_reg_1,
             |NULL as rptd_deal_reg_1,
             |NULL as deal_reg_2,
             |NULL as rptd_deal_reg_2,
             |0 as nt_cst_aftr_rbt_lcy,
             |0 as ptnr_prch_lcy_unt_prc,
             |0 as ptnr_sl_lcy_unt_prc,
             |0 as ptnr_sl_usd_unt_prc,
             |0 as sls_bndl_qty,
             |0 as sls_qty,
             |NULL as pos_iud_flag,
             |NULL as pos_reserve_field_1,
             |NULL as pos_reserve_field_2,
             |NULL as rptg_prd_strt_dt,
             |NULL as rptg_prd_end_dt,
             |NULL as pos_reserve_field_5,
             |NULL as pos_reserve_field_6,
             |NULL as pos_reserve_field_7,
             |NULL as pos_reserve_field_8,
             |NULL as pos_reserve_field_9,
             |NULL as pos_reserve_field_10,
             |NULL as bndl_prod_id,
             |NULL as orgl_prod_ln_id,
             |NULL as ptnr_bndl_prod_id,
             |NULL as ptnr_inrn_prod_nr,
             |prod_nr as prod_nr,
             |NULL as rptd_prod_nr,
             |NULL as vl_vol_flg,
             |NULL as reporter_id,
             |NULL as sl_frm_cntct_nm,
             |NULL as sl_frm_ctry_rptd,
             |NULL as sl_frm_ky,
             |NULL as sl_frm_ph_nr,
             |NULL as sl_frm_rw_addr_id,
             |NULL as sl_frm_rptd_id,
             |NULL as shp_frm_cntct_nm,
             |NULL as shp_frm_ctry_rptd,
             |NULL as shp_frm_ky,
             |NULL as shp_frm_ph_nr,
             |NULL as shp_frm_rw_addr_id,
             |NULL as shp_frm_rptd_id,
             |NULL as shp_to_asian_addr,
             |NULL as shp_to_co_tx_id,
             |NULL as shp_to_co_tx_id_enr,
             |NULL as shp_to_cntct_nm,
             |NULL as shp_to_ctry_rptd,
             |NULL as shp_to_ky,
             |NULL as shp_to_ph_nr,
             |NULL as shp_to_rw_addr_id,
             |NULL as shp_to_rptd_id,
             |NULL as sld_to_asian_addr,
             |NULL as sld_to_co_tx_id,
             |NULL as sld_to_co_tx_id_enr,
             |NULL as sld_to_cntct_nm,
             |NULL as sld_to_ctry_rptd,
             |NULL as sld_to_ky,
             |NULL as sld_to_ph_nr,
             |NULL as sld_to_rw_addr_id,
             |NULL as sld_to_rptd_id,
             |NULL as end_usr_addr_src_rptd,
             |NULL as end_usr_asian_addr,
             |NULL as end_usr_co_tx_id,
             |NULL as end_usr_co_tx_id_enr,
             |NULL as end_usr_cntct_nm,
             |NULL as end_usr_ctry_rptd,
             |NULL as end_usr_ky,
             |NULL as end_usr_ph_nr,
             |NULL as end_usr_rw_addr_id,
             |NULL as end_usr_rptd_id,
             |NULL as enttld_asian_addr,
             |NULL as enttld_co_tx_id,
             |NULL as enttld_co_tx_id_enr,
             |NULL as enttld_cntct_nm,
             |NULL as enttld_ctry_rptd,
             |NULL as enttld_ky,
             |NULL as enttld_ph_nr,
             |NULL as enttld_rw_addr_id,
             |NULL as enttld_rptd_id,
             |NULL as agt_flg,
             |NULL as brm_err_flg,
             |NULL as crss_shp_flg,
             |NULL as cust_to_ptnr_po_nr,
             |dy_dt as dt,
             |NULL as drp_shp_flg,
             |NULL as e2open_fl_id,
             |NULL as e2open_fl_rcvd_dt,
             |NULL as hpe_inv_nr,
             |NULL as nt_cst_aftr_rbt_curr_cd,
             |NULL as org_sls_nm,
             |NULL as orgl_hpe_asngd_trsn_id,
             |NULL as orgl_trsn_id,
             |NULL as prnt_trsn_id,
             |NULL as ptnr_inrn_trsn_id,
             |NULL as ptnr_inv_dt,
             |inv_nr as ptnr_inv_nr,
             |NULL as ptnr_prch_curr_cd,
             |NULL as ptnr_sls_rep_nm,
             |NULL as ptnr_sl_prc_curr_cd,
             |NULL as ptnr_sm_dt,
             |NULL as ptnr_to_hpe_po_nr,
             |NULL as prv_trsn_id,
             |NULL as prod_orgn_cd,
             |NULL as prchg_src_ws_ind,
             |NULL as splyr_iso_ctry_cd,
             |NULL as tty_mgr_cd,
             |trsn_id as trsn_id,
             |NULL as vldtn_wrnng_cd,
             |NULL as rptd_upfrnt_deal_1,
             |trim(uf_big_deal_id) as upfrnt_deal_id_1_id,
             |NULL as rptd_upfrnt_deal_2,
             |NULL as upfrnt_deal_id_2,
             |NULL as rptd_upfrnt_deal_3,
             |NULL as upfrnt_deal_id_3_id,
             |NULL as ope_bmt_dstr_bsn_rshp_typ,
             |NULL as ope_bmt_dstr_prty_id,
             |NULL as ope_bmt_dstr_sls_comp_rlvnt,
             |NULL as ope_dstr_bsn_rshp_typ,
             |NULL as ope_dstr_prty_id,
             |NULL as ope_dstr_rsn_cd,
             |NULL as ope_bmt_end_cust_bsn_rshp_typ,
             |NULL as ope_bmt_end_cust_prty_id,
             |NULL as ope_bmt_end_cust_sls_comp_rlvnt,
             |NULL as ope_deal_end_cust_bsn_rshp_typ,
             |NULL as ope_deal_end_cust_prty_id,
             |NULL as ope_end_cust_bsn_rshp_typ,
             |NULL as ope_end_cust_prty_id,
             |NULL as ope_end_cust_rsn_cd,
             |NULL as ope_lvl_3_bsn_rshp_typ,
             |NULL as ope_lvl_3_prty_id,
             |NULL as ope_lvl_3_rshp,
             |NULL as ope_src_enttld_prty_ctry,
             |NULL as ope_src_enttld_prty_prty_id,
             |NULL as ope_src_end_cust_ctry,
             |NULL as ope_src_end_cust_prty_id,
             |NULL as ope_enttld_prty_ctry,
             |NULL as ope_entitlted_prty_id,
             |NULL as ope_dstr_bsn_mgr_rptg_dstr,
             |NULL as ope_dstr_rptg_dstr,
             |NULL as ope_end_cust_sls_rep_rptg_dstr,
             |NULL as ope_flipped_dstr_bsn_mgr_vw_flg,
             |NULL as ope_flipped_dstr_vw_flg,
             |NULL as ope_flipped_end_cust_vw_flg,
             |NULL as ope_flipped_ptnr_bsn_mgr_vw_flg,
             |NULL as ope_flipped_rslr_vw_flg,
             |NULL as ope_lvl_1_bsn_rshp_typ,
             |NULL as ope_lvl_1_prty_id,
             |NULL as ope_lvl_1_rshp,
             |NULL as ope_ptnr_bsn_mgr_rptg_dstr,
             |NULL as ope_rslr_rptg_dstr,
             |NULL as ope_bmt_rslr_bsn_rshp_typ,
             |NULL as ope_bmt_rslr_prty_id,
             |NULL as ope_bmt_rslr_sls_comp_rlvnt,
             |NULL as ope_rslr_bsn_rshp_typ,
             |NULL as ope_rslr_prty_id,
             |NULL as ope_rslr_rsn_cd,
             |NULL as ope_lvl_2_bsn_rshp_typ,
             |NULL as ope_lvl_2_prty_id,
             |NULL as ope_lvl_2_rshp,
             |NULL as ope_sld_to_br_typ,
             |NULL as ope_sld_to_ctry,
             |NULL as ope_sld_to_prty_id,
             |NULL as ope_big_deal_xclsn,
             |NULL as ope_crss_brdr,
             |NULL as ope_crss_srcd,
             |NULL as ope_dstr_rptg_dstr_to_another_rptg_dstr,
             |NULL as ope_dstr_bookings_comp_vw,
             |NULL as ope_dstr_bookings_vw,
             |NULL as ope_dstr_bsn_mgr_bookings_vw,
             |NULL as ope_dstr_bsn_mgr_comp_vw,
             |NULL as ope_drp_shp,
             |NULL as ope_emb_srv_ln_itms,
             |NULL as ope_end_cust_rptg_dstr_to_another_rptg_dstr,
             |NULL as ope_end_cust_sls_rep_bookings_vw,
             |NULL as ope_end_cust_sls_rep_comp_vw,
             |NULL as ope_fdrl_bsn,
             |NULL as ope_ptnr_bsn_mgr_bookings_vw,
             |NULL as ope_ptnr_bsn_mgr_comp_vw,
             |NULL as ope_pft_cntr,
             |NULL as ope_rslr_bookings_vw,
             |NULL as ope_rslr_comp_vw,
             |NULL as ope_srv_ln_itm,
             |NULL as ope_trsn_id,
             |NULL as ope_reporter_prty_id,
             |NULL as ope_br_ty_1_cd,
             |NULL as rptr_prty_ky,
             |NULL as bll_to_raw_address_id,
             |NULL as bll_to_lctn_ctry_cd,
             |NULL as bll_to_lctn_addr_1,
             |NULL as bll_to_lctn_addr_2,
             |NULL as bll_to_lctn_cty_nm,
             |NULL as bll_to_lctn_nm,
             |NULL as bll_to_lctn_pstl_cd,
             |NULL as bll_to_lctn_st_cd,
             |NULL as sell_from_raw_address_id,
             |NULL as sl_frm_lctn_ctry_cd,
             |NULL as sl_frm_lctn_addr_1,
             |NULL as sl_frm_lctn_addr_2,
             |NULL as sl_frm_lctn_cty_nm,
             |NULL as sl_frm_lctn_nm,
             |NULL as sl_frm_lctn_pstl_cd,
             |NULL as sl_frm_lctn_st_cd,
             |NULL as ship_to_raw_address_id,
             |NULL as shp_to_lctn_ctry_cd,
             |NULL as shp_to_lctn_addr_1,
             |NULL as shp_to_lctn_addr_2,
             |NULL as shp_to_lctn_cty_nm,
             |NULL as shp_to_lctn_nm,
             |NULL as shp_to_lctn_pstl_cd,
             |NULL as shp_to_lctn_st_cd,
             |NULL as ship_from_raw_address_id,
             |NULL as shp_frm_lctn_ctry_cd,
             |NULL as shp_frm_lctn_addr_1,
             |NULL as shp_frm_lctn_addr_2,
             |NULL as shp_frm_lctn_cty_nm,
             |NULL as shp_frm_lctn_nm,
             |NULL as shp_frm_lctn_pstl_cd,
             |NULL as shp_frm_lctn_st_cd,
             |NULL as sold_to_raw_address_id,
             |NULL as sld_to_lctn_ctry_cd,
             |NULL as sld_to_lctn_addr_1,
             |NULL as sld_to_lctn_addr_2,
             |NULL as sld_to_lctn_cty_nm,
             |NULL as sld_to_lctn_nm,
             |NULL as sld_to_lctn_pstl_cd,
             |NULL as sld_to_lctn_st_cd,
             |NULL as end_cust_raw_address_id,
             |NULL as end_cust_lctn_ctry_cd,
             |NULL as end_cust_lctn_addr_1,
             |NULL as end_cust_lctn_addr_2,
             |NULL as end_cust_lctn_cty_nm,
             |NULL as end_cust_lctn_nm,
             |NULL as end_cust_lctn_pstl_cd,
             |NULL as end_cust_lctn_st_cd,
             |NULL as enttld_lctn_raw_address_id,
             |NULL as enttld_lctn_ctry_cd,
             |NULL as enttld_lctn_addr_1,
             |NULL as enttld_lctn_addr_2,
             |NULL as enttld_lctn_cty_nm,
             |NULL as enttld_lctn_nm,
             |NULL as enttld_lctn_pstl_cd,
             |NULL as enttld_lctn_st_cd,
             |NULL as bll_to_duns,
             |NULL as bll_to_ent_mdm_br_typ,
             |NULL as bll_to_ent_mdm_prty_id,
             |NULL as bll_to_mdcp_br_typ,
             |NULL as bll_to_mdcp_bri_id,
             |NULL as bll_to_mdcp_loc_id,
             |NULL as bll_to_mdcp_opsi_id,
             |NULL as bll_to_mdcp_org_id,
             |NULL as bll_to_ptnr_pro_id,
             |NULL as bill_to_rawaddr_id,
             |NULL as bill_to_rawaddr_key,
             |NULL as pos_grp_bill_to_reserve_field_1,
             |NULL as pos_grp_bill_to_reserve_field_2,
             |NULL as pos_grp_bill_to_reserve_field_3,
             |NULL as pos_grp_bill_to_reserve_field_4,
             |NULL as pos_grp_bill_to_reserve_field_5,
             |NULL as sl_frm_duns,
             |NULL as sl_frm_ent_mdm_br_typ,
             |NULL as sl_frm_ent_mdm_prty_id,
             |NULL as sl_frm_mdcp_br_typ,
             |NULL as sl_frm_mdcp_bri_id,
             |NULL as sl_frm_mdcp_loc_id,
             |NULL as sl_frm_mdcp_opsi_id,
             |NULL as sl_frm_mdcp_org_id,
             |NULL as sl_frm_ptnr_pro_id,
             |NULL as sell_from_rawaddr_id,
             |NULL as sell_from_rawaddr_key,
             |NULL as pos_grp_sell_from_reserve_field_1,
             |NULL as pos_grp_sell_from_reserve_field_2,
             |NULL as pos_grp_sell_from_reserve_field_3,
             |NULL as pos_grp_sell_from_reserve_field_4,
             |NULL as pos_grp_sell_from_reserve_field_5,
             |NULL as ship_from_rawaddr_id,
             |NULL as ship_from_rawaddr_key,
             |NULL as pos_grp_ship_from_reserve_field_1,
             |NULL as pos_grp_ship_from_reserve_field_2,
             |NULL as pos_grp_ship_from_reserve_field_3,
             |NULL as pos_grp_ship_from_reserve_field_4,
             |NULL as pos_grp_ship_from_reserve_field_5,
             |NULL as shp_frm_duns,
             |NULL as shp_frm_ent_mdm_br_typ,
             |NULL as shp_frm_ent_mdm_prty_id,
             |NULL as shp_frm_mdcp_br_typ,
             |NULL as shp_frm_mdcp_bri_id,
             |NULL as shp_frm_mdcp_loc_id,
             |NULL as shp_frm_mdcp_opsi_id,
             |NULL as shp_frm_mdcp_org_id,
             |NULL as shp_frm_ptnr_pro_id,
             |NULL as ship_to_rawaddr_id,
             |NULL as ship_to_rawaddr_key,
             |NULL as pos_grp_ship_to_reserve_field_1,
             |NULL as pos_grp_ship_to_reserve_field_2,
             |NULL as pos_grp_ship_to_reserve_field_3,
             |NULL as pos_grp_ship_to_reserve_field_4,
             |NULL as pos_grp_ship_to_reserve_field_5,
             |NULL as shp_to_duns,
             |NULL as shp_to_ent_mdm_br_typ,
             |NULL as shp_to_ent_mdm_prty_id,
             |NULL as shp_to_mdcp_br_typ,
             |NULL as shp_to_mdcp_bri_id,
             |NULL as shp_to_mdcp_loc_id,
             |NULL as shp_to_mdcp_opsi_id,
             |NULL as shp_to_mdcp_org_id,
             |NULL as shp_to_ptnr_pro_id,
             |NULL as sold_to_rawaddr_id,
             |NULL as sold_to_rawaddr_key,
             |NULL as pos_grp_sold_to_reserve_field_1,
             |NULL as pos_grp_sold_to_reserve_field_2,
             |NULL as pos_grp_sold_to_reserve_field_3,
             |NULL as pos_grp_sold_to_reserve_field_4,
             |NULL as pos_grp_sold_to_reserve_field_5,
             |NULL as sld_to_duns,
             |NULL as sld_to_ent_mdm_br_typ,
             |NULL as sld_to_ent_mdm_prty_id,
             |NULL as sld_to_mdcp_br_typ,
             |NULL as sld_to_mdcp_bri_id,
             |NULL as sld_to_mdcp_loc_id,
             |NULL as sld_to_mdcp_opsi_id,
             |NULL as sld_to_mdcp_org_id,
             |NULL as sld_to_ptnr_pro_id,
             |NULL as end_cust_rawaddr_id,
             |NULL as end_cust_rawaddr_key,
             |NULL as pos_grp_end_customer_reserve_field_1,
             |NULL as pos_grp_end_customer_reserve_field_2,
             |NULL as pos_grp_end_customer_reserve_field_3,
             |NULL as pos_grp_end_customer_reserve_field_4,
             |NULL as pos_grp_end_customer_reserve_field_5,
             |NULL as end_cust_duns,
             |NULL as end_cust_ent_mdm_br_typ,
             |NULL as end_cust_ent_mdm_prty,
             |NULL as end_cust_mdcp_br_typ,
             |NULL as end_cust_mdcp_bri_id,
             |NULL as end_cust_mdcp_loc_id,
             |NULL as end_cust_mdcp_opsi_id,
             |NULL as end_cust_mdcp_org_id,
             |NULL as end_cust_ptnr_pro_id,
             |NULL as enttld_ent_rawaddr_id,
             |NULL as enttld_ent_rawaddr_key,
             |NULL as pos_grp_enttld_ent_reserve_field_1,
             |NULL as pos_grp_enttld_ent_reserve_field_2,
             |NULL as pos_grp_enttld_ent_reserve_field_3,
             |NULL as pos_grp_enttld_ent_reserve_field_4,
             |NULL as pos_grp_enttld_ent_reserve_field_5,
             |NULL as enttld_duns,
             |NULL as enttld_ent_mdm_br_typ,
             |NULL as enttld_ent_mdm_prty_id,
             |NULL as enttld_mdcp_br_typ,
             |NULL as enttld_mdcp_bri_id,
             |NULL as enttld_mdcp_loc_id,
             |NULL as enttld_mdcp_opsi_id,
             |NULL as enttld_mdcp_org_id,
             |NULL as enttld_ptnr_pro_id,
             |NULL as backend_deal_1_ln_itm,
             |NULL as backend_deal_1_mc_cd,
             |NULL as backend_deal_1_vrsn,
             |NULL as backend_deal_2_ln_itm_cd,
             |NULL as backend_deal_2_m_cd,
             |NULL as backend_deal_2_vrsn,
             |NULL as backend_deal_3_ln_itm,
             |NULL as backend_deal_3_mc_cd,
             |NULL as backend_deal_3_vrsn,
             |NULL as backend_deal_4_ln_itm,
             |NULL as backend_deal_4_mc_cd,
             |NULL as backend_deal_4_vrsn,
             |NULL as ndp_coefficient,
             |0 as ndp_estimated_lcy_ext,
             |0 as ndp_estimated_lcy_unit,
             |0 as ndp_estimated_usd_ext,
             |0 as ndp_estimated_usd_unit,
             |NULL as hpe_order,
             |NULL as hpe_order_item_mcc,
             |NULL as hpe_order_item_number,
             |NULL as hpe_order_date,
             |0 as hpe_order_item_disc_lcy_ext,
             |0 as hpe_order_item_disc_usd_ext,
             |NULL as hpe_upfront_deal,
             |NULL as hpe_order_item_cleansed_deal,
             |NULL as hpe_upfront_deal_type,
             |NULL as hpe_order_item_orig_deal,
             |0 as hpe_upfront_disc_lcy_ext,
             |0 as hpe_upfront_disc_usd_ext,
             |NULL as hpe_order_end_user_addr,
             |NULL as hpe_upfront_deal_end_user_addr,
             |NULL as upfront_deal_1_type,
             |NULL as upfront_deal_1_end_user_addr,
             |NULL as backend_deal_1_type,
             |NULL as backend_deal_1_end_user_addr,
             |NULL as partner_purchase_price_lcy_ext,
             |0 as partner_purchase_price_lcy_unit,
             |NULL as partner_purchase_price_usd_ext,
             |0 as partner_purchase_price_usd_unit,
             |NULL as net_cost_after_rebate_lcy_ext,
             |NULL as net_cost_after_rebate_usd_ext,
             |NULL as net_cost_currency_code,
             |NULL as rebate_amount_lcy_ext,
             |0 as rebate_amount_lcy_unit,
             |NULL as rebate_amount_usd_ext,
             |0 as rebate_amount_usd_unit,
             |NULL as partner_sell_price_lcy_ext,
             |0 as partner_sell_price_lcy_unit,
             |NULL as partner_sell_price_usd_ext,
             |0 as partner_sell_price_usd_unit,
             |NULL as backend_deal_1_dscnt_lcy_ext,
             |0 as backend_deal_1_dscnt_lcy_pr_unt,
             |NULL as backend_deal_1_dscnt_usd_ext,
             |0 as backend_deal_1_dscnt_usd_pr_unt,
             |0 as backend_deal_1_lcy_unt_prc,
             |0 as backend_deal_1_usd_unt_prc,
             |NULL as backend_deal_2_dscnt_lcy_ext,
             |0 as backend_deal_2_dscnt_lcy_pr_unt,
             |NULL as backend_deal_2_dscnt_usd_ext,
             |0 as backend_deal_2_dscnt_usd_pr_unt,
             |0 as backend_deal_2_lcy_unt_prc,
             |0 as backend_deal_2_usd_unt_prc,
             |NULL as backend_deal_3_dscnt_lcy_ext,
             |0 as backend_deal_3_dscnt_lcy_pr_unt,
             |NULL as backend_deal_3_dscnt_usd_ext,
             |0 as backend_deal_3_dscnt_usd_pr_unt,
             |0 as backend_deal_3_lcy_unt_prc,
             |0 as backend_deal_3_usd_unt_prc,
             |NULL as backend_deal_4_dscnt_lcy_ext,
             |0 as backend_deal_4_dscnt_lcy_pr_unt,
             |NULL as backend_deal_4_dscnt_usd_ext,
             |0 as backend_deal_4_dscnt_usd_pr_unt,
             |0 as backend_deal_4_lcy_unt_prc,
             |0 as backend_deal_4_usd_unt_prc,
             |0 as backend_rbt_1_lcy_pr_unt,
             |0 as backend_rbt_1_usd_pr_unt,
             |0 as backend_rbt_2_lcy_pr_unt,
             |0 as backend_rbt_2_usd_pr_unt,
             |0 as backend_rbt_3_lcy_pr_unt,
             |0 as backend_rbt_3_usd_pr_unt,
             |0 as backend_rbt_4_lcy_pr_unt,
             |0 as backend_rbt_4_usd_pr_unt,
             |0 as prch_agrmnt_dscnt_lcy_unt,
             |0 as prch_agrmnt_dscnt_usd_unt,
             |0 as sls_asp_lcy,
             |NULL as sls_asp_lcy_unt_prc,
             |NULL as sls_asp_usd,
             |NULL as sls_asp_usd_unt_prc,
             |0 as sls_avrg_deal_nt_lcy,
             |0 as sls_avrg_deal_nt_lcy_unt_prc,
             |0 as sls_avrg_deal_nt_usd,
             |0 as sls_avrg_deal_nt_usd_unt_prc,
             |0 as sls_avrg_deal_nt_vcy,
             |0 as sls_avrg_deal_nt_vcy_unt_prc,
             |0 as sls_deal_nt_lcy,
             |NULL as sls_deal_nt_lcy_unt_prc,
             |COALESCE(tcm_deal_nt_usd_cd,0) as sls_deal_nt_usd,
             |NULL as sls_deal_nt_usd_unt_prc,
             |0 as sls_deal_nt_vcy,
             |0 as sls_deal_nt_vcy_unt_prc,
             |0 as sls_lst_lcy,
             |0 as sls_lst_lcy_unt_prc,
             |0 as sls_lst_usd,
             |NULL as sls_lst_usd_unt_prc,
             |0 as sls_lst_vcy,
             |0 as sls_lst_vcy_unt_prc,
             |0 as sls_ndp_lcy,
             |NULL as sls_ndp_lcy_unt_prc,
             |0 as sls_ndp_usd,
             |NULL as sls_ndp_usd_unt_prc,
             |0 as sls_ndp_vcy,
             |0 as sls_ndp_vcy_unt_prc,
             |NULL as upfrnt_deal_1_dscnt_lcy,
             |0 as upfrnt_deal_1_dscnt_lcy_pr_unt,
             |0 as upfrnt_deal_1_dscnt_usd,
             |0 as upfrnt_deal_1_dscnt_usd_pr_unt,
             |0 as upfrnt_deal_1_lcy_unt_prc,
             |0 as upfrnt_deal_1_usd_unt_prc,
             |NULL as upfrnt_deal_2_dscnt_lcy,
             |0 as upfrnt_deal_2_dscnt_lcy_pr_unt,
             |NULL as upfrnt_deal_2_dscnt_usd,
             |0 as upfrnt_deal_2_dscnt_usd_pr_unt,
             |0 as upfrnt_deal_2_lcy_unt_prc,
             |0 as upfrnt_deal_2_usd_unt_prc,
             |NULL as upfrnt_deal_3_dscnt_lcy_ext,
             |0 as upfrnt_deal_3_dscnt_lcy_pr_unt,
             |NULL as upfrnt_deal_3_dscnt_usd_ext,
             |0 as upfrnt_deal_3_dscnt_usd_pr_unt,
             |0 as upfrnt_deal_3_lcy_unt_prc,
             |0 as upfrnt_deal_3_usd_unt_prc,
             |NULL as pos_prs_iud_flag,
             |NULL as pos_prs_reserve_field_1,
             |NULL as pos_prs_reserve_field_2,
             |NULL as pos_prs_reserve_field_3,
             |NULL as pos_prs_reserve_field_4,
             |NULL as pos_prs_reserve_field_5,
             |NULL as pos_prs_reserve_field_6,
             |NULL as pos_prs_reserve_field_7,
             |NULL as pos_prs_reserve_field_8,
             |NULL as pos_prs_reserve_field_9,
             |NULL as pos_prs_reserve_field_10,
             |NULL as upfrnt_deal_1_ln_itm,
             |NULL as upfrnt_deal_1_mc_cd,
             |NULL as upfrnt_deal_1_vrsn,
             |NULL as upfrnt_deal_2_ln_itm,
             |NULL as upfrnt_deal_2_mc_cd,
             |NULL as upfrnt_deal_2_vrsn,
             |NULL as upfrnt_deal_3_ln_itm,
             |NULL as upfrnt_deal_3_mc_cd,
             |NULL as upfrnt_deal_3_vrsn,
             |NULL as asp_coefficient,
             |0 as avrg_deal_nt_coefficient,
             |NULL as deal_geo,
             |NULL as dscnt_geo,
             |NULL as ex_rate_pcy_to_usd,
             |NULL as ex_rate_lcy_to_usd,
             |0 as ex_rate_vcy_to_usd,
             |NULL as lcl_curr_cd,
             |NULL as valtn_curr_cd,
             |NULL as valtn_cust_appl_cd,
             |NULL as valtn_prch_agrmnt_dscnt,
             |NULL as valtn_prch_agrmnt,
             |NULL as valtn_prc_dsc,
             |NULL as valtn_trsn_id,
             |NULL as valtn_wrnng_cd,
             |NULL as e2open_fl_id_valtn_rspns,
             |NULL as prnt_trsn_id_valtn_rspns,
             |NULL as prv_trsn_id_valtn_rspns,
             |NULL as reporter_id_valtn_rspns,
             |NULL as trsn_id_valtn_rspns,
             |NULL as ptnr_rptd_prc_lcy,
             |NULL as ptnr_rptd_prc_usd,
             |NULL as sls_bs_qty,
             |NULL as sls_opt_qty,
             |ps.src_sys_upd_ts as src_sys_upd_ts,
             |ps.src_sys_ky as src_sys_ky,
             |ps.lgcl_dlt_ind as lgcl_dlt_ind,
             |ps.ins_gmt_ts as ins_gmt_ts,
             |ps.upd_gmt_ts as upd_gmt_ts,
             |ps.src_sys_extrc_gmt_ts as src_sys_extrc_gmt_ts,
             |ps.src_sys_btch_nr as src_sys_btch_nr,
             |ps.fl_nm as fl_nm,
             |ps.ld_jb_nr as ld_jb_nr,
             |'BASE BUSINESS' as deal_ind,
             |'OTHERS' as deal_ind_grp,
             |NULL as bs_prod_nr,
             |'N' as bi_trsn_xclsn_flg,
             |'N' as bi_trsn_xclsn_rsn_cd,
             |NULL as prod_opt,
             |vol_sku_flg_cd as vol_sku_flg,
             |CASE WHEN skuBmt.ctobto_flg is NULL and skuBmtOptCode.ctobto_flg is NULL THEN 'BTO' WHEN skuBmt.ctobto_flg is NULL and skuBmtOptCode.ctobto_flg is NOT NULL THEN skuBmtOptCode.ctobto_flg ELSE COALESCE(skuBmt.ctobto_flg,'N/A') END as bi_cto_bto_flg,
             |trim(bi_rslr_ctry_cd) as bi_rslr_ctr_cd,
             |trim(bi_dstr_ctry_cd) as bi_dstr_ctr_cd,
             |NULL as bi_rptr_ctr_cd,
             |'NO ENTITLED PARTY' as bi_enttld_ctry_cd,
             |CASE WHEN bi_end_cust_lgcy_opsi_id IS NULL THEN 'NO END CUSTOMER' else trim(ps.bi_end_cust_ctry_cd) end as bi_end_cust_ctry_cd,
             |NULL as mc_cd,
             |NULL as mc_cd_dn,
             |NULL as upfrnt_deal_1_mc_dn_cd,
             |NULL as upfrnt_deal_2_mc_dn_cd,
             |NULL as upfrnt_deal_3_mc_dn_cd,
             |NULL as backend_deal_1_mc_dn_cd,
             |NULL as backend_deal_2_mc_dn_cd,
             |NULL as backend_deal_3_mc_dn_cd,
             |NULL as backend_deal_4_mc_dn_cd,
             |NULL as ps_e2open_extrt_nm,
             |NULL as ps_e2open_seq_id,
             |NULL as ps_e2open_extrt_dt,
             |NULL as ps_e2open_extrt_ts,
             |NULL as ps_prs_e2open_extrt_nm,
             |NULL as ps_prs_e2open_seq_id,
             |NULL as ps_prs_e2open_extrt_dt,
             |NULL as ps_prs_e2open_extrt_ts,
             |NULL as ps_eap_load_dt,
             |NULL as ps_eap_load_ts,
             |NULL as ps_prs_eap_load_dt,
             |NULL as ps_prs_eap_load_ts,
             |NULL as hpe_invoice_dt,
             |NULL as hpe_ord_bundle_id,
             |NULL as hpe_ord_deal_id,
             |NULL as hpe_obj_srvc_mtrl_id,
             |NULL as hpe_ord_item_nr,
             |NULL as hpe_ord_mc_cd,
             |NULL as hpe_ord_pft_cntr,
             |NULL as hpe_ord_sales_dvsn_cd,
             |NULL as hpe_shpmt_dt,
             |NULL as hpe_ord_dt,
             |NULL as hpe_ord_drp_shp_flg,
             |NULL as hpe_ord_end_cust_prty_id,
             |NULL as hpe_ord_nr,
             |NULL as hpe_ord_rtm,
             |NULL as hpe_ord_sld_br_typ,
             |NULL as hpe_ord_sld_pa_nr,
             |NULL as hpe_ord_sld_prty_id,
             |NULL as hpe_ord_tier2_rslr_br_typ,
             |NULL as hpe_ord_tier2_rslr_prty_id,
             |NULL as hpe_ord_typ,
             |trim(bi_rslr_prty_id) as bi_rslr_prty_id,
             |upper(COALESCE(trim(bi_rslr_rsn_cd),'NO RESELLER')) as bi_rslr_rsn_cd,
             |trim(bi_dstr_prty_id) as bi_dstr_prty_id,
             |COALESCE(UPPER(trim(bi_dstr_rsn_cd)),'NO DISTRIBUTOR') as bi_dstr_rsn_cd,
             |BI_rslr_BR_typ_nm as BI_rslr_BR_typ_nm,
             |BI_Dstr_BR_typ_nm as BI_Dstr_BR_typ_nm,
             |CASE WHEN bi_end_cust_lgcy_opsi_id IS NULL THEN 'NO END CUSTOMER' else trim(bi_end_cust_prty_id) end as end_cust_party_id,
             |CASE WHEN bi_end_cust_lgcy_opsi_id IS NULL THEN 'NO END CUSTOMER' else NULL END as bi_end_cust_br_typ_nm,
             |CASE WHEN bi_end_cust_lgcy_opsi_id IS NULL THEN 'NO END CUSTOMER' ELSE 'CSIS AMS' END as bi_end_rsn_cd,
             |'NO ENTITLED PARTY' as bi_enti_rsn_cd,
             |'NO ENTITLED PARTY' as entitled_party_id,
             |Current_timestamp as ins_ts,
             |case when trim(upper(bi_rslr_rsn_cd))='NO RESELLER' or bi_rslr_rsn_cd is NULL then 'NO RESELLER' ELSE trim(bi_rslr_lgcy_pro_id) END AS bi_rslr_lgcy_pro_id,
             |case when trim(upper(bi_dstr_rsn_cd))='NO DISTRIBUTOR' or bi_dstr_rsn_cd is NULL then 'NO DISTRIBUTOR' ELSE trim(bi_dstr_lgcy_pro_id) END AS dstr_lgcy_pro_id,
             |COALESCE(trim(bi_end_cust_lgcy_opsi_id),'NO END CUSTOMER') as bi_end_customer_legacy_opsi_id,
             |'NO ENTITLED PARTY' as bi_enttld_prty_br_typ_nm,
             |NULL as ope_rtm,
             |substring(dy_dt,0,7) as partition_year_month  from csisAmsSimNtBmtRef PS
             |LEFT JOIN (select * from ea_common.bmt_bi_sku_dmnsn where UPPER(TRIM(rec_src))='CTO_BTO' and  product_nr is not NULL) skuBmt ON PS.prod_nr = skuBmt.product_nr
             |LEFT JOIN (select * from ea_common.bmt_bi_sku_dmnsn where UPPER(TRIM(rec_src))='CTO_BTO' and  product_option is not NULL) skuBmtOptCode ON split(PS.prod_nr,'#')[1]=skuBmtOptCode.product_option
             |""".stripMargin)

        val loadPOSSF_DF = chnlptnrPsFact.repartition(25).write.mode("overwrite").format("orc").insertInto("ea_common_an"+ "." + "chnlptnr_ps_wout_sn_fact_inc")
        logger.info("POS_FACT_Full_Load is completed")
      
        csisAmsWoSnDataLoad.repartition(25).write.mode("append").insertInto("ea_common_an"+ "." + "chnlptnr_ps_wout_sn_fact_inc")
        load_pos_sn_fact_load = true
        logger.info("CSIS_AMS_HIST Load is completed")
    }
    spark.catalog.dropTempView("rptg_ptnr_mstr_dmnsn_tmp")
    val tgtCnt = spark.sql(s"""select trsn_id from ea_common.chnlptnr_ps_wout_sn_fact_inc """)
    tgt_count = 0
    }
    else {
      logger.info("*******No_incremental_data in PS_DMNSN Table**********")
      src_count = 0
      tgt_count = 0
      load_pos_sn_fact_load = true
    }

    auditObj.setAudBatchId(ld_jb_nr + "_" + batchId)
    auditObj.setAudDataLayerName("cnsmptn")
    auditObj.setAudApplicationName("job_EA_loadConsumption")
    //**auditObj.setAudObjectName(propertiesObject.getObjName())**\\
    auditObj.setAudObjectName("ChannelPartnerSFPOSLoadINC")
    auditObj.setAudLoadTimeStamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    if (load_pos_sn_fact_load) {
      auditObj.setAudJobStatusCode("success")
    } else {
      auditObj.setAudJobStatusCode("failed")
    }
    auditObj.setAudSrcRowCount(src_count)
    auditObj.setAudTgtRowCount(tgt_count)
    auditObj.setAudErrorRecords(0)
    auditObj.setAudCreatedBy(configObject.getSsc().sparkContext.sparkUser)
    auditObj.setFlNm("")
    auditObj.setSysBtchNr(ld_jb_nr)
    auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
    Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)

  } catch {

    case sslException: InterruptedException => {
      logger.error("Interrupted Exception")
      auditObj.setAudBatchId(ld_jb_nr + "_" + batchId)
      auditObj.setAudDataLayerName("cnsmptn")
      auditObj.setAudApplicationName("ChannelPartnerSFPOSLoad")
      auditObj.setAudObjectName(objName)
      auditObj.setAudJobStatusCode("failed")
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
      auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
      Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
      jobStatusFlag = false
      sqlCon.close()
      spark.close()
      System.exit(1)
    }
    case nseException: NoSuchElementException => {
      logger.error("No Such element found: " + nseException.printStackTrace())
      auditObj.setAudBatchId(ld_jb_nr + "_" + batchId)
      auditObj.setAudDataLayerName("cnsmptn")
      auditObj.setAudApplicationName("ChannelPartnerSFPOSLoad")
      auditObj.setAudObjectName(objName)
      auditObj.setAudJobStatusCode("failed")
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
      auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
      Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
      jobStatusFlag = false
      sqlCon.close()
      spark.close()
      System.exit(1)

    }
    case anaException: AnalysisException => {
      logger.error("SQL Analysis Exception: " + anaException.printStackTrace())
      auditObj.setAudBatchId(ld_jb_nr + "_" + batchId)
      auditObj.setAudDataLayerName("cnsmptn")
      auditObj.setAudApplicationName("ChannelPartnerSFPOSLoad")
      auditObj.setAudObjectName(objName)
      auditObj.setAudJobStatusCode("failed")
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
      auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
      Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
      jobStatusFlag = false
      sqlCon.close()
      spark.close()
      System.exit(1)

    }
    case connException: ConnectException => {
      logger.error("Connection Exception: " + connException.printStackTrace())
      auditObj.setAudBatchId(ld_jb_nr + "_" + batchId)
      auditObj.setAudDataLayerName("cnsmptn")
      auditObj.setAudApplicationName("ChannelPartnerSFPOSLoad")
      auditObj.setAudObjectName(objName)
      auditObj.setAudJobStatusCode("failed")
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
      auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
      Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
      jobStatusFlag = false
      sqlCon.close()
      spark.close()
      System.exit(1)
    }
    case illegalArgs: IllegalArgumentException => {
      logger.error("Connection Exception: " + illegalArgs.printStackTrace())
      auditObj.setAudBatchId(ld_jb_nr + "_" + batchId)
      auditObj.setAudDataLayerName("cnsmptn")
      auditObj.setAudApplicationName("ChannelPartnerSFPOSLoad")
      auditObj.setAudObjectName(objName)
      auditObj.setAudJobStatusCode("failed")
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
      auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
      Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
      jobStatusFlag = false
      sqlCon.close()
      spark.close()
      System.exit(1)
    }
    case allException: Exception => {
      logger.error("Connection Exception: " + allException.printStackTrace())
      auditObj.setAudJobStatusCode("failed")
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
      auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
      Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
      jobStatusFlag = false
      sqlCon.close()
      spark.close()
      System.exit(1)
    }
  } finally {
    logger.info("//*********************** Log End for ChannelPartnerSFPOSLoad.scala ************************//")
    sqlCon.close()
    spark.close()
    if (!jobStatusFlag) System.exit(1)
  }
}
