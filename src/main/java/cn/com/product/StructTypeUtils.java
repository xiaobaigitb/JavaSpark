package cn.com.product;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructTypeUtils {

    private static StructType st1;
    private static StructType st2;
    private static StructType st3;
    private static StructType st4;
    private static StructType st5;
    //kudu
    private static StructType st6;

    static {
        List<StructField> schemaFields = null;
        {
            schemaFields = new ArrayList<StructField>();
            schemaFields.add(DataTypes.createStructField("cserialno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree_of_bl", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org_of_bl", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_custmngr_type", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_custmngr", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("area_rule_id", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_tano", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_date", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("occur_shares", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("occur_shares_sum", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("split_ratio", DataTypes.createDecimalType(12, 2), true));
            schemaFields.add(DataTypes.createStructField("accu_split_ratio", DataTypes.createDecimalType(12, 2), true));
            schemaFields.add(DataTypes.createStructField("ratio", DataTypes.createDecimalType(12, 2), true));
            schemaFields.add(DataTypes.createStructField("rnk", DataTypes.createDecimalType(12, 0), true));
            st1 = DataTypes.createStructType(schemaFields);
        }

        {
            schemaFields = new ArrayList<StructField>();
            schemaFields.add(DataTypes.createStructField("dk_tano", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("cserialno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_date", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_account", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_tradeacco_reg", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_product", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_share_type", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("agencyno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("netno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree_of_bl", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org_of_bl", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_custmngr_type", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_custmngr", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("ratio", DataTypes.createDecimalType(12, 2), true));
            schemaFields.add(DataTypes.createStructField("inserttime", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_system_of_upd", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_cust_type", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_cust_type_sub", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("occur_shares", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("effective_to", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("effective_from", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("area_rule_id", DataTypes.createDecimalType(12, 0), true));
            st2 = DataTypes.createStructType(schemaFields);

        }
        {
            schemaFields = new ArrayList<StructField>();
            schemaFields.add(DataTypes.createStructField("dk_tano", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("cserialno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("ori_cserialno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_cust_type", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_mkt_trade_type", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("ori_sk_mkt_trade_type", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_account", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_tradeacco_reg", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_product", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_share_type", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_agency", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("agencyno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("netno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree_of_bl", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org_of_bl", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_custmngr_type", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_custmngr", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("occur_shares", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("shares", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("effective_from", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("effective_to", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("dymc_ratio", DataTypes.createDecimalType(12, 2), true));
            schemaFields.add(DataTypes.createStructField("sk_date", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("area_rule_id", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("memo", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_crt", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_modi", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_system_of_upd", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("batchno", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("inserttime", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("updatetime", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("uuid", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_check_status", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_record_status", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("bk_fundaccount", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("bk_tradeaccount", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_bourseflag", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_cust_type_sub", DataTypes.StringType, true));
            st3 = DataTypes.createStructType(schemaFields);
        }

        {
            schemaFields = new ArrayList<StructField>();
            schemaFields.add(DataTypes.createStructField("sk_org", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_org_of_bl", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_custmngr", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_tano", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_account", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_tradeacco_reg", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("agencyno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("netno", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_product", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_share_type", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("shares_custmngr", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("all_shares", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("share_ratio", DataTypes.createDecimalType(12, 2), true));
            schemaFields.add(DataTypes.createStructField("rnk", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("cnt", DataTypes.createDecimalType(12, 0), true));
            st4 = DataTypes.createStructType(schemaFields);
        }

        {
            schemaFields = new ArrayList<StructField>();
            schemaFields.add(DataTypes.createStructField("dk_tano_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("cserialno_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_date_1", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_account_1", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_tradeacco_reg_1", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_product_1", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_share_type_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("agencyno_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("netno_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org_1", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree_of_bl_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org_of_bl_1", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_custmngr_type_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_custmngr_1", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("ratio_1", DataTypes.createDecimalType(12, 2), true));
            schemaFields.add(DataTypes.createStructField("inserttime_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_system_of_upd_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_cust_type_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_cust_type_sub_1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("occur_shares_1", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("effective_to_1", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("effective_from_1", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("area_rule_id_1", DataTypes.createDecimalType(12, 0), true));

            schemaFields.add(DataTypes.createStructField("dk_tano_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("cserialno_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("ori_cserialno_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_cust_type_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_mkt_trade_type_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("ori_sk_mkt_trade_type_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_account_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_tradeacco_reg_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_product_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_share_type_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_agency_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("agencyno_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("netno_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_org_tree_of_bl_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_org_of_bl_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_custmngr_type_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_custmngr_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("occur_shares_2", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("shares_2", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("effective_from_2", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("effective_to_2", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("dymc_ratio_2", DataTypes.createDecimalType(12, 2), true));
            schemaFields.add(DataTypes.createStructField("sk_date_2", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("area_rule_id_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("memo_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_crt_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("sk_invpty_of_modi_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("dk_system_of_upd_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("batchno_2", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("inserttime_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("updatetime_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("uuid_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_check_status_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_record_status_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("bk_fundaccount_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("bk_tradeaccount_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_bourseflag_2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("dk_cust_type_sub_2", DataTypes.StringType, true));
            st5 = DataTypes.createStructType(schemaFields);
        }
        {
            schemaFields = new ArrayList<StructField>();
            schemaFields.add(DataTypes.createStructField("SK_ACCOUNT", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("SK_TRADEACCO_REG_OF_ORI", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("SK_PRODUCT", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("AGENCYNO", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("NETNO", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("DK_SHARE_TYPE", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("EFFECTIVE_FROM", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("DK_TANO", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("DK_SYSTEM_OF_UPD", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("TRD_SHR_TRX_SERIALNO", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("SK_INVPTY", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("SK_TRADEACCO_REG", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("SK_CURRENCY", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("SK_AGENCY", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("BK_FUNDACCOUNT", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("BK_TRADEACCOUNT", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("DK_BONUS_TYPE", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("DK_BOURSEFLAG", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("EFFECTIVE_TO", DataTypes.createDecimalType(8, 0), true));
            schemaFields.add(DataTypes.createStructField("OCCUR_SHARES", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("SHARES", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("OCCUR_FREEZE", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("FREEZE_SHARE", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField(" MEMO", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("SK_INVPTY_OF_CRT", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("SK_INVPTY_OF_MODI", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("DAC", DataTypes.createDecimalType(38, 12), true));
            schemaFields.add(DataTypes.createStructField("DDVC", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("BATCHNO", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("INSERTTIME", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("UPDATETIME", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("UUID", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("NAME_INBANK", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("DK_CAPITALMODE", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("BANKNO", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("BANK_NAME", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("BANK_ACCOUNT", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("RULE_ID", DataTypes.createDecimalType(12, 0), true));
            schemaFields.add(DataTypes.createStructField("CUST_COUNT", DataTypes.createDecimalType(12, 0), true));
            st6 = DataTypes.createStructType(schemaFields);
        }

    }

    public static StructType getStructType(String tableName) {
        if (tableName.equals("tmp_split_ratio_data")) {
            return st1;
        } else if (tableName.equals("mid_trd_specar_tran_split")) {
            return st2;

        } else if (tableName.equals("trd_specar_bal_detail")) {
            return st3;
        } else if (tableName.equals("tmp_chnlmngr_data")) {
            return st4;
        } else if (tableName.equals("mid_trd_specar_tran_split_trd_specar_bal_detail")) {
            return st5;
        }
        //kudu
        else if (tableName.equals("tmp_trd_fundbal_detail")) {
            return st6;
        }

        return null;
    }
}
