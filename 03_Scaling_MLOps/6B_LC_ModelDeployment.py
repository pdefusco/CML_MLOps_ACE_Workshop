from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

spark = SparkSession\
    .builder\
    .appName("LC_Predictor")\
    .getOrCreate()

model = PipelineModel.load("s3a://cdp-sandbox-default-se.s3.us-east-2.amazonaws.com/pdefusco")

feature_schema = StructType([StructField("acc_now_delinq", IntegerType(), True),
StructField("acc_open_past_24mths", IntegerType(), True),
StructField("annual_inc", DoubleType(), True),
StructField("avg_cur_bal", IntegerType(), True),
StructField("chargeoff_within_12_mths", IntegerType(), True),
StructField("collections_12_mths_ex_med", IntegerType(), True),
StructField("delinq_2yrs", IntegerType(), True), 
StructField("delinq_amnt", IntegerType(), True),
StructField("dti", DoubleType(), True),
StructField("funded_amnt", IntegerType(), True),
StructField("funded_amnt_inv", DoubleType(), True),
StructField("grade", StringType(), True),
StructField("inq_last_6mths", IntegerType(), True),
StructField("installment", DoubleType(), True),
StructField("int_rate", DoubleType(), True),
StructField("loan_amnt", IntegerType(), True),
StructField("mo_sin_old_rev_tl_op", IntegerType(), True),
StructField("mo_sin_rcnt_rev_tl_op", IntegerType(), True),
StructField("mo_sin_rcnt_tl", IntegerType(), True), 
StructField("mort_acc", IntegerType(), True),
StructField("num_accts_ever_120_pd", IntegerType(), True),
StructField("num_actv_bc_tl", IntegerType(), True),
StructField("num_actv_rev_tl", IntegerType(), True),
StructField("num_bc_sats", IntegerType(), True),
StructField("num_bc_tl", IntegerType(), True),
StructField("num_il_tl", IntegerType(), True),
StructField("num_op_rev_tl", IntegerType(), True),
StructField("num_rev_accts", DoubleType(), True),
StructField("num_rev_tl_bal_gt_0", IntegerType(), True),
StructField("num_sats", IntegerType(), True),
StructField("num_tl_30dpd", IntegerType(), True),
StructField("num_tl_90g_dpd_24m", IntegerType(), True),
StructField("num_tl_op_past_12m", IntegerType(), True),
StructField("open_acc", IntegerType(), True),
StructField("pct_tl_nvr_dlq", DoubleType(), True),
StructField("policy_code", IntegerType(), True),
StructField("pub_rec", IntegerType(), True), 
StructField("pub_rec_bankruptcies", IntegerType(), True),
StructField("revol_bal", IntegerType(), True),
StructField("revol_util", IntegerType(), True),
StructField("tax_liens", StringType(), True),
StructField("tot_cur_bal", DoubleType(), True),
StructField("tot_hi_cred_lim", IntegerType(), True),
StructField("total_acc", IntegerType(), True),
StructField("total_bal_ex_mort", IntegerType(), True),
StructField("total_bc_limit", IntegerType(), True),
StructField("total_il_high_credit_limit", DoubleType(), True),
StructField("total_rev_hi_lim", IntegerType(), True),
StructField("verification_status", StringType(), True),
StructField("issue_month", IntegerType(), True)])
                            
#args = {"feature":"1.0", "0.0", "0.0", 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 8.0, 65000.0, 10086.0, 0.0, 0.0, 0.0, 0.0, 12000.0, 12000.0, 0.0, 253.79, 12000.0, 145.0, 4.0, 1.0, 2.0, 0.0, 2.0, 4.0, 5.0, 10.0, 13.0, 11.0, 22.0, 4.0, 18.0, 0.0, 0.0, 3.0, 18.0, 100.0, 1.0, 0.0, 0.0, "9786.0", "0.0", "181540.0", "291626.0", "37.0", "74787.0", "49500.0"}                            

def predict(args):
    LC_features = args["feature"].split(",")
    LC_features = spark.DataFrame(
    [
        (
            int(LC_features[0]), 
            int(LC_features[1]), 
            float(LC_features[2]), 
            int(LC_features[3]), 
            int(LC_features[4]), 
            int(LC_features[5]), 
            int(LC_features[6]), 
            int(LC_features[7]), 
            float(LC_features[8]), 
            int(LC_features[9]), 
            float(LC_features[10]), 
            str(LC_features[11]), 
            int(LC_features[12]), 
            float(LC_features[13]), 
            float(LC_features[14]), 
            int(LC_features[15]), 
            int(LC_features[16]), 
            int(LC_features[17]), 
            int(LC_features[18]), 
            int(LC_features[19]), 
            int(LC_features[20]), 
            int(LC_features[21]),
            int(LC_features[22]), 
            int(LC_features[23]), 
            int(LC_features[24]), 
            int(LC_features[25]), 
            float(LC_features[26]), 
            int(LC_features[27]), 
            int(LC_features[28]), 
            int(LC_features[29]), 
            int(LC_features[30]), 
            float(LC_features[31]), 
            int(LC_features[32]),
            int(LC_features[33]), 
            int(LC_features[34]), 
            int(LC_features[35]), 
            int(LC_features[36]), 
            int(LC_features[37]), 
            int(LC_features[38]), 
            int(LC_features[39]), 
            str(LC_features[40]), 
            int(LC_features[41]), 
            int(LC_features[42]), 
            int(LC_features[43]), 
            int(LC_features[44]), 
            int(LC_features[45]),
            int(LC_features[46]), 
            int(LC_features[47]), 
            str(LC_features[48]), 
            int(LC_features[49])
        )
    ], schema=feature_schema)
                            
        
        result = model.transform(LC_features).collect()[0].prediction
        return {"result" : result}
                            



