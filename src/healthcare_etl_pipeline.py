import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
import boto3
from datetime import datetime, date

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_BUCKET',
    'TARGET_BUCKET',
    'CATALOG_DATABASE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
SOURCE_BUCKET = args.get('SOURCE_BUCKET', 'healthcare-data-lake-chauhan')
TARGET_BUCKET = args.get('TARGET_BUCKET', 'healthcare-data-lake-chauhan')
CATALOG_DB = args.get('CATALOG_DATABASE', 'healthcare_db')

print(f"Starting ETL Pipeline for Healthcare Data Lake: {SOURCE_BUCKET}")

# ==============================================================================
# STAGE 1: DATA INGESTION & INITIAL VALIDATION
# ==============================================================================

def read_source_data():
    """Read all source datasets with error handling"""
    
    try:
        # Read Patients Data
        patients_df = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [f"s3://{SOURCE_BUCKET}/raw-data/patients/patients.csv"],
                "recurse": True
            },
            transformation_ctx="patients_source"
        )
        
        # Read Insurance Claims
        claims_df = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [f"s3://{SOURCE_BUCKET}/raw-data/claims/insurance_claims.csv"],
                "recurse": True
            },
            transformation_ctx="claims_source"
        )
        
        # Read Medical Procedures
        procedures_df = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [f"s3://{SOURCE_BUCKET}/raw-data/procedures/medical_procedures.csv"],
                "recurse": True
            },
            transformation_ctx="procedures_source"
        )
        
        # Read Problematic Patients (for data quality reference)
        problematic_patients_df = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [f"s3://{SOURCE_BUCKET}/raw-data-quality/problematic_patients.csv"],
                "recurse": True
            },
            transformation_ctx="problematic_patients_source"
        )
        
        print("Successfully read all source datasets")
        return patients_df, claims_df, procedures_df, problematic_patients_df
        
    except Exception as e:
        print(f"Error reading source data: {str(e)}")
        raise

# ==============================================================================
# STAGE 2: DATA QUALITY & CLEANSING
# ==============================================================================

def apply_data_quality_rules(patients_df, claims_df, procedures_df, problematic_df):
    """Apply comprehensive data quality checks and cleansing"""
    
    print("Starting Data Quality Validation...")
    
    # Convert to Spark DataFrames for complex operations
    patients_spark = patients_df.toDF()
    claims_spark = claims_df.toDF()
    procedures_spark = procedures_df.toDF()
    problematic_spark = problematic_df.toDF()
    
    # Get problematic patient IDs for filtering
    problematic_ids = [row.patient_id for row in problematic_spark.select("patient_id").collect()]
    
    # PATIENTS DATA QUALITY
    patients_cleaned = patients_spark.filter(
        # Remove problematic patients
        (~F.col("patient_id").isin(problematic_ids)) &
        # Basic validation rules
        (F.col("patient_id").isNotNull()) &
        (F.col("first_name").isNotNull()) &
        (F.col("last_name").isNotNull()) &
        (F.length(F.col("patient_id")) > 0) &
        # Age validation (reasonable range)
        (F.col("age").between(0, 120))
    ).withColumn(
        # Standardize names
        "first_name", F.initcap(F.trim(F.col("first_name")))
    ).withColumn(
        "last_name", F.initcap(F.trim(F.col("last_name")))
    ).withColumn(
        # Add data quality flags
        "data_quality_score", F.lit(100.0)
    ).withColumn(
        "processed_date", F.current_date()
    )
    
    # CLAIMS DATA QUALITY
    claims_cleaned = claims_spark.filter(
        (F.col("claim_id").isNotNull()) &
        (F.col("patient_id").isNotNull()) &
        (F.col("claim_amount") > 0) &
        (F.col("claim_amount") < 1000000)  # Reasonable upper limit
    ).withColumn(
        # Standardize claim status
        "claim_status", F.upper(F.trim(F.col("claim_status")))
    ).withColumn(
        # Calculate days since claim
        "days_since_claim", F.datediff(F.current_date(), F.col("claim_date"))
    )
    
    # PROCEDURES DATA QUALITY
    procedures_cleaned = procedures_spark.filter(
        (F.col("procedure_id").isNotNull()) &
        (F.col("patient_id").isNotNull()) &
        (F.col("procedure_code").isNotNull())
    ).withColumn(
        "procedure_name", F.initcap(F.trim(F.col("procedure_name")))
    )
    
    print("Data Quality validation completed")
    
    # Convert back to Dynamic Frames
    return (
        DynamicFrame.fromDF(patients_cleaned, glueContext, "patients_cleaned"),
        DynamicFrame.fromDF(claims_cleaned, glueContext, "claims_cleaned"),
        DynamicFrame.fromDF(procedures_cleaned, glueContext, "procedures_cleaned")
    )

# ==============================================================================
# STAGE 3: DATA TRANSFORMATION & ENRICHMENT
# ==============================================================================

def create_analytical_datasets(patients_df, claims_df, procedures_df):
    """Create enriched analytical datasets using CLEANED data"""
    
    print("Creating analytical datasets from cleaned data...")
    
    # Convert CLEANED Dynamic Frames to Spark DataFrames
    patients = patients_df.toDF()  # This is cleaned data
    claims = claims_df.toDF()      # This is cleaned data  
    procedures = procedures_df.toDF()  # This is cleaned data
    
    # PATIENT SUMMARY WITH CLAIMS & PROCEDURES
    patient_summary = patients.alias("p").join(
        claims.groupBy("patient_id").agg(
            F.count("claim_id").alias("total_claims"),
            F.sum("claim_amount").alias("total_claim_amount"),
            F.avg("claim_amount").alias("avg_claim_amount"),
            F.max("claim_date").alias("last_claim_date"),
            F.min("claim_date").alias("first_claim_date")
        ).alias("c"),
        F.col("p.patient_id") == F.col("c.patient_id"),
        "left"
    ).join(
        procedures.groupBy("patient_id").agg(
            F.count("procedure_id").alias("total_procedures"),
            F.collect_set("procedure_code").alias("procedure_codes"),
            F.max("procedure_date").alias("last_procedure_date")
        ).alias("pr"),
        F.col("p.patient_id") == F.col("pr.patient_id"),
        "left"
    ).select(
        F.col("p.*"),
        F.coalesce(F.col("c.total_claims"), F.lit(0)).alias("total_claims"),
        F.coalesce(F.col("c.total_claim_amount"), F.lit(0.0)).alias("total_claim_amount"),
        F.coalesce(F.col("c.avg_claim_amount"), F.lit(0.0)).alias("avg_claim_amount"),
        F.col("c.last_claim_date"),
        F.col("c.first_claim_date"),
        F.coalesce(F.col("pr.total_procedures"), F.lit(0)).alias("total_procedures"),
        F.col("pr.procedure_codes"),
        F.col("pr.last_procedure_date")
    ).withColumn(
        # Patient risk score based on claims and procedures
        "patient_risk_score", 
        F.when(F.col("total_claim_amount") > 50000, F.lit("HIGH"))
         .when(F.col("total_claim_amount") > 10000, F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    ).withColumn(
        # Patient lifetime value
        "patient_lifetime_value", 
        F.col("total_claim_amount") + (F.col("total_procedures") * 500)
    )
    
    # CLAIMS ANALYSIS WITH PATIENT INFO
    claims_enriched = claims.alias("c").join(
        patients.select("patient_id", "age", "gender", "city", "state").alias("p"),
        F.col("c.patient_id") == F.col("p.patient_id"),
        "inner"
    ).join(
        procedures.groupBy("patient_id", "procedure_date").agg(
            F.count("procedure_id").alias("procedures_same_day")
        ).alias("pr"),
        (F.col("c.patient_id") == F.col("pr.patient_id")) & 
        (F.col("c.claim_date") == F.col("pr.procedure_date")),
        "left"
    ).select(
        F.col("c.*"),
        F.col("p.age"),
        F.col("p.gender"),
        F.col("p.city"),
        F.col("p.state"),
        F.coalesce(F.col("pr.procedures_same_day"), F.lit(0)).alias("related_procedures")
    ).withColumn(
        # Age group classification
        "age_group",
        F.when(F.col("age") < 18, F.lit("PEDIATRIC"))
         .when(F.col("age") < 65, F.lit("ADULT"))
         .otherwise(F.lit("SENIOR"))
    ).withColumn(
        # Claim complexity score
        "claim_complexity", 
        F.col("claim_amount") / 1000 + F.col("related_procedures")
    )
    
    # PROCEDURE ANALYTICS
    procedure_analytics = procedures.alias("pr").join(
        patients.select("patient_id", "age", "gender").alias("p"),
        F.col("pr.patient_id") == F.col("p.patient_id"),
        "inner"
    ).join(
        claims.groupBy("patient_id").agg(
            F.avg("claim_amount").alias("avg_patient_claim")
        ).alias("c"),
        F.col("pr.patient_id") == F.col("c.patient_id"),
        "left"
    ).withColumn(
        "procedure_month", F.date_format(F.col("procedure_date"), "yyyy-MM")
    ).withColumn(
        "procedure_year", F.year(F.col("procedure_date"))
    )
    
    print("Analytical datasets created successfully")
    
    return (
        DynamicFrame.fromDF(patient_summary, glueContext, "patient_summary"),
        DynamicFrame.fromDF(claims_enriched, glueContext, "claims_enriched"),
        DynamicFrame.fromDF(procedure_analytics, glueContext, "procedure_analytics")
    )

# ==============================================================================
# STAGE 4: DATA AGGREGATION & METRICS
# ==============================================================================

def create_business_metrics(patient_summary_df, claims_enriched_df, procedures_df):
    """Create business intelligence metrics"""
    
    print("Generating business metrics...")
    
    patients = patient_summary_df.toDF()
    claims = claims_enriched_df.toDF()
    procedures = procedures_df.toDF()
    
    # MONTHLY METRICS
    monthly_metrics = claims.groupBy(
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("month")
    ).agg(
        F.count("claim_id").alias("total_claims"),
        F.sum("claim_amount").alias("total_revenue"),
        F.avg("claim_amount").alias("avg_claim_amount"),
        F.countDistinct("patient_id").alias("unique_patients"),
        F.sum(F.when(F.col("claim_status") == "APPROVED", 1).otherwise(0)).alias("approved_claims"),
        F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied_claims")
    ).withColumn(
        "approval_rate", 
        F.col("approved_claims") / F.col("total_claims") * 100
    ).withColumn(
        "revenue_per_patient", 
        F.col("total_revenue") / F.col("unique_patients")
    )
    
    # STATE-WISE ANALYSIS
    state_analysis = patients.groupBy("state").agg(
        F.count("patient_id").alias("patient_count"),
        F.avg("total_claim_amount").alias("avg_claim_per_patient"),
        F.avg("age").alias("avg_age"),
        F.sum("patient_lifetime_value").alias("total_state_value")
    ).withColumn(
        "state_rank", F.row_number().over(
            Window.orderBy(F.col("total_state_value").desc())
        )
    )
    
    # RISK ANALYSIS
    risk_metrics = patients.groupBy("patient_risk_score").agg(
        F.count("patient_id").alias("patient_count"),
        F.avg("total_claim_amount").alias("avg_claims"),
        F.avg("total_procedures").alias("avg_procedures"),
        F.sum("patient_lifetime_value").alias("total_value")
    )
    
    print("Business metrics generated successfully")
    
    return (
        DynamicFrame.fromDF(monthly_metrics, glueContext, "monthly_metrics"),
        DynamicFrame.fromDF(state_analysis, glueContext, "state_analysis"),
        DynamicFrame.fromDF(risk_metrics, glueContext, "risk_metrics")
    )

# ==============================================================================
# STAGE 5: DATA EXPORT & CATALOGING
# ==============================================================================

def write_to_data_lake(dataframe, path, partition_cols=None):
    """Write data to S3 with proper partitioning and cataloging"""
    
    try:
        if partition_cols:
            glueContext.write_dynamic_frame.from_options(
                frame=dataframe,
                connection_type="s3",
                format="parquet",
                connection_options={
                    "path": f"s3://{TARGET_BUCKET}/{path}",
                    "partitionKeys": partition_cols
                },
                transformation_ctx=f"write_{path.replace('/', '_')}"
            )
        else:
            glueContext.write_dynamic_frame.from_options(
                frame=dataframe,
                connection_type="s3",
                format="parquet",
                connection_options={
                    "path": f"s3://{TARGET_BUCKET}/{path}"
                },
                transformation_ctx=f"write_{path.replace('/', '_')}"
            )
        print(f"Successfully wrote data to: s3://{TARGET_BUCKET}/{path}")
        
    except Exception as e:
        print(f"Error writing to {path}: {str(e)}")
        raise

# ==============================================================================
# MAIN ETL PIPELINE EXECUTION
# ==============================================================================

def main():
    """Main ETL pipeline execution"""
    
    print("Starting Healthcare Data Pipeline...")
    print("="*60)
    
    try:
        # Stage 1: Data Ingestion
        print("STAGE 1: Data Ingestion")
        patients_raw, claims_raw, procedures_raw, problematic_raw = read_source_data()
        
        # Stage 2: Data Quality & Cleansing
        print("\nSTAGE 2: Data Quality & Cleansing")
        patients_clean, claims_clean, procedures_clean = apply_data_quality_rules(
            patients_raw, claims_raw, procedures_raw, problematic_raw
        )
        
        # Stage 3: Data Transformation
        print("\nSTAGE 3: Data Transformation & Enrichment")
        patient_summary, claims_enriched, procedure_analytics = create_analytical_datasets(
            patients_clean, claims_clean, procedures_clean
        )
        
        # Stage 4: Business Metrics
        print("\nSTAGE 4: Business Metrics Generation")
        monthly_metrics, state_analysis, risk_metrics = create_business_metrics(
            patient_summary, claims_enriched, procedure_analytics
        )
        
        # Stage 5: Data Export
        print("\nSTAGE 5: Data Export to Data Lake")
        
        # Write cleansed data
        write_to_data_lake(patients_clean, "processed-data/patients")
        write_to_data_lake(claims_clean, "processed-data/claims", ["claim_status"])
        write_to_data_lake(procedures_clean, "processed-data/procedures")
        
        # Write analytical datasets
        write_to_data_lake(patient_summary, "analytics/patient-summary", ["patient_risk_score"])
        write_to_data_lake(claims_enriched, "analytics/claims-enriched", ["age_group", "state"])
        write_to_data_lake(procedure_analytics, "analytics/procedure-analytics", ["procedure_year"])
        
        # Write business metrics
        write_to_data_lake(monthly_metrics, "metrics/monthly-performance")
        write_to_data_lake(state_analysis, "metrics/state-analysis")
        write_to_data_lake(risk_metrics, "metrics/risk-analysis")
        
        # Final Statistics
        print("\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Processed Patients: {patients_clean.count()}")
        print(f"Processed Claims: {claims_clean.count()}")
        print(f"Processed Procedures: {procedures_clean.count()}")
        print(f"Generated 6 analytical datasets")
        print(f"Generated 3 business metric reports")
        print("All data written to S3 with proper partitioning")
        print("Healthcare Data Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed with error: {str(e)}")
        raise
    
    finally:
        job.commit()

if __name__ == "__main__":
    main()