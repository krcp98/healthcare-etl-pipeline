# 🏥 Healthcare Data Pipeline - AWS Glue ETL

A comprehensive end-to-end healthcare data processing pipeline built with AWS Glue, demonstrating advanced ETL patterns, data quality management, and business intelligence generation.

![AWS Glue](https://img.shields.io/badge/AWS-Glue-FF9900?style=flat-square&logo=amazon-aws)
![Python](https://img.shields.io/badge/Python-3.9-3776AB?style=flat-square&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C?style=flat-square&logo=apache-spark)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

## 🎯 Project Overview

This project implements a production-ready ETL pipeline for healthcare data processing, featuring:

- **5-Stage ETL Process**: Ingestion → Quality → Transform → Metrics → Export
- **Advanced Data Quality**: Multi-layered validation and cleansing
- **Business Intelligence**: Patient risk scoring, geographic analysis, performance metrics
- **Scalable Architecture**: Handles datasets from 1K to 1M+ records

## 📊 Pipeline Architecture


```tRaw Data (CSV) → Data Quality → Transformation → Business Metrics → Analytics (Parquet)
     ↓              ↓              ↓               ↓                    ↓
┌─────────────┐ ┌──────────────┐ ┌─────────────┐ ┌──────────────┐ ┌─────────────┐
│   S3 Raw    │ │  Validation  │ │    Joins    │ │     KPIs     │ │S3 Processed │
│   Storage   │ │   & Clean    │ │  & Enrich   │ │  & Reports   │ │  & Analytics│
└─────────────┘ └──────────────┘ └─────────────┘ └──────────────┘ └─────────────┘
```

## 🚀 Quick Start

### Prerequisites
- AWS Account with Glue access
- S3 bucket with healthcare data
- IAM role with appropriate permissions

### 1. Clone Repository
bash
git clone https://github.com/yourusername/healthcare-etl-pipeline.git
cd healthcare-etl-pipeline


### 2. Deploy to AWS Glue
bash
# Upload the script to S3
aws s3 cp src/healthcare_etl_pipeline.py s3://your-glue-scripts-bucket/

# Create Glue Job using AWS CLI
aws glue create-job --cli-input-json file://deployment/glue-job-definition.json


### 3. Configure Job Parameters
bash
--SOURCE_BUCKET: healthcare-data-lake-chauhan
--TARGET_BUCKET: healthcare-data-lake-chauhan  
--CATALOG_DATABASE: healthcare_db


### 4. Run Pipeline
bash
aws glue start-job-run --job-name healthcare-etl-pipeline


## 📋 Data Schema

### Input Data Sources

#### Patients (patients.csv)
| Column | Type | Description |
|--------|------|-------------|
| patient_id | String | Unique patient identifier |
| first_name | String | Patient first name |
| last_name | String | Patient last name |
| age | Integer | Patient age (0-120) |
| gender | String | Patient gender |
| city | String | Patient city |
| state | String | Patient state |

#### Claims (insurance_claims.csv)
| Column | Type | Description |
|--------|------|-------------|
| claim_id | String | Unique claim identifier |
| patient_id | String | Foreign key to patients |
| claim_amount | Double | Claim amount in USD |
| claim_date | Date | Date of claim |
| claim_status | String | APPROVED/DENIED/PENDING |

#### Procedures (medical_procedures.csv)
| Column | Type | Description |
|--------|------|-------------|
| procedure_id | String | Unique procedure identifier |
| patient_id | String | Foreign key to patients |
| procedure_code | String | Medical procedure code |
| procedure_name | String | Procedure description |
| procedure_date | Date | Date of procedure |

#### Problematic Patients (problematic_patients.csv)
| Column | Type | Description |
|--------|------|-------------|
| patient_id | String | Patient ID with data quality issues |
| issue_type | String | Type of data quality problem |
| issue_description | String | Detailed description of the issue |

## 🎯 Key Features

### Data Quality Management
- **Validation Rules**: Age bounds, null checks, amount limits
- **Anomaly Detection**: Uses problematic_patients.csv to identify and exclude known data quality issues
- **Data Standardization**: Name formatting, status normalization
- **Quality Scoring**: Assigns quality scores (0-100) based on validation results

### Business Intelligence
- **Patient Risk Scoring**: HIGH/MEDIUM/LOW classification
- **Geographic Analysis**: State-wise performance metrics  
- **Temporal Analytics**: Monthly trends and seasonality
- **Healthcare KPIs**: Approval rates, patient lifetime value

### Performance Optimization
- **Smart Partitioning**: Optimized for analytics queries
- **Parquet Format**: Columnar storage with compression
- **Efficient Joins**: Broadcast joins for dimension tables
- **Memory Management**: Configurable worker allocation


## 🔧 Configuration

### Environment Variables
bash
export AWS_REGION=us-east-1
export SOURCE_BUCKET=your-source-bucket
export TARGET_BUCKET=your-target-bucket


### Glue Job Parameters
json
{
  "WorkerType": "G.1X",
  "NumberOfWorkers": 2,
  "Timeout": 15,
  "MaxRetries": 1,
  "GlueVersion": "4.0"
}


## 📊 Output Structure

After successful execution:

s3://your-bucket/
├── processed-data/
│   ├── patients/           # Clean patient records
│   ├── claims/            # Validated claims (partitioned by status)  
│   └── procedures/        # Clean procedure records
├── analytics/
│   ├── patient-summary/   # Risk-scored patient analytics
│   ├── claims-enriched/   # Enhanced claims with demographics
│   └── procedure-analytics/ # Procedure trend analysis
└── metrics/
    ├── monthly-performance/ # Time-series KPIs
    ├── state-analysis/     # Geographic insights
    └── risk-analysis/      # Risk distribution metrics


## 🤝 Contributing

1. Fork the repository
2. Create feature branch (git checkout -b feature/amazing-feature)
3. Commit changes (git commit -m 'Add amazing feature')
4. Push to branch (git push origin feature/amazing-feature)
5. Open Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- AWS Glue Documentation and Best Practices
- Apache Spark Performance Tuning Guidelines
- Healthcare Data Standards (HL7 FHIR)


**⭐ If this project helped you learn AWS Glue, please give it a star!**
