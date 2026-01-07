# My Data Warehouse Project

Welcome to my **Data Warehouse Project** repository! This project demonstrates building a data warehouse using SQL Server and the medallion architecture (Bronze/Silver/Gold layers). This is part of my learning journey to earn a certificate in SQL and data warehousing.

---

## 📋 Project Overview

This project involves:
- Building a modern data warehouse using SQL Server
- Implementing the medallion architecture (Bronze → Silver → Gold)
- Importing and transforming data from multiple sources (ERP and CRM systems)
- Creating analytics-ready data models for business intelligence

---

## 🎯 Objectives

### Data Engineering
- Import raw data from CSV files into Bronze layer
- Cleanse and standardize data in Silver layer
- Create dimensional models in Gold layer for analytics

### Analytics & Reporting
Develop insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

---

## 🗂️ Project Structure
```
my-data-warehouse-project/
├── datasets/           # Source CSV data files
├── scripts/
│   ├── bronze/        # Raw data ingestion scripts
│   ├── silver/        # Data transformation scripts
│   └── gold/          # Analytics layer scripts
└── docs/              # Documentation
```

---

## 🛠️ Technologies Used

- **SQL Server** (running in Docker)
- **VS Code** with SQL extensions
- **Git & GitHub** for version control
- **Draw.io** for data architecture diagrams

---

## 🚀 Getting Started

1. Clone this repository
2. Set up SQL Server (Docker recommended for Mac)
3. Run `init_database.sql` to create the database structure
4. Follow the scripts in bronze → silver → gold order

---

## 👤 About Me

Hi! I'm Mohamad Abadi, a chemical engineer trying to enter this part of the world. I'm working through an SQL course to strengthen my data warehousing skills. This project is part of my learning journey.

Feel free to explore the code and reach out if you have questions!

---

## 📝 License

This project is for educational purposes.
