#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os
import pandas as pd
from flask import Flask
from sqlalchemy import create_engine
from google.cloud import storage
import logging

app = Flask(__name__)

# Google Cloud Storage Setup
BUCKET_NAME = "10a-test-bucket"
FOLDER_NAME = "10a-folder"
FILE_NAME = "10a.csv"
TEMP_FILE_PATH = f"/tmp/{FILE_NAME}" if os.name != 'nt' else FILE_NAME  # /tmp for Unix, plain for Windows

# RDS Settings
RDS_HOST = "domo-replica.c6c95xn17jbt.us-east-2.rds.amazonaws.com"
RDS_USER = "gkumar"
RDS_PASSWORD = "WcRYaTFHVWcI7Wm"
RDS_DATABASE = "aje_main"

@app.route("/", methods=["GET", "POST"])
def query_rds_to_gcs():
    try:
        # Set up basic logging
        logging.basicConfig(level=logging.INFO)
        logging.info("Starting process...")

        # Database connection
        logging.info("Creating DB connection...")
        db_uri = f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}/{RDS_DATABASE}"
        engine = create_engine(db_uri)

        # SQL query
        query = """ 
        SELECT
	s.order_identity AS submission_id,
	s.order_identity AS fulfillment_identity,
	IFNULL(s0.external_id, esr.external_id) AS external_id,
	s.date AS submission_date,
	w.company_id AS company,
        ofd.type AS from_order_form_discount_type,
	s0.group_id AS group_id,
	IF(ehv.order_identity IS NOT NULL, 1, 0) AS eles_high_volume,
	lw.short_name AS short_name,
	CASE
		WHEN lw.id = 5 THEN 'review'
		ELSE lw.type 
	END AS type,
	lwt.is_premium AS is_premium,
	IF(lw.language_id != 1, lw.language_id, lw.to_language_id) AS language,

	IFNULL(s0.turnaround_time, esr.hours/24) AS timeline,
	sc.word_count AS cart_word_bin,
	COALESCE(wc.processor_wordcount, wc.calculated_wordcount, wc.QCE_wordcount, wc.JE_wordcount, wc.SE_wordcount) AS wordcount,
	CASE
		WHEN w.company_id = 'acs' AND s.date < '2019-01-18' THEN '2019-01-17'
		WHEN w.company_id = 'acs' AND s.date < '2019-10-01' THEN '2019-09-30'
		WHEN w.company_id = 'acs' AND s.date < '2020-09-01' THEN '2020-08-31'
		WHEN w.company_id = 'acs' AND s.date < '2021-10-01' THEN '2021-08-31'
		WHEN w.company_id = 'eles' AND s.date < '2016-01-01' THEN '2015-12-31'
		WHEN w.company_id = 'eles' AND s.date < '2020-01-01' THEN '2019-12-31'
		WHEN w.company_id = 'npgle' AND s.date < '2020-01-01' THEN '2019-12-31'
		ELSE '2099-12-31'
	END AS max_date,

	IF(t.invoice_id IS NULL AND sc.suppress_invoice = 1, 1, 0) AS invoice_suppressed,
	t.invoice_status,
	t.payment_method,
	t.currency_code,
	t.exchange_rate,
	t.invoice_revenue,
	t.is_paid,
        t.invoice_number,

	MAX(IF(ws.service_id IN(1, 18, 24, 71, 82, 83, 84, 85) AND (s0.group_id IS NULL OR s0.group_id != 80971), 1, 0)) AS has_standard,
	MAX(IF(ws.service_id IN(2, 4, 19, 25, 35, 48, 55, 56, 63), 1, 0)) AS has_premium,
	MAX(IF(ws.service_id IN(76) OR s0.group_id = 80971, 1, 0)) AS has_scied,
	MAX(IF(ws.service_id IN(7, 28, 60), 1, 0)) AS has_chinese,
	MAX(IF(ws.service_id IN(8, 26, 61), 1, 0)) AS has_portuguese,
	MAX(IF(ws.service_id IN(11, 22, 62), 1, 0)) AS has_spanish,
	MAX(IF(ws.service_id IN(9, 10, 12, 13, 14, 21, 23), 1, 0)) AS has_other_translation,

       MAX(IF(ws.role_id IN(4) AND ws.service_id != 76, sr.num_words, NULL)) AS `Editor Word Count`,

	w.active AS active
FROM
	operations_aje_main.submissions s LEFT JOIN aje_main.order s0 ON s.order_identity = s0.fulfillment_identity
        LEFT JOIN aje_main.order_form `of` ON s0.external_id = `of`.order_external_id
        LEFT JOIN aje_main.order_form__discount ofd ON `of`.id = ofd.order_form_id
	LEFT JOIN operations_aje_main.eles_service_request esr ON s.order_identity = esr.order_identity
	INNER JOIN operations_aje_main.workflows w ON s.order_identity = w.order_identity
	LEFT JOIN operations_aje_main.workflow_steps ws ON w.id = ws.workflow_id
        LEFT JOIN operations_aje_main.revisions sr ON ws.final_revision_id = sr.id
	LEFT JOIN operations_aje_main.task wt ON ws.task_id = wt.id
	LEFT JOIN aje_main.order_form sc ON s0.external_id = sc.order_external_id
	LEFT JOIN operations_aje_main.package lw ON w.package_id = lw.id
	LEFT JOIN (
		SELECT
			tri.invoice_id,
			tri.submission,
                        tri.invoice_number,
			GROUP_CONCAT(DISTINCT tri.status ORDER BY tri.status) AS invoice_status,
			GROUP_CONCAT(DISTINCT IF(tri.payment_method = '', NULL, tri.payment_method) ORDER BY tri.payment_method) AS payment_method,
			GROUP_CONCAT(DISTINCT tri.currency_code ORDER BY tri.currency_code) AS currency_code,
			IFNULL(
				ROUND(SUM((tri.total - IFNULL(tri.credits_applied, 0)) * tri.exchange_rate) /
					SUM((tri.total - IFNULL(tri.credits_applied, 0))), 6),
				1
			) AS exchange_rate,
			IFNULL(ROUND(SUM((tri.total - IFNULL(tri.credits_applied, 0)) * tri.exchange_rate), 2), 0) AS invoice_revenue,
			MIN(CASE
				WHEN tri.status = 'paid' THEN 1
				WHEN tri.payment_method = 'Free' THEN 1
				ELSE 0
			END) AS is_paid
		FROM transactions_reporting.invoices tri
		WHERE tri.submission IS NOT NULL
		GROUP BY tri.submission
	) t ON s0.external_id = t.submission
	LEFT JOIN (
		SELECT
			lps.package_id AS package_id,
			MAX(IF(lps.service_id IN(2, 4, 16, 19, 25, 35, 55, 56, 63, 71), 1, 0)) AS is_premium
		FROM
			operations_aje_main.package__service lps
		GROUP BY lps.package_id
	) lwt ON lw.id = lwt.package_id
	LEFT JOIN (
		SELECT
			s.id AS submission_id,
			MAX(IF(ws.role_id = 80, r.num_words, ws.manual_word_count)) AS processor_wordcount,
			MAX(IF(ws.role_id IN(12, 29), r.num_words, NULL)) AS QCE_wordcount,
			MAX(IF(ws.role_id IN(6), r.num_words, NULL)) AS SE_wordcount,
			MAX(IF(ws.role_id IN(4), r.num_words, NULL)) AS JE_wordcount,
			COALESCE(MAX(IF(ws.step_type_id = 2 AND ws.role_id IN(4), wt.word_count, NULL)),
				 MAX(IF(ws.step_type_id = 2 AND ws.role_id IN(4), ws.manual_word_count, NULL)),
		 		 MAX(IF(ws.step_type_id = 2 AND ws.role_id IN(4), ws.automated_word_count, NULL)),
				 MAX(IF(ws.step_type_id IN(2, 3), wt.word_count, NULL)),
		 		 MAX(IF(ws.step_type_id IN(2, 3), ws.manual_word_count, NULL)),
		 		 MAX(IF(ws.step_type_id IN(2, 3), ws.automated_word_count, NULL))) AS calculated_wordcount
		FROM
			operations_aje_main.submissions s
			INNER JOIN operations_aje_main.workflows w ON s.order_identity = w.order_identity
			INNER JOIN operations_aje_main.workflow_steps ws ON w.id = ws.workflow_id AND ws.step_type_id IN(2, 3) AND
				ws.role_id IN(4, 6, 12, 29, 30, 31, 35, 80)
			INNER JOIN operations_aje_main.task wt ON ws.task_id = wt.id
			LEFT JOIN operations_aje_main.revisions r ON ws.id = r.workflow_step_id
		WHERE ws.service_id NOT IN(30, 31, 38, 39, 40, 41, 42, 43, 46, 47, 50, 51, 53, 57, 59, 75, 78, 79, 80, 81, 86, 87, 88, 91)
		GROUP BY s.id
	) AS wc ON s.id = wc.submission_id
	LEFT JOIN (
		SELECT z.order_identity
		FROM
			(SELECT
				x.order_identity,
				IF(@cmonth = x.sub_month, @csub := @csub + 1, @csub := 1) AS sub_count,
				@cmonth := x.sub_month AS current_month
			FROM
				(SELECT s.date, DATE_FORMAT(s.date, '%%Y%%m') AS sub_month, s.order_identity
				FROM operations_aje_main.submissions s INNER JOIN operations_aje_main.workflows w ON s.order_identity = w.order_identity
				WHERE w.company_id = 'eles' AND w.package_id = 7 AND w.active = 1 AND s.date >= '2020-09-01'
				ORDER BY s.date ASC) x, (SELECT @csub := 0, @cmonth := '') vars
			) z
		WHERE z.sub_count > 620
	) ehv ON s.order_identity = ehv.order_identity
WHERE
	s.customer_identity != '948ad755-7817-4d15-8582-886869c297d9' 
GROUP BY
	s.id
        """

        # Execute query
        logging.info("Executing SQL query...")
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)

        logging.info(f"Retrieved {len(df)} rows.")

        # Write to CSV
        df.to_csv(TEMP_FILE_PATH, index=False)
        logging.info(f"CSV written to {TEMP_FILE_PATH}")

        # Upload to GCS
        logging.info("Uploading to GCS...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{FOLDER_NAME}/{FILE_NAME}")
        blob.upload_from_filename(TEMP_FILE_PATH)

        logging.info(f"File uploaded to gs://{BUCKET_NAME}/{FOLDER_NAME}/{FILE_NAME}")
        return f"Success: File uploaded to gs://{BUCKET_NAME}/{FOLDER_NAME}/{FILE_NAME}", 200

    except Exception as e:
        logging.exception("Error occurred")
        return f"Error: {str(e)}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# In[ ]:
