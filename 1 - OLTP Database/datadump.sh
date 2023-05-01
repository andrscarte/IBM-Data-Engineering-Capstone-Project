#! /bin/bash
mysqldump --host=127.0.0.1 --port=3306 --user=root --password sales sales_data > sales_data.sql