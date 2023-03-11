## What's in this folder?
Here you'll find modified versions of scripts developed for the orchestration of ETL and reverse-ETL tasks.

- [`dbt_monitoring.py`](https://github.com/ryanwags/portfolio/blob/main/etl/dbt_monitoring.py): This script contains a condensed version of a custom Python module developed for interacting with dbt's metadata APIs. The full version of this module was used to fetch various dbt artifacts, including run states, model run timing, and the results of tests and source freshness checks. This information was later fed into a dashboard used to monitor the health of our dbt account.
- [`mixpanel_user_properties.py`](https://github.com/ryanwags/portfolio/blob/main/etl/mixpanel_user_properties.py): Mixpanel is a browser-based reporting platform that summarizes event- and user-level activity from web and mobile applications (think Tableau for product health). This script is a condensed version of a production script used to dynamically update user properties in the Mixpanel UI. At runtime, the current and previous snapshots of a dbt model containing property values are compared, and user profiles with at least one changed property are marked for updating. Comparison is made using an MD5 surrogate key constructed from all property values. Updated profiles are serialized as JSON, batched to accommodate API limits, and posted using exponential backoff to avoid 429 errors.
- [`tealium_events.py`](https://github.com/ryanwags/portfolio/blob/main/etl/tealium_events.py): Tealium is a tag management system that generates event- and user-level data from web and mobile applications, which is made available for ingestion as unstructured data in S3. This script contains a condensed version of a custom Python module containing wrapper functions for each step of the ETL process: checking for unfetched files in S3, fetching them, deserializing and transforming event records, and upserting finished data into a warehouse. In production, a separate entry-point script loaded this module and executed its functions in order.
---
_Copyright © 2023 by Ryan Wagner. All works are original and may not be copied or distributed without permission._