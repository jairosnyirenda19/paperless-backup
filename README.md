The Paperless NGX system at MEPA is now receiving near-daily content updates, making a reliable backup solution essential. The current backup script is deployed on the MEPA Paperless NGX server.

At present, the script performs incremental, additive backups. However, since users may occasionally delete data or alter storage paths, the system must also support sync-style backups—removing files from AWS that are deleted on the server and adding new ones accordingly.

To enhance resilience, we recommend implementing a weekly full backup routine, at least until network usage approaches the Starlink data quota threshold.

Additionally, the database is backed up daily to ensure data integrity.
