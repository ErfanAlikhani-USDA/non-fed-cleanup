# ree_sharepoint_non_fed_cleanup
Ingests Non-Fed Cleanup Files from SharePoint into Delta bronze and gold tables.

## Structure
- `src/sharepoint_config.py` — config variables (mappings, schemas, paths)
- `src/setup_secrets.py` — one-time secrets setup (run manually)
- `src/non_fed_cleanup.py` — main pipeline notebook (scheduled daily)
- `resources/non_fed_cleanup_job.yml` — job definition

## Tables
- Bronze: `ree_edapt.hr_bronze.ree_usaccess_applicant_status_bronze`, `ree_edapt.hr_bronze.ree_empowhr_contractor_bronze`
- Gold: `ree_edapt.hr.ree_usaccess_applicant_status`, `ree_edapt.hr.ree_empowhr_contractor`
